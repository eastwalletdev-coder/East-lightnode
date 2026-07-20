import { createServer, IncomingMessage, ServerResponse } from "http";
import { WebSocketServer, WebSocket } from "ws";
import {
  InboundMessage, BlockHeader, Role, EAST_CHAIN_ID,
} from "./types";

const PORT = Number(process.env.PORT || process.env.WS_PORT || 8081);
const VALIDATOR_SECRET = process.env.RAILWAY_VALIDATOR_SECRET || "";
const DEBUG_MODE = process.env.DEBUG_MODE === "true";

// ─── Utility: Enhanced Logging with Timestamps ─────────────────
function log(prefix: string, msg: string, data?: unknown) {
  const timestamp = new Date().toISOString();
  if (data) {
    console.log(`[${timestamp}] [${prefix}] ${msg}`, JSON.stringify(data, null, 2));
  } else {
    console.log(`[${timestamp}] [${prefix}] ${msg}`);
  }
}

function logDebug(prefix: string, msg: string, data?: unknown) {
  if (DEBUG_MODE) {
    const timestamp = new Date().toISOString();
    if (data) {
      console.debug(`[${timestamp}] [${prefix}] [DEBUG] ${msg}`, JSON.stringify(data, null, 2));
    } else {
      console.debug(`[${timestamp}] [${prefix}] [DEBUG] ${msg}`);
    }
  }
}

if (!VALIDATOR_SECRET) {
  log("RAILWAY", "⚠️  WARNING: RAILWAY_VALIDATOR_SECRET is not set — anyone could connect as validator!");
}

log("RAILWAY", `🚀 Starting Railway Hub on port ${PORT}`, { DEBUG_MODE });

// Top-N light nodes by score get promoted to "relay"
const RELAY_ROSTER_SIZE = 5;
const RELAY_RESCORE_INTERVAL_MS = 60_000;

// Connection ceiling & per-IP rate limit
const MAX_LIGHT_NODES = Number(process.env.MAX_LIGHT_NODES || 5000);
const IP_RATE_LIMIT_MAX = 10;
const IP_RATE_LIMIT_WINDOW_MS = 60_000;
const connectionAttemptsByIp = new Map<string, number[]>();

function isRateLimited(ip: string): boolean {
  const now = Date.now();
  const recent = (connectionAttemptsByIp.get(ip) || []).filter((t) => now - t < IP_RATE_LIMIT_WINDOW_MS);
  recent.push(now);
  connectionAttemptsByIp.set(ip, recent);
  const limited = recent.length > IP_RATE_LIMIT_MAX;
  if (limited) {
    log("RATE_LIMIT", `⛔ IP ${ip} exceeded rate limit`, { attempts: recent.length, max: IP_RATE_LIMIT_MAX });
  }
  return limited;
}

// Prevents connectionAttemptsByIp from growing forever
setInterval(() => {
  const cutoff = Date.now() - IP_RATE_LIMIT_WINDOW_MS;
  let cleaned = 0;
  connectionAttemptsByIp.forEach((timestamps, ip) => {
    const recent = timestamps.filter((t) => t > cutoff);
    if (recent.length === 0) {
      connectionAttemptsByIp.delete(ip);
      cleaned++;
    } else {
      connectionAttemptsByIp.set(ip, recent);
    }
  });
  if (cleaned > 0) {
    logDebug("RATE_LIMIT", `Cleaned up ${cleaned} IP entries from rate limit map`);
  }
}, IP_RATE_LIMIT_WINDOW_MS);

interface EastSocket extends WebSocket {
  isAlive?: boolean;
  role?: Role;
  nodeId?: string;
  connectedAt?: number;
}

// ─── State ────────────────────────────────────────────────────────
let validatorSocket: EastSocket | null = null;
const lightNodes = new Map<string, EastSocket>();
let latestHeader: BlockHeader | null = null;
const recentHeaders: BlockHeader[] = []; // rolling buffer, newest last
const BACKFILL_SIZE = 50; // Increased from 5 to 50 for better sync

// Node telemetry for /status endpoint
interface NodeTelemetry {
  lastHeartbeat: number;
  lastAckHeight: number;
  connectedAt: number;
  avgLatencyMs: number;
  participationSeconds: number;
  verifiedHeaderCount: number;
  isRelay: boolean;
  hasFullLedger: boolean;
  messagesReceived: number;
  messagesSent: number;
  lastMessageType?: string;
  lastMessageTime?: number;
}
const telemetry = new Map<string, NodeTelemetry>();
let currentRelayRoster: string[] = [];
let currentFullSyncProviders: string[] = [];

// ─── Message counters for statistics ─────────────────────────────
interface MessageStats {
  [key: string]: number;
}
const messageStats: MessageStats = {};

function recordMessageStat(type: string) {
  messageStats[type] = (messageStats[type] || 0) + 1;
}

function recomputeFullSyncProviders() {
  const providers = [...telemetry.entries()]
    .filter(([, t]) => t.hasFullLedger)
    .map(([nodeId]) => nodeId);
  const changed =
    providers.length !== currentFullSyncProviders.length ||
    providers.some((id) => !currentFullSyncProviders.includes(id));
  if (changed) {
    currentFullSyncProviders = providers;
    broadcastToLightNodes({ type: "full_sync_providers", nodeIds: currentFullSyncProviders });
    log("FULL_SYNC", `✅ Full-sync providers updated`, { providers: currentFullSyncProviders, count: providers.length });
  } else {
    logDebug("FULL_SYNC", `Full-sync providers unchanged`, { count: providers.length });
  }
}

function score(t: NodeTelemetry): number {
  const latencyScore = 1000 / Math.max(t.avgLatencyMs, 1);
  return latencyScore * Math.log1p(t.participationSeconds) * Math.log1p(t.verifiedHeaderCount);
}

function recomputeRelayRoster() {
  const validNodes = [...telemetry.entries()].filter(([, t]) => t.avgLatencyMs > 0);
  logDebug("RELAY", `Computing roster from ${validNodes.length} valid nodes`, { totalNodes: telemetry.size });

  const ranked = validNodes
    .sort(([, a], [, b]) => score(b) - score(a))
    .slice(0, RELAY_ROSTER_SIZE)
    .map(([nodeId]) => nodeId);

  const newlyPromoted = ranked.filter((id) => !currentRelayRoster.includes(id));
  const newlyDemoted = currentRelayRoster.filter((id) => !ranked.includes(id));

  for (const [nodeId, t] of telemetry.entries()) {
    t.isRelay = ranked.includes(nodeId);
  }

  newlyPromoted.forEach((id) => {
    const s = lightNodes.get(id);
    if (s) {
      send(s, { type: "relay:promoted" });
      log("RELAY", `🎯 Node promoted to relay`, { nodeId: id });
    }
  });

  newlyDemoted.forEach((id) => {
    const s = lightNodes.get(id);
    if (s) {
      send(s, { type: "relay:demoted" });
      log("RELAY", `📉 Node demoted from relay`, { nodeId: id });
    }
  });

  currentRelayRoster = ranked;
  broadcastToLightNodes({ type: "relay:roster", relayNodeIds: currentRelayRoster });

  if (newlyPromoted.length || newlyDemoted.length) {
    log("RELAY", `🔄 Relay roster updated`, {
      roster: currentRelayRoster,
      promoted: newlyPromoted,
      demoted: newlyDemoted,
      total: currentRelayRoster.length,
    });
  } else {
    logDebug("RELAY", "Relay roster unchanged", { roster: currentRelayRoster });
  }
}

function send(socket: EastSocket, msg: unknown) {
  if (socket.readyState === WebSocket.OPEN) {
    try {
      socket.send(JSON.stringify(msg));
      logDebug("SOCKET", `Sent message to ${socket.nodeId}`, msg);
    } catch (err) {
      log("SOCKET", `❌ Error sending message to ${socket.nodeId}`, err);
    }
  } else {
    logDebug("SOCKET", `Cannot send - socket not open for ${socket.nodeId}`, { state: socket.readyState });
  }
}

function broadcastToLightNodes(msg: unknown) {
  const json = JSON.stringify(msg);
  let sentCount = 0;
  lightNodes.forEach((socket) => {
    if (socket.readyState === WebSocket.OPEN) {
      try {
        socket.send(json);
        sentCount++;
      } catch (err) {
        log("BROADCAST", `❌ Error broadcasting to ${socket.nodeId}`, err);
      }
    }
  });
  logDebug("BROADCAST", `Broadcast complete`, {
    messageType: (msg as any).type,
    sentTo: sentCount,
    totalConnected: lightNodes.size,
  });
}

function publishBlock(header: BlockHeader) {
  const oldHeight = latestHeader?.height ?? -1;
  latestHeader = header;
  recentHeaders.push(header);
  if (recentHeaders.length > 20) recentHeaders.shift();

  log("BLOCK", `📦 Publishing block`, {
    height: header.height,
    hash: header.hash.substring(0, 16) + "...",
    previousHeight: oldHeight,
    blockGap: header.height - oldHeight,
    recentHeadersCount: recentHeaders.length,
    lightNodesConnected: lightNodes.size,
    timestamp: header.timestamp,
  });

  recordMessageStat("block:new");
  broadcastToLightNodes({ type: "block:new", header });

  log("BLOCK", `✅ Block relayed to light nodes`, {
    height: header.height,
    targetedNodes: lightNodes.size,
  });
}

// ─── HTTP: validator (Vercel serverless) publishes a block via POST ──
function handleHttp(req: IncomingMessage, res: ServerResponse) {
  const timestamp = new Date().toISOString();

  if (req.method === "GET" && req.url === "/status") {
    const status = {
      ok: true,
      timestamp,
      validatorConnected: !!validatorSocket,
      lightNodesConnected: lightNodes.size,
      maxLightNodes: MAX_LIGHT_NODES,
      latestHeight: latestHeader?.height ?? -1,
      latestBlockHash: latestHeader?.hash?.substring(0, 16) + "..." ?? "none",
      relayRoster: currentRelayRoster,
      relayRosterSize: currentRelayRoster.length,
      fullSyncProviders: currentFullSyncProviders,
      fullSyncProvidersCount: currentFullSyncProviders.length,
      recentHeadersCount: recentHeaders.length,
      backfillSize: BACKFILL_SIZE,
      connectionAttemptsByIpCount: connectionAttemptsByIp.size,
      messageStats,
      nodes: [...telemetry.entries()].map(([nodeId, t]) => ({
        nodeId,
        isRelay: t.isRelay,
        hasFullLedger: t.hasFullLedger,
        connectedAtSeconds: Math.round((Date.now() - t.connectedAt) / 1000),
        lastAckHeight: t.lastAckHeight,
        lastHeartbeatAgo: Math.round((Date.now() - t.lastHeartbeat) / 1000),
        avgLatencyMs: t.avgLatencyMs,
        participationSeconds: t.participationSeconds,
        verifiedHeaderCount: t.verifiedHeaderCount,
        messagesReceived: t.messagesReceived,
        messagesSent: t.messagesSent,
        lastMessageType: t.lastMessageType,
      })),
    };
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(status, null, 2));
    log("HTTP", `📊 Status endpoint accessed`, { lightNodesConnected: lightNodes.size });
    return;
  }

  if (req.method === "POST" && req.url === "/internal/publish-block") {
    const authHeader = req.headers["x-railway-secret"];
    const headerValid = VALIDATOR_SECRET && authHeader === VALIDATOR_SECRET;

    if (!headerValid) {
      const reason = !VALIDATOR_SECRET ? "NO_SECRET_CONFIGURED" : "INVALID_SECRET";
      log("HTTP", `🚫 Publish-block: UNAUTHORIZED`, {
        reason,
        headerProvided: !!authHeader,
      });
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "UNAUTHORIZED", reason }));
      return;
    }

    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 1024 * 1024) {
        log("HTTP", "❌ Publish-block: Payload too large", { size: body.length });
        res.writeHead(413, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "PAYLOAD_TOO_LARGE" }));
      }
    });

    req.on("end", () => {
      try {
        log("HTTP", `📥 Publish-block request received`, { payloadSize: body.length });
        const { header } = JSON.parse(body) as { header: BlockHeader };

        if (!header || typeof header.height !== "number" || !header.hash) {
          log("HTTP", `❌ Publish-block: Invalid header`, { hasHeader: !!header, height: header?.height, hash: !!header?.hash });
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: "INVALID_HEADER" }));
          return;
        }

        publishBlock(header);
        res.writeHead(200, { "Content-Type": "application/json" });
        const response = { success: true, relayedTo: lightNodes.size, height: header.height };
        res.end(JSON.stringify(response));
        log("HTTP", `✅ Publish-block: Success`, { height: header.height, relayedTo: lightNodes.size });
      } catch (err) {
        log("HTTP", `❌ Publish-block: JSON parse error`, err);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "INVALID_JSON" }));
      }
    });
    return;
  }

  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, uptime: process.uptime() }));
    return;
  }

  log("HTTP", `⚠️  Unhandled HTTP request`, { method: req.method, url: req.url });
  res.writeHead(404);
  res.end();
}

const httpServer = createServer(handleHttp);
const wss = new WebSocketServer({ server: httpServer, perMessageDeflate: false, maxPayload: 64 * 1024 });

wss.on("connection", (socket: EastSocket, req) => {
  const ip = (req.headers["x-forwarded-for"] as string)?.split(",")[0]?.trim()
    || req.socket.remoteAddress || "unknown";

  log("CONNECTION", `🔗 New connection attempt`, { ip, connectedNodes: lightNodes.size });

  if (isRateLimited(ip)) {
    send(socket, { type: "error", message: "RATE_LIMITED — too many connection attempts, try again shortly" });
    socket.close();
    log("CONNECTION", `❌ Connection rejected: RATE_LIMITED`, { ip });
    return;
  }

  if (lightNodes.size >= MAX_LIGHT_NODES) {
    send(socket, { type: "error", message: "HUB_AT_CAPACITY" });
    socket.close();
    log("CONNECTION", `❌ Connection rejected: HUB_AT_CAPACITY`, { ip, currentNodes: lightNodes.size, max: MAX_LIGHT_NODES });
    return;
  }

  socket.isAlive = true;
  socket.connectedAt = Date.now();

  log("CONNECTION", `✅ Connection accepted`, { ip, totalConnections: lightNodes.size + 1 });

  socket.on("pong", () => {
    socket.isAlive = true;
    logDebug("HEARTBEAT", `Pong received from ${socket.nodeId}`);
  });

  socket.on("message", (raw) => {
    let msg: InboundMessage;
    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      log("MESSAGE", `❌ Invalid JSON from ${socket.nodeId}`, { error: String(err) });
      send(socket, { type: "error", message: "Invalid JSON" });
      return;
    }

    recordMessageStat(msg.type);

    const t = telemetry.get(socket.nodeId || "unknown");
    if (t) {
      t.messagesReceived++;
      t.lastMessageType = msg.type;
      t.lastMessageTime = Date.now();
    }

    logDebug("MESSAGE", `Received ${msg.type}`, { nodeId: socket.nodeId, role: socket.role });

    switch (msg.type) {
      case "hello": {
        if (msg.chainId !== undefined && msg.chainId !== EAST_CHAIN_ID) {
          log("AUTH", `❌ WRONG_NETWORK from ${ip}`, { expectedChainId: EAST_CHAIN_ID, receivedChainId: msg.chainId });
          send(socket, { type: "error", message: `WRONG_NETWORK — expected chainId ${EAST_CHAIN_ID}` });
          socket.close();
          return;
        }

        if (msg.role === "validator") {
          if (!VALIDATOR_SECRET || msg.secret !== VALIDATOR_SECRET) {
            log("AUTH", `❌ UNAUTHORIZED validator attempt from ${ip}`);
            send(socket, { type: "error", message: "UNAUTHORIZED" });
            socket.close();
            return;
          }
          validatorSocket = socket;
          socket.role = "validator";
          socket.nodeId = msg.nodeId || "EASTCHAIN-L1";
          log("AUTH", `✅ Validator authenticated and connected`, { nodeId: socket.nodeId });
        } else {
          socket.role = "light-node";
          socket.nodeId = msg.nodeId;
          lightNodes.set(msg.nodeId, socket);
          telemetry.set(msg.nodeId, {
            lastHeartbeat: Date.now(),
            lastAckHeight: -1,
            connectedAt: Date.now(),
            avgLatencyMs: 0,
            participationSeconds: 0,
            verifiedHeaderCount: 0,
            isRelay: false,
            hasFullLedger: false,
            messagesReceived: 1,
            messagesSent: 0,
            lastMessageType: "hello",
          });

          log("NODE", `✅ Light node connected`, {
            nodeId: msg.nodeId,
            ip,
            totalLightNodes: lightNodes.size,
            chainId: msg.chainId,
          });

          // Send current relay roster and full-sync providers
          send(socket, { type: "relay:roster", relayNodeIds: currentRelayRoster });
          send(socket, { type: "full_sync_providers", nodeIds: currentFullSyncProviders });

          if (t = telemetry.get(msg.nodeId)) {
            t.messagesSent += 2;
          }
        }

        send(socket, {
          type: "welcome",
          network: "EAST",
          chainId: EAST_CHAIN_ID,
          version: "1.2",
          role: socket.role,
          latestHeight: latestHeader?.height ?? -1,
        });

        if (socket.role === "light-node" && (t = telemetry.get(socket.nodeId!))) {
          t.messagesSent++;
        }

        break;
      }

      case "ping": {
        send(socket, { type: "pong", time: Date.now() });
        logDebug("PING", `Pong sent to ${socket.nodeId}`);
        if (t) t.messagesSent++;
        break;
      }

      // ── Validator → Railway → all light nodes ──────────────────
      case "block:new": {
        if (socket.role !== "validator") {
          log("BLOCK", `❌ FORBIDDEN: Non-validator tried block:new from ${socket.nodeId}`, { role: socket.role });
          send(socket, { type: "error", message: "FORBIDDEN — validator role required" });
          return;
        }
        publishBlock(msg.header);
        break;
      }

      // ── Light node → Railway → validator (best-effort passthrough) ──
      case "heartbeat": {
        if (socket.role !== "light-node" || !socket.nodeId) {
          logDebug("HEARTBEAT", `⚠️  Heartbeat from non-light-node`, { role: socket.role, nodeId: socket.nodeId });
          return;
        }
        const t = telemetry.get(socket.nodeId);
        if (t) {
          t.lastHeartbeat = Date.now();
          t.messagesSent++;
        }
        if (validatorSocket) {
          send(validatorSocket, msg);
          log("HEARTBEAT", `📍 Heartbeat relayed`, {
            nodeId: socket.nodeId,
            height: msg.height,
            timestamp: msg.timestamp,
          });
        } else {
          log("HEARTBEAT", `⚠️  Heartbeat dropped - validator offline`, { nodeId: socket.nodeId, height: msg.height });
        }
        break;
      }

      case "sync_request": {
        if (socket.role !== "light-node" || !socket.nodeId) {
          logDebug("SYNC", `⚠️  Sync request from non-light-node`, { role: socket.role });
          return;
        }

        const backfill = recentHeaders.slice(-BACKFILL_SIZE);
        log("SYNC", `🔄 Sync request received`, {
          nodeId: socket.nodeId,
          requestFromHeight: msg.fromHeight,
          backfillSize: backfill.length,
          maxBackfillSize: BACKFILL_SIZE,
          availableBlockRange: recentHeaders.length > 0
            ? `${recentHeaders[0]?.height} - ${recentHeaders[recentHeaders.length - 1]?.height}`
            : "empty",
          latestHeight: latestHeader?.height ?? -1,
        });

        if (backfill.length > 0) {
          send(socket, { type: "block:backfill", headers: backfill });
          const t = telemetry.get(socket.nodeId);
          if (t) t.messagesSent++;
          log("SYNC", `📦 Backfill sent`, {
            nodeId: socket.nodeId,
            blocksCount: backfill.length,
            heightRange: `${backfill[0]?.height} - ${backfill[backfill.length - 1]?.height}`,
          });
        } else {
          log("SYNC", `⚠️  No backfill available`, { nodeId: socket.nodeId, recentHeadersCount: recentHeaders.length });
        }

        if (validatorSocket) {
          send(validatorSocket, msg);
          log("SYNC", `📤 Sync request forwarded to validator`, { nodeId: socket.nodeId, fromHeight: msg.fromHeight });
        } else {
          log("SYNC", `⚠️  Sync request not forwarded - validator offline`, { nodeId: socket.nodeId });
        }
        break;
      }

      case "ack": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const t = telemetry.get(socket.nodeId);
        if (t) {
          t.lastAckHeight = msg.height;
          t.messagesSent++;
        }

        log("ACK", `✅ ACK received and relayed`, {
          nodeId: socket.nodeId,
          ackHeight: msg.height,
          timestamp: msg.timestamp,
          heightGapFromLatest: (latestHeader?.height ?? -1) - msg.height,
        });

        if (validatorSocket) send(validatorSocket, msg);
        break;
      }

      // ── Light node → Railway → validator (tx relay) ──────────────
      case "tx:submit": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        if (!validatorSocket) {
          send(socket, { type: "error", message: "VALIDATOR_OFFLINE" });
          log("TX", `❌ tx:submit dropped - validator offline`, { nodeId: socket.nodeId });
          return;
        }
        log("TX", `💰 Transaction submitted`, {
          nodeId: socket.nodeId,
          hasPayload: !!msg.payload,
        });
        send(validatorSocket, msg);
        const t = telemetry.get(socket.nodeId);
        if (t) t.messagesSent++;
        break;
      }

      // ── Relay scoring: node self-reports, Railway just stores it ────
      case "relay_stats": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const t = telemetry.get(socket.nodeId);
        if (t) {
          t.avgLatencyMs = msg.avgLatencyMs;
          t.participationSeconds = msg.participationSeconds;
          t.verifiedHeaderCount = msg.verifiedHeaderCount;
          const hadFullLedger = t.hasFullLedger;
          t.hasFullLedger = msg.hasFullLedger ?? false;

          log("RELAY_STATS", `📊 Relay stats updated`, {
            nodeId: socket.nodeId,
            avgLatencyMs: msg.avgLatencyMs,
            participationSeconds: msg.participationSeconds,
            verifiedHeaderCount: msg.verifiedHeaderCount,
            hasFullLedger: msg.hasFullLedger,
            fullLedgerChanged: hadFullLedger !== (msg.hasFullLedger ?? false),
          });

          if (t.hasFullLedger !== hadFullLedger) {
            recomputeFullSyncProviders();
          }
        }
        break;
      }

      // ── Peer-to-peer full sync
      case "full_sync_request": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const target = lightNodes.get(msg.toNodeId);
        if (!target) {
          send(socket, { type: "error", message: `PEER_OFFLINE — ${msg.toNodeId}` });
          log("FULL_SYNC", `❌ Full sync request failed - peer offline`, {
            fromNodeId: socket.nodeId,
            toNodeId: msg.toNodeId,
            heightRange: `${msg.fromHeight} - ${msg.toHeight}`,
          });
          return;
        }
        log("FULL_SYNC", `🔄 Full sync request relayed`, {
          fromNodeId: socket.nodeId,
          toNodeId: msg.toNodeId,
          heightRange: `${msg.fromHeight} - ${msg.toHeight}`,
          blockCount: msg.toHeight - msg.fromHeight + 1,
        });
        send(target, { ...msg, fromNodeId: socket.nodeId });
        const t2 = telemetry.get(msg.toNodeId);
        if (t2) t2.messagesSent++;
        break;
      }

      case "full_sync_response": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const target = lightNodes.get(msg.toNodeId);
        if (!target) {
          log("FULL_SYNC", `⚠️  Full sync response dropped - requester offline`, { fromNodeId: socket.nodeId, toNodeId: msg.toNodeId });
          return;
        }
        log("FULL_SYNC", `📦 Full sync response relayed`, {
          fromNodeId: socket.nodeId,
          toNodeId: msg.toNodeId,
          blocksCount: msg.blocks?.length ?? 0,
        });
        send(target, { ...msg, fromNodeId: socket.nodeId });
        const t2 = telemetry.get(msg.toNodeId);
        if (t2) t2.messagesSent++;
        break;
      }

      // ── WebRTC signaling
      case "webrtc_offer":
      case "webrtc_answer":
      case "ice_candidate": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const target = lightNodes.get(msg.toNodeId);
        if (!target) {
          send(socket, { type: "error", message: `PEER_OFFLINE — ${msg.toNodeId}` });
          if (msg.type !== "ice_candidate") {
            log("WEBRTC", `❌ ${msg.type} failed - peer offline`, {
              fromNodeId: socket.nodeId,
              toNodeId: msg.toNodeId,
            });
          }
          return;
        }
        if (msg.type !== "ice_candidate") {
          log("WEBRTC", `🔗 ${msg.type} relayed`, {
            fromNodeId: socket.nodeId,
            toNodeId: msg.toNodeId,
            sdpLength: msg.sdp?.length ?? 0,
          });
        }
        send(target, { ...msg, fromNodeId: socket.nodeId });
        const t2 = telemetry.get(msg.toNodeId);
        if (t2) t2.messagesSent++;
        break;
      }

      default:
        log("MESSAGE", `⚠️  Unknown message type`, { type: (msg as any).type, nodeId: socket.nodeId });
        send(socket, { type: "error", message: "Unknown message type" });
    }
  });

  socket.on("close", () => {
    const duration = socket.connectedAt ? Math.round((Date.now() - socket.connectedAt) / 1000) : 0;
    const t = telemetry.get(socket.nodeId || "");

    if (socket.role === "validator") {
      validatorSocket = null;
      log("CONNECTION", `❌ Validator disconnected`, { nodeId: socket.nodeId, connectedForSeconds: duration });
    } else if (socket.nodeId) {
      lightNodes.delete(socket.nodeId);
      telemetry.delete(socket.nodeId);

      log("CONNECTION", `❌ Light node disconnected`, {
        nodeId: socket.nodeId,
        ip,
        connectedForSeconds: duration,
        remainingNodes: lightNodes.size,
        messagesReceived: t?.messagesReceived ?? 0,
        messagesSent: t?.messagesSent ?? 0,
        lastAckHeight: t?.lastAckHeight ?? -1,
      });

      // Update relay roster if needed
      if (currentRelayRoster.includes(socket.nodeId)) {
        currentRelayRoster = currentRelayRoster.filter((id) => id !== socket.nodeId);
        broadcastToLightNodes({ type: "relay:roster", relayNodeIds: currentRelayRoster });
        log("RELAY", `🔄 Relay roster updated after node disconnect`, { roster: currentRelayRoster });
      }
      if (currentFullSyncProviders.includes(socket.nodeId)) {
        recomputeFullSyncProviders();
      }
    }
  });

  socket.on("error", (err) => {
    log("SOCKET", `❌ Socket error for ${socket.nodeId}`, err);
  });
});

// Dead-connection cleanup
setInterval(() => {
  let terminated = 0;
  wss.clients.forEach((client) => {
    const ws = client as EastSocket;
    if (!ws.isAlive) {
      terminated++;
      ws.terminate();
      log("HEARTBEAT", `🔴 Dead connection terminated for ${ws.nodeId}`);
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
  if (terminated > 0) {
    log("HEARTBEAT", `⏱️  Heartbeat cleanup`, { terminatedConnections: terminated, activeConnections: wss.clients.size });
  } else {
    logDebug("HEARTBEAT", "Heartbeat check complete", { activeConnections: wss.clients.size });
  }
}, 30000);

// Rescore + re-promote relay candidates periodically
setInterval(() => {
  log("RESCORE", `🔄 Starting relay roster rescore`, { currentRosterSize: currentRelayRoster.length, totalNodes: telemetry.size });
  recomputeRelayRoster();
}, RELAY_RESCORE_INTERVAL_MS);

setInterval(() => {
  logDebug("RESCORE", `Starting full-sync providers rescore`);
  recomputeFullSyncProviders();
}, RELAY_RESCORE_INTERVAL_MS);

// Periodic status dump
setInterval(() => {
  const status = {
    timestamp: new Date().toISOString(),
    uptime: Math.round(process.uptime()),
    validatorConnected: !!validatorSocket,
    lightNodesConnected: lightNodes.size,
    latestHeight: latestHeader?.height ?? -1,
    recentHeadersCount: recentHeaders.length,
    relayRosterSize: currentRelayRoster.length,
    fullSyncProvidersCount: currentFullSyncProviders.length,
    memoryUsage: process.memoryUsage(),
    messageStats,
  };
  log("STATUS", `📈 Periodic status dump`, status);
}, 5 * 60 * 1000); // Every 5 minutes

httpServer.listen(PORT, () => {
  log("RAILWAY", `✅ EAST Hub is listening`, {
    port: PORT,
    endpoints: [
      `ws://localhost:${PORT} (WebSocket)`,
      `http://localhost:${PORT}/status (Status)`,
      `http://localhost:${PORT}/health (Health Check)`,
      `http://localhost:${PORT}/internal/publish-block (Block Publish)`,
    ],
  });
});

// Graceful shutdown
process.on("SIGTERM", () => {
  log("RAILWAY", "📡 SIGTERM received - shutting down gracefully");
  httpServer.close(() => {
    log("RAILWAY", "👋 Server closed");
    process.exit(0);
  });
});

process.on("SIGINT", () => {
  log("RAILWAY", "📡 SIGINT received - shutting down gracefully");
  httpServer.close(() => {
    log("RAILWAY", "👋 Server closed");
    process.exit(0);
  });
});

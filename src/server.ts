import { createServer, IncomingMessage, ServerResponse } from "http";
import { WebSocketServer, WebSocket } from "ws";
import {
  InboundMessage, BlockHeader, Role, EAST_CHAIN_ID,
} from "./types";

const PORT = Number(process.env.PORT || process.env.WS_PORT || 8081);
const VALIDATOR_SECRET = process.env.RAILWAY_VALIDATOR_SECRET || "";

if (!VALIDATOR_SECRET) {
  console.warn("[RAILWAY] WARNING: RAILWAY_VALIDATOR_SECRET is not set — anyone could connect as validator!");
}

// Top-N light nodes by score get promoted to "relay" — other light nodes
// prefer dialing them via WebRTC over always pulling from Railway alone.
// This is a bandwidth/resilience optimization, NOT a trust tier: every
// header a relay forwards is still independently signature-verified by
// the receiving client (see verifyHeader() in lightnode/client.ts), so a
// malicious or just-bad relay can only be USELESS, never authoritative.
const RELAY_ROSTER_SIZE = 5;
const RELAY_RESCORE_INTERVAL_MS = 60_000;

// ── Connection ceiling & per-IP rate limit ──────────────────────────
// lightNodes was previously unbounded — fine at dozens of real users, a
// real risk at thousands: one actor opening tens of thousands of bogus
// connections can exhaust RAM/CPU that legitimate users need. Tune
// MAX_LIGHT_NODES to whatever your current Railway plan can actually
// hold — this is a safety ceiling, not a target to reach.
const MAX_LIGHT_NODES = Number(process.env.MAX_LIGHT_NODES || 5000);
// Max NEW connections a single IP may open within the window below.
// Generous enough for someone with several tabs/devices on one NAT'd
// network, tight enough to blunt a single-source connection flood.
const IP_RATE_LIMIT_MAX = 10;
const IP_RATE_LIMIT_WINDOW_MS = 60_000;
const connectionAttemptsByIp = new Map<string, number[]>(); // ip -> timestamps within the window

function isRateLimited(ip: string): boolean {
  const now = Date.now();
  const recent = (connectionAttemptsByIp.get(ip) || []).filter((t) => now - t < IP_RATE_LIMIT_WINDOW_MS);
  recent.push(now);
  connectionAttemptsByIp.set(ip, recent);
  return recent.length > IP_RATE_LIMIT_MAX;
}

// Prevents connectionAttemptsByIp from growing forever with one-off IPs —
// without this it's the same unbounded-Map problem we're trying to fix.
setInterval(() => {
  const cutoff = Date.now() - IP_RATE_LIMIT_WINDOW_MS;
  connectionAttemptsByIp.forEach((timestamps, ip) => {
    const recent = timestamps.filter((t) => t > cutoff);
    if (recent.length === 0) connectionAttemptsByIp.delete(ip);
    else connectionAttemptsByIp.set(ip, recent);
  });
}, IP_RATE_LIMIT_WINDOW_MS);

interface EastSocket extends WebSocket {
  isAlive?: boolean;
  role?: Role;
  nodeId?: string;
}

// ─── State ────────────────────────────────────────────────────────
let validatorSocket: EastSocket | null = null;
const lightNodes = new Map<string, EastSocket>();
let latestHeader: BlockHeader | null = null;
const recentHeaders: BlockHeader[] = []; // rolling buffer, newest last
const BACKFILL_SIZE = 20;

// Lightweight per-node telemetry for the /status endpoint (debug only —
// this is NOT the source of truth for reward eligibility; that lives on
// the user's device and is proven when they actually claim).
interface NodeTelemetry {
  lastHeartbeat: number;
  lastAckHeight: number;
  connectedAt: number;
  avgLatencyMs: number;
  participationSeconds: number;
  verifiedHeaderCount: number;
  isRelay: boolean;
}
const telemetry = new Map<string, NodeTelemetry>();
let currentRelayRoster: string[] = [];

function score(t: NodeTelemetry): number {
  // Higher is better. Latency dominates inversely (a laggy node makes a bad
  // relay regardless of how long it's been up); uptime and verified-header
  // count are secondary tie-breakers that favor established, honest nodes.
  const latencyScore = 1000 / Math.max(t.avgLatencyMs, 1);
  return latencyScore * Math.log1p(t.participationSeconds) * Math.log1p(t.verifiedHeaderCount);
}

function recomputeRelayRoster() {
  const ranked = [...telemetry.entries()]
    .filter(([, t]) => t.avgLatencyMs > 0) // no stats reported yet — skip, don't promote blind
    .sort(([, a], [, b]) => score(b) - score(a))
    .slice(0, RELAY_ROSTER_SIZE)
    .map(([nodeId]) => nodeId);

  const newlyPromoted = ranked.filter((id) => !currentRelayRoster.includes(id));
  const newlyDemoted = currentRelayRoster.filter((id) => !ranked.includes(id));

  for (const [nodeId, t] of telemetry.entries()) t.isRelay = ranked.includes(nodeId);

  newlyPromoted.forEach((id) => { const s = lightNodes.get(id); if (s) send(s, { type: "relay:promoted" }); });
  newlyDemoted.forEach((id) => { const s = lightNodes.get(id); if (s) send(s, { type: "relay:demoted" }); });

  currentRelayRoster = ranked;
  broadcastToLightNodes({ type: "relay:roster", relayNodeIds: currentRelayRoster });

  if (newlyPromoted.length || newlyDemoted.length) {
    console.log(`[RAILWAY] Relay roster updated: [${currentRelayRoster.join(", ")}]`);
  }
}

function send(socket: EastSocket, msg: unknown) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(msg));
}

function broadcastToLightNodes(msg: unknown) {
  const json = JSON.stringify(msg);
  lightNodes.forEach((socket) => {
    if (socket.readyState === WebSocket.OPEN) socket.send(json);
  });
}

function publishBlock(header: BlockHeader) {
  latestHeader = header;
  recentHeaders.push(header);
  if (recentHeaders.length > 20) recentHeaders.shift();
  broadcastToLightNodes({ type: "block:new", header });
  console.log(`[RAILWAY] Relayed block #${header.height} to ${lightNodes.size} light node(s)`);
}

// ─── HTTP: validator (Vercel serverless) publishes a block via POST ──
// Avoids requiring Vercel functions to hold open a persistent WS
// connection — a function invocation just fires this and returns.
function handleHttp(req: IncomingMessage, res: ServerResponse) {
  if (req.method === "GET" && req.url === "/status") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      ok: true,
      validatorConnected: !!validatorSocket,
      lightNodesConnected: lightNodes.size,
      latestHeight: latestHeader?.height ?? -1,
      relayRoster: currentRelayRoster,
      nodes: [...telemetry.entries()].map(([nodeId, t]) => ({ nodeId, ...t })),
    }));
    return;
  }

  if (req.method === "POST" && req.url === "/internal/publish-block") {
    const authHeader = req.headers["x-railway-secret"];
    if (!VALIDATOR_SECRET || authHeader !== VALIDATOR_SECRET) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "UNAUTHORIZED" }));
      return;
    }
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      try {
        const { header } = JSON.parse(body) as { header: BlockHeader };
        if (!header || typeof header.height !== "number" || !header.hash) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: "INVALID_HEADER" }));
          return;
        }
        publishBlock(header);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true, relayedTo: lightNodes.size }));
      } catch {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "INVALID_JSON" }));
      }
    });
    return;
  }

  res.writeHead(404);
  res.end();
}

const httpServer = createServer(handleHttp);
// perMessageDeflate OFF on purpose: compression gives a zlib buffer PER
// CONNECTION (can be 100s of KB each), which adds up fast at 1GB RAM —
// and our messages (headers, votes, signaling) are all a few hundred
// bytes, so compression barely helps anyway. maxPayload caps a single
// message at 64KB — generous for anything we actually send, but stops a
// buggy/malicious client from forcing a huge buffer allocation.
const wss = new WebSocketServer({ server: httpServer, perMessageDeflate: false, maxPayload: 64 * 1024 });

wss.on("connection", (socket: EastSocket, req) => {
  // req.socket.remoteAddress is Railway's edge IP unless x-forwarded-for is
  // trusted — Railway's proxy does set this correctly for the real client IP.
  const ip = (req.headers["x-forwarded-for"] as string)?.split(",")[0]?.trim()
    || req.socket.remoteAddress || "unknown";

  if (isRateLimited(ip)) {
    send(socket, { type: "error", message: "RATE_LIMITED — too many connection attempts, try again shortly" });
    socket.close();
    return;
  }
  if (lightNodes.size >= MAX_LIGHT_NODES) {
    send(socket, { type: "error", message: "HUB_AT_CAPACITY" });
    socket.close();
    console.log(`[RAILWAY] Rejected connection from ${ip} — at MAX_LIGHT_NODES (${MAX_LIGHT_NODES})`);
    return;
  }

  socket.isAlive = true;

  socket.on("pong", () => { socket.isAlive = true; });

  socket.on("message", (raw) => {
    let msg: InboundMessage;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      send(socket, { type: "error", message: "Invalid JSON" });
      return;
    }

    switch (msg.type) {
      case "hello": {
        // Fail open on missing chainId (pre-upgrade client) but fail closed
        // on a WRONG chainId — that's a client pointed at the wrong network,
        // not just an old version, and shouldn't be allowed to exchange
        // headers/peers with this hub at all.
        if (msg.chainId !== undefined && msg.chainId !== EAST_CHAIN_ID) {
          send(socket, { type: "error", message: `WRONG_NETWORK — expected chainId ${EAST_CHAIN_ID}` });
          socket.close();
          return;
        }
        if (msg.role === "validator") {
          if (!VALIDATOR_SECRET || msg.secret !== VALIDATOR_SECRET) {
            send(socket, { type: "error", message: "UNAUTHORIZED" });
            socket.close();
            return;
          }
          validatorSocket = socket;
          socket.role = "validator";
          socket.nodeId = msg.nodeId || "EASTCHAIN-L1";
          console.log("[RAILWAY] Validator connected");
        } else {
          socket.role = "light-node";
          socket.nodeId = msg.nodeId;
          lightNodes.set(msg.nodeId, socket);
          telemetry.set(msg.nodeId, {
            lastHeartbeat: Date.now(), lastAckHeight: -1, connectedAt: Date.now(),
            avgLatencyMs: 0, participationSeconds: 0, verifiedHeaderCount: 0, isRelay: false,
          });
          console.log(`[RAILWAY] Light node connected: ${msg.nodeId} (${lightNodes.size} total)`);
          // Let the new node know who's currently relay-eligible right away,
          // instead of waiting up to RELAY_RESCORE_INTERVAL_MS for the next tick.
          send(socket, { type: "relay:roster", relayNodeIds: currentRelayRoster });
        }
        send(socket, {
          type: "welcome", network: "EAST", chainId: EAST_CHAIN_ID, version: "1.1",
          role: socket.role, latestHeight: latestHeader?.height ?? -1,
        });
        break;
      }

      case "ping":
        send(socket, { type: "pong", time: Date.now() });
        break;

      // ── Validator → Railway → all light nodes ──────────────────
      case "block:new": {
        if (socket.role !== "validator") {
          send(socket, { type: "error", message: "FORBIDDEN — validator role required" });
          return;
        }
        publishBlock(msg.header);
        break;
      }

      // ── Light node → Railway → validator (best-effort passthrough) ──
      case "heartbeat": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const t = telemetry.get(socket.nodeId);
        if (t) { t.lastHeartbeat = Date.now(); }
        if (validatorSocket) send(validatorSocket, msg);
        break;
      }

      case "sync_request": {
        if (socket.role !== "light-node") return;
        // Give the node a real backfill of the last few blocks so its UI
        // can show genuine "downloading + verifying" progress, instead of
        // silently waiting for the next organic block to happen to arrive.
        const backfill = recentHeaders.slice(-BACKFILL_SIZE);
        if (backfill.length > 0) {
          send(socket, { type: "block:backfill", headers: backfill });
        }
        if (validatorSocket) send(validatorSocket, msg);
        break;
      }

      case "ack": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const t = telemetry.get(socket.nodeId);
        if (t) t.lastAckHeight = msg.height;
        if (validatorSocket) send(validatorSocket, msg);
        break;
      }

      // ── Light node → Railway → validator (tx relay) ─────────────
      case "tx:submit": {
        if (socket.role !== "light-node") return;
        if (!validatorSocket) {
          send(socket, { type: "error", message: "VALIDATOR_OFFLINE" });
          console.log(`[RAILWAY] tx:submit from ${socket.nodeId} dropped — validator offline`);
          return;
        }
        console.log(`[RAILWAY] tx:submit relayed — from ${socket.nodeId}`);
        send(validatorSocket, msg);
        break;
      }

      // ── Relay scoring: node self-reports, Railway just stores it ────
      // (see recomputeRelayRoster() for why a lying node can't gain trust,
      // only waste its own shot at being picked as a relay)
      case "relay_stats": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const t = telemetry.get(socket.nodeId);
        if (t) {
          t.avgLatencyMs = msg.avgLatencyMs;
          t.participationSeconds = msg.participationSeconds;
          t.verifiedHeaderCount = msg.verifiedHeaderCount;
        }
        break;
      }

      // ── WebRTC signaling — blind passthrough by nodeId, light-node
      // only. Railway never reads sdp/candidate content. ─────────────
      case "webrtc_offer":
      case "webrtc_answer":
      case "ice_candidate": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const target = lightNodes.get(msg.toNodeId);
        if (!target) {
          send(socket, { type: "error", message: `PEER_OFFLINE — ${msg.toNodeId}` });
          console.log(`[RAILWAY] ${msg.type} from ${socket.nodeId} → ${msg.toNodeId} failed — peer offline`);
          return;
        }
        // Only log offer/answer, not every ICE candidate — a single WebRTC
        // handshake can produce a dozen+ candidates and would flood this
        // otherwise. Offer+answer alone is enough to see who's pairing up;
        // whether the DataChannel actually opens afterward is invisible to
        // Railway by design (see PeerMesh in webrtc-peer.ts client-side).
        if (msg.type !== "ice_candidate") {
          console.log(`[RAILWAY] ${msg.type} relayed: ${socket.nodeId} → ${msg.toNodeId}`);
        }
        send(target, { ...msg, fromNodeId: socket.nodeId });
        break;
      }

      default:
        send(socket, { type: "error", message: "Unknown message type" });
    }
  });

  socket.on("close", () => {
    if (socket.role === "validator") {
      validatorSocket = null;
      console.log("[RAILWAY] Validator disconnected");
    } else if (socket.nodeId) {
      lightNodes.delete(socket.nodeId);
      telemetry.delete(socket.nodeId);
      console.log(`[RAILWAY] Light node disconnected: ${socket.nodeId} (${lightNodes.size} remaining)`);
      // Don't wait for the next rescore tick to tell everyone else this
      // relay is gone — a peer mid-WebRTC-dial to a dead node just hangs.
      if (currentRelayRoster.includes(socket.nodeId)) {
        currentRelayRoster = currentRelayRoster.filter((id) => id !== socket.nodeId);
        broadcastToLightNodes({ type: "relay:roster", relayNodeIds: currentRelayRoster });
      }
    }
  });
});

// Dead-connection cleanup
setInterval(() => {
  wss.clients.forEach((client) => {
    const ws = client as EastSocket;
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// Rescore + re-promote relay candidates periodically. Runs independently of
// any block activity — a quiet chain shouldn't mean a stale relay roster.
setInterval(recomputeRelayRoster, RELAY_RESCORE_INTERVAL_MS);

httpServer.listen(PORT, () => {
  console.log(`[RAILWAY] EAST Hub listening on :${PORT} (WS + HTTP)`);
});

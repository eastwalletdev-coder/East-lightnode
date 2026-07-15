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
const BACKFILL_SIZE = 5;

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
const wss = new WebSocketServer({ server: httpServer });

wss.on("connection", (socket: EastSocket) => {
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
          return;
        }
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
          return;
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

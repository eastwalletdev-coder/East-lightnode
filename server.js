/**
 * EASTCHAIN Railway Hub
 * ----------------------
 * Standalone relay server for the Light Node network. Has nothing to do
 * with Postgres/Next.js — it does exactly two things:
 *
 *  1. HTTP  POST /publish   — Vercel (the mining server) pushes each newly
 *                             sealed block header here after every claim.
 *  2. WS    /                — Light Nodes (browsers running the Mini App)
 *                             connect here, get backfilled on the last few
 *                             headers, then receive new ones live.
 *
 * Protocol mirrors src/lib/lightnode/protocol.ts in the main app — keep
 * both in sync if you change message shapes.
 */

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const PUBLISH_SECRET = process.env.RAILWAY_VALIDATOR_SECRET || "";
const MAX_HISTORY = 50;   // how many recent headers we keep in memory
const BACKFILL_COUNT = 5; // matches MIN_VERIFIED_HEADERS stand-in in client.ts

if (!PUBLISH_SECRET) {
  console.warn("[EAST Hub] WARNING: RAILWAY_VALIDATOR_SECRET is not set — /publish will reject everything.");
}

/** @type {{height:number, hash:string, previousHash:string, merkleRoot:string, validator:string|null, timestamp:number, epoch:number}[]} */
const history = [];
let latestHeight = -1;

/** ws -> { nodeId: string, role: string, lastHeight: number } */
const clients = new Map();

function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const ws of clients.keys()) {
    if (ws.readyState === ws.OPEN) ws.send(data);
  }
}

function readBody(req, maxBytes = 1_000_000) {
  return new Promise((resolve, reject) => {
    let body = "";
    let size = 0;
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        reject(new Error("payload_too_large"));
        req.destroy();
        return;
      }
      body += chunk;
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function isValidHeader(h) {
  return h
    && typeof h.height === "number"
    && typeof h.hash === "string" && h.hash.length > 0
    && typeof h.previousHash === "string"
    && typeof h.merkleRoot === "string"
    && typeof h.timestamp === "number"
    && typeof h.epoch === "number";
}

const server = http.createServer(async (req, res) => {
  // Health check — handy for Railway's own health checks and for you to
  // eyeball connected-node count without opening a WS client.
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      status: "ok",
      connectedNodes: clients.size,
      latestHeight,
      historySize: history.length,
    }));
    return;
  }

  if (req.method === "POST" && req.url === "/publish") {
    const secret = req.headers["x-railway-secret"];
    if (!PUBLISH_SECRET || secret !== PUBLISH_SECRET) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "unauthorized" }));
      return;
    }

    try {
      const raw = await readBody(req);
      const parsed = JSON.parse(raw);
      const header = parsed.header;

      if (!isValidHeader(header)) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "invalid_header" }));
        return;
      }

      history.push(header);
      if (history.length > MAX_HISTORY) history.shift();
      latestHeight = Math.max(latestHeight, header.height);

      broadcast({ type: "block:new", header });

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true, broadcastTo: clients.size, latestHeight }));
    } catch (err) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "invalid_request", detail: String(err.message || err) }));
    }
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "not_found" }));
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (msg.type) {
      case "hello": {
        clients.set(ws, { nodeId: msg.nodeId, role: msg.role, lastHeight: -1 });
        ws.send(JSON.stringify({
          type: "welcome",
          network: "EAST",
          version: "1.0.0",
          role: msg.role,
          latestHeight,
        }));
        break;
      }

      case "sync_request": {
        const fromHeight = typeof msg.fromHeight === "number" ? msg.fromHeight : 0;
        const headers = history.filter((h) => h.height >= fromHeight).slice(-BACKFILL_COUNT);
        ws.send(JSON.stringify({ type: "block:backfill", headers }));
        break;
      }

      case "ping": {
        ws.send(JSON.stringify({ type: "pong", time: Date.now() }));
        break;
      }

      case "heartbeat": {
        const info = clients.get(ws);
        if (info) info.lastHeight = msg.height;
        break;
      }

      case "ack": {
        // Node confirms it applied a header. No response required by the
        // protocol; logged only if you need to debug node behaviour.
        break;
      }

      default:
        break;
    }
  });

  ws.on("close", () => clients.delete(ws));
  ws.on("error", () => clients.delete(ws));
});

server.listen(PORT, () => {
  console.log(`[EAST Hub] Listening on port ${PORT}`);
});

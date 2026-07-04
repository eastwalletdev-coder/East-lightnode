# EASTCHAIN Railway Hub

Standalone relay server for the Light Node network. Plain Node.js, no
Next.js, no Postgres — just an HTTP endpoint to receive new block headers
and a WebSocket server to relay them to connected Light Nodes.

## How it fits together

```
Vercel (mining-actions.ts)  --POST /publish-->  Railway Hub  --WS broadcast-->  Light Node (Mini App browser)
```

## Deploy to Railway

**Option A — new repo (simplest):**
1. Push this folder (`railway-hub/`) as its own GitHub repo.
2. In Railway: New Project → Deploy from GitHub repo → pick it.
3. Railway auto-detects Node.js and runs `npm install && npm start`.

**Option B — subfolder of the existing `east-poc` repo:**
1. Add this folder to your existing repo (e.g. at `railway-hub/`).
2. In Railway: New Project → Deploy from GitHub repo → pick `east-poc`.
3. In the new service's Settings → Source → **Root Directory**, set it to
   `railway-hub`. Railway will only build/run from that subfolder.

## Environment variables (set in Railway → Variables)

| Key | Value |
|---|---|
| `RAILWAY_VALIDATOR_SECRET` | Same random secret you'll put in Vercel's `RAILWAY_VALIDATOR_SECRET` — must match exactly on both sides. |

`PORT` is provided automatically by Railway — don't set it yourself.

## After deploying

1. Railway gives you a public domain, e.g. `eastchain-hub.up.railway.app`.
   Go to Settings → Networking → make sure a public domain is generated.
2. Set these in **Vercel** (Settings → Environment Variables), then redeploy:

| Key | Value |
|---|---|
| `RAILWAY_PUBLISH_URL` | `https://eastchain-hub.up.railway.app/publish` |
| `RAILWAY_VALIDATOR_SECRET` | the exact same secret you set on Railway |
| `NEXT_PUBLIC_RAILWAY_WS_URL` | `wss://eastchain-hub.up.railway.app` (note `wss://`, not `https://`) |

## Sanity checks

- `curl https://<your-domain>/health` → should return
  `{"status":"ok","connectedNodes":0,"latestHeight":-1,"historySize":0}`
- After a real mining claim goes through on Vercel, `latestHeight` here
  should bump up and `historySize` should grow.
- Open the Mini App → Profile → Network Relay → tap Reconnect. Status
  should flip to `CONNECTED`, and `connectedNodes` on `/health` should
  go up by 1.

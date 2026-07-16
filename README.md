# EAST Railway Hub

Pure relay — **not** part of consensus. Sits between the L1 validator
(EASTCHAIN on Vercel) and every connected Light Node.

```
Validator (Vercel, L1)
   │  POST /internal/publish-block  (x-railway-secret header)
   ▼
Railway Hub  ───────────────►  broadcasts block:new to every Light Node (WS)
   ▲
   │  heartbeat / sync_request / ack / tx:submit  (WS, forwarded best-effort)
Light Node (user's phone, browser WS client)
```

## Deploy on Railway
1. Push this folder as its own repo (or a Railway service pointed at this subfolder).
2. Railway auto-detects `package.json` → runs `npm run build` then `npm start`.
3. Set environment variables in Railway:
   - `RAILWAY_VALIDATOR_SECRET` — random long string. Must match the value
     Vercel uses when it POSTs to `/internal/publish-block`.
   - `PORT` — Railway sets this automatically, no need to set manually.
4. Note the public URL Railway gives you (e.g. `https://east-hub-production.up.railway.app`).
   - WS endpoint for Light Nodes: `wss://<that-domain>/`
   - HTTP publish endpoint for Vercel: `https://<that-domain>/internal/publish-block`
   - Debug status: `GET https://<that-domain>/status`

## On the Vercel (Next.js) side
Set these env vars:
- `RAILWAY_PUBLISH_URL=https://<domain>/internal/publish-block`
- `RAILWAY_VALIDATOR_SECRET=<same value as above>`
- `NEXT_PUBLIC_RAILWAY_WS_URL=wss://<domain>/` (public — used by the browser Light Node client)

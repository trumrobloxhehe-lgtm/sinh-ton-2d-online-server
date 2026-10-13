# Sinh Ton 2D Survivor — Online Server

WebSocket server for random matchmaking, private rooms, lobby/match chat, player-state relay, and basic PvP messages.

## Deploy to Render

1. Open https://dashboard.render.com and sign in with GitHub.
2. Choose **New + → Blueprint**.
3. Select repository `trumrobloxhehe-lgtm/sinh-ton-2d-online-server`.
4. Render reads `render.yaml`; choose the Free plan for testing if prompted.
5. Wait until the service is **Live**.
6. Open `https://YOUR-SERVICE.onrender.com/health`. A working service returns JSON containing `"ok": true`.
7. The WebSocket URL is `wss://YOUR-SERVICE.onrender.com/ws`. Use the exact hostname Render assigned, not the example text.

## Local test

Requires Node.js 18 or newer:

```bash
npm install
npm start
```

Then open `http://localhost:3000/health`. WebSocket endpoint: `ws://localhost:3000/ws`.

## Current limitations

- Matchmaking pairs two random players in the same mode (Co-op or PvP).
- Supports lobby and match chat, private rooms, ready/start messages, and basic player position/health state relay.
- Co-op is still a prototype: enemies, XP, items, and the full PvE simulation are not authoritative/shared across clients.
- Room and matchmaking state is kept in server memory and is lost if the service restarts.
- Render Free may sleep after inactivity, so the first connection can take a while to start.

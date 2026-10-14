'use strict';
const http = require('node:http');
const https = require('node:https');
const { randomUUID } = require('node:crypto');
const { WebSocket, WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const MAX_ROOM_PLAYERS = 4;
const MAX_CHAT_LENGTH = 180;
const COLORS = ['#3b82f6', '#c084fc', '#34d399', '#fb923c'];
const ROOM_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const rooms = new Map();
const queues = { coop: [], pvp: [] };
const qaRateByAddress = new Map();
const QA_WINDOW_MS = 72 * 60 * 60 * 1000;
const QA_BUFFER_MAX = 3000;
const qaBootAt = Date.now();
const qaEvents = [];
const DISCORD_WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL || '';
const DISCORD_NOTIFY_COOLDOWN_MS = 60_000;
let lastDiscordNotifyAt = 0;
function notifyDiscordMatchmaking(mode) {
  if (!DISCORD_WEBHOOK_URL) return;
  const now = Date.now();
  if (now - lastDiscordNotifyAt < DISCORD_NOTIFY_COOLDOWN_MS) return;
  let webhook;
  try {
    webhook = new URL(DISCORD_WEBHOOK_URL);
    if (webhook.protocol !== 'https:' ||
        !['discord.com', 'discordapp.com', 'canary.discord.com', 'ptb.discord.com'].includes(webhook.hostname) ||
        !webhook.pathname.startsWith('/api/webhooks/')) return;
  } catch (_) { return; }
  lastDiscordNotifyAt = now;
  const time = new Date(now).toLocaleString('vi-VN', { timeZone: 'Asia/Ho_Chi_Minh' });
  const body = JSON.stringify({
    content: '🔎 Sinh Tồn 2D: Có người đang tìm đồng đội ' +
      (mode === 'pvp' ? 'PvP' : 'CO-OP') + ' lúc ' + time + ' (giờ Việt Nam).',
    allowed_mentions: { parse: [] }
  });
  const req = https.request(webhook, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) },
    timeout: 5000
  }, res => {
    res.resume();
    if (res.statusCode < 200 || res.statusCode >= 300) {
      console.error('Discord webhook returned HTTP ' + res.statusCode);
    }
  });
  req.on('timeout', () => req.destroy(new Error('Discord webhook timeout')));
  req.on('error', err => console.error('Discord notification failed: ' + (err.message || 'request error')));
  req.write(body);
  req.end();
}
function qaRecord(event) {
  const stored = { at: Date.now(), ...event };
  qaEvents.push(stored);
  if (qaEvents.length > QA_BUFFER_MAX) qaEvents.splice(0, qaEvents.length - QA_BUFFER_MAX);
}
function makeQaReport(windowHours = 72) {
  const hours = Math.max(1, Math.min(72, Number(windowHours) || 72));
  const cutoff = Date.now() - hours * 60 * 60 * 1000;
  const events = qaEvents.filter(e => e.at >= cutoff);
  const counts = {};
  const sessions = new Set();
  const errorMap = new Map();
  let fpsSum = 0, fpsSamples = 0, lowFps = 0, minFps = null, worstFrameMs = null;
  const network = { packets: 0, projectilesSent: 0, projectilesReceived: 0, worldSnapshotsReceived: 0, playerStatesReceived: 0, serverErrorsReceived: 0, websocketOpens: 0, websocketErrors: 0, websocketCloses: 0 };
  for (const e of events) {
    counts[e.event] = (counts[e.event] || 0) + 1;
    if (e.session) sessions.add(e.session);
    if (e.event === 'online_event_batch') {
      network.packets += e.packetCount || 0;
      network.projectilesSent += e.projectilesTx || 0;
      network.projectilesReceived += e.projectilesRx || 0;
      network.worldSnapshotsReceived += e.snapshotsRx || 0;
      network.playerStatesReceived += e.stateRx || 0;
      network.serverErrorsReceived += e.errorsRx || 0;
    }
    if (e.event === 'online_ws_open') network.websocketOpens++;
    if (e.event === 'online_ws_error') network.websocketErrors++;
    if (e.event === 'online_ws_close') network.websocketCloses++;
    if (e.event === 'fps_sample' && Number.isFinite(e.fps)) {
      fpsSum += e.fps;
      fpsSamples++;
      minFps = minFps === null ? (e.minFps ?? e.fps) : Math.min(minFps, e.minFps ?? e.fps);
      worstFrameMs = worstFrameMs === null ? (e.frameMs ?? null) : Math.max(worstFrameMs, e.frameMs ?? 0);
      if (e.fps < 50 || (e.minFps ?? e.fps) < 35) lowFps++;
    }
    if (e.event === 'js_error' || e.event === 'promise_rejection' || e.event === 'online_ws_error') {
      const msg = qaText(e.message || e.event, 120) || e.event;
      errorMap.set(msg, (errorMap.get(msg) || 0) + 1);
    }
  }
  const topErrors = [...errorMap.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([message, count]) => ({ message, count }));
  return {
    generatedAt: new Date().toISOString(),
    windowHours: hours,
    windowStart: new Date(cutoff).toISOString(),
    serverStartedAt: new Date(qaBootAt).toISOString(),
    telemetryEventsObserved: events.length,
    telemetryBufferCapacity: QA_BUFFER_MAX,
    uniquePageSessionsObserved: sessions.size,
    eventCounts: counts,
    performance: {
      fpsSamples,
      averageFps: fpsSamples ? Math.round(fpsSum / fpsSamples * 10) / 10 : null,
      lowestReportedFps: minFps === null ? null : Math.round(minFps * 10) / 10,
      lowFpsSamples: lowFps,
      worstReportedFrameMs: worstFrameMs === null ? null : Math.round(worstFrameMs * 10) / 10
    },
    network,
    topErrors,
    limitations: 'In-memory aggregate resets on server restart or sleep; it is not a complete historical audit. Only clients using the instrumented game build are counted.'
  };
}

let qaGlobalWindowStart = Date.now();
let qaGlobalCount = 0;
const QA_EVENTS = new Set([
  'client_loaded', 'gameplay_start', 'gameplay_end', 'js_error', 'promise_rejection',
  'fps_sample', 'online_ws_open', 'online_ws_error', 'online_ws_close',
  'online_event_batch', 'page_visibility'
]);
function qaNumber(value, min, max) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : null;
}
function qaText(value, max = 180) {
  return String(value == null ? '' : value)
    .replace(/[\u0000-\u001f\u007f]/g, ' ')
    .replace(/https?:\/\/\S+/gi, '[url]')
    .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, '[email]')
    .replace(/\s+/g, ' ').trim().slice(0, max);
}
function qaCleanPayload(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return null;
  const event = String(input.event || '');
  if (!QA_EVENTS.has(event)) return null;
  const out = {
    event,
    session: /^[a-f0-9-]{16,40}$/i.test(String(input.session || '')) ? String(input.session).slice(0, 40) : undefined,
    atClient: qaText(input.at, 32) || undefined,
    build: /^[a-z0-9._-]{1,32}$/i.test(String(input.build || '')) ? String(input.build) : undefined
  };
  const enums = {
    mode: new Set(['solo', 'coop', 'pvp', 'unknown']),
    source: new Set(['window_error', 'unhandled_rejection']),
    errorKind: new Set(['network', 'parse', 'runtime', 'unknown'])
  };
  for (const k of ['mode', 'source', 'errorKind']) {
    const v = String(input[k] || '');
    if (enums[k].has(v)) out[k] = v;
  }
  for (const [k, range] of Object.entries({
    fps: [0, 240], minFps: [0, 240], frameMs: [0, 2000], stage: [1, 100],
    wave: [1, 20], enemyCount: [0, 500], bulletCount: [0, 3000],
    durationSec: [0, 86400], closeCode: [0, 65535], snapshotsRx: [0, 100000],
    projectilesRx: [0, 100000], projectilesTx: [0, 100000],
    stateRx: [0, 100000], errorsRx: [0, 10000], packetCount: [0, 100000]
  })) {
    const n = qaNumber(input[k], range[0], range[1]);
    if (n !== null) out[k] = Math.round(n * 100) / 100;
  }
  if (typeof input.wasClean === 'boolean') out.wasClean = input.wasClean;
  if (typeof input.visible === 'boolean') out.visible = input.visible;
  if (input.message) out.message = qaText(input.message, 180);
  if (input.line != null) out.line = Math.round(qaNumber(input.line, 0, 100000) || 0);
  return out;
}
function qaTelemetryAllowed(req) {
  const now = Date.now();
  if (now - qaGlobalWindowStart >= 60000) { qaGlobalWindowStart = now; qaGlobalCount = 0; }
  if (qaGlobalCount >= 1200) return false;
  qaGlobalCount++;
  const address = String(req.socket.remoteAddress || 'unknown').slice(0, 80);
  let entry = qaRateByAddress.get(address);
  if (!entry || now - entry.start >= 60000) entry = { start: now, count: 0 };
  entry.count++;
  qaRateByAddress.set(address, entry);
  if (qaRateByAddress.size > 5000) {
    for (const [key, value] of qaRateByAddress) if (now - value.start > 60000) qaRateByAddress.delete(key);
    while (qaRateByAddress.size > 5000) qaRateByAddress.delete(qaRateByAddress.keys().next().value);
  }
  return entry.count <= 12;
}
function qaCorsHeaders() {
  return {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'POST, OPTIONS',
    'access-control-allow-headers': 'content-type',
    'access-control-max-age': '86400',
    'cache-control': 'no-store'
  };
}


function send(ws, packet) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(packet)); } catch (_) {}
  }
}
function safeName(value) {
  return String(value || 'Player').replace(/[<>\u0000-\u001f\u007f]/g, '').trim().slice(0, 18) || 'Player';
}
function safeChat(value) {
  return String(value || '').replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '').trim().slice(0, MAX_CHAT_LENGTH);
}
function fail(ws, message) { send(ws, { type: 'error', message }); }
function makeCode() {
  let code;
  do {
    code = '';
    for (let i = 0; i < 5; i++) code += ROOM_CHARS[Math.floor(Math.random() * ROOM_CHARS.length)];
  } while (rooms.has(code));
  return code;
}
function publicPlayer(p) {
  return { id: p.id, name: p.name, slot: p.slot, color: p.color, x: p.x, y: p.y, hp: p.hp, maxHp: p.maxHp, level: p.level, unlockedStage: p.unlockedStage, selectedStage: p.selectedStage };
}
function playerList(room) {
  return [...room.players.values()].map(publicPlayer).sort((a, b) => a.slot - b.slot);
}
function broadcast(room, packet, except = null) {
  const raw = JSON.stringify(packet);
  for (const p of room.players.values()) {
    if (p.socket !== except && p.socket.readyState === WebSocket.OPEN) {
      try { p.socket.send(raw); } catch (_) {}
    }
  }
}
function lobbyPacket(room) {
  return {
    type: 'lobby', roomCode: room.code, mode: room.mode, hostId: room.hostId,
    started: room.started, matchmaking: room.matchmaking, readyIds: [...room.readyIds],
    players: playerList(room)
  };
}
function broadcastLobby(room) { broadcast(room, lobbyPacket(room)); }
function getSkillPickers(room) {
  return [...room.skillPickers.values()].map(entry => ({
    playerId: entry.playerId, slot: entry.slot, deadlineAt: entry.deadlineAt
  }));
}
function broadcastSkillPauseState(room, playerId, active, timedOut = false) {
  const p = room.players.get(playerId);
  broadcast(room, {
    type: 'coopSkillPause',
    active: !!active,
    playerId,
    slot: p ? p.slot : (room.skillPickers.get(playerId)?.slot || 0),
    deadlineAt: room.skillPickers.get(playerId)?.deadlineAt || Date.now(),
    timedOut: !!timedOut,
    activePickers: getSkillPickers(room)
  });
}
function endSkillPick(room, playerId, timedOut = false) {
  const entry = room.skillPickers.get(playerId);
  if (entry && entry.timer) clearTimeout(entry.timer);
  room.skillPickers.delete(playerId);
  broadcastSkillPauseState(room, playerId, false, timedOut);
}
function clampNumber(value, min, max, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback;
}
function cleanId(value) {
  const id = String(value || '').slice(0, 40);
  return /^[A-Za-z0-9_-]{1,40}$/.test(id) ? id : '';
}
function cleanWorldSnapshot(msg, room) {
  const seenEnemies = new Set();
  const enemies = (Array.isArray(msg.enemies) ? msg.enemies : []).slice(0, 70).map(e => {
    if (!e || typeof e !== 'object') return null;
    const id = cleanId(e.id);
    const k = String(e.k || '').slice(0, 32);
    if (!id || !/^[A-Z0-9_]+$/.test(k) || seenEnemies.has(id)) return null;
    seenEnemies.add(id);
    return {
      id, k, b: e.b ? 1 : 0,
      x: clampNumber(e.x, -100, 1100), y: clampNumber(e.y, -100, 800),
      r: clampNumber(e.r, 4, 150, 12), h: clampNumber(e.h, 0, 100000),
      m: clampNumber(e.m, 1, 100000, 1), d: clampNumber(e.d, 0, 10000),
      e: clampNumber(e.e, 0, 10000), fa: clampNumber(e.fa, -1000, 1000),
      ah: e.ah ? 1 : 0, hf: clampNumber(e.hf, 0, 30), sf: clampNumber(e.sf, 0, 30),
      st: e.st ? 1 : 0, la: clampNumber(e.la, -1000, 1000),
      so: clampNumber(e.so, -100000, 100000), at: clampNumber(e.at, 0, 100000000),
      df: clampNumber(e.df, 0, 10000), h2: e.h2 ? 1 : 0
    };
  }).filter(Boolean);

  const seenDrops = new Set();
  const previousCollected = room.collectedDrops || new Set();
  const drops = (Array.isArray(msg.drops) ? msg.drops : []).slice(0, 80).map(d => {
    if (!d || typeof d !== 'object') return null;
    const id = cleanId(d.id);
    if (!id || seenDrops.has(id) || previousCollected.has(id)) return null;
    if (d.t !== 'E' && d.t !== 'G') return null;
    seenDrops.add(id);
    return { id, x: clampNumber(d.x, -100, 1100), y: clampNumber(d.y, -100, 800), t: d.t, v: clampNumber(d.v, 0, 100000) };
  }).filter(Boolean);

  return {
    stage: Math.floor(clampNumber(msg.stage, 1, 100, 1)),
    wave: Math.floor(clampNumber(msg.wave, 1, 20, 1)),
    gameState: msg.gameState === 'GAMEOVER' ? 'GAMEOVER' : 'PLAYING',
    won: !!msg.won, enemies, drops
  };
}
function makeRoom(mode, matchmaking = false) {
  const room = {
    code: makeCode(), mode, matchmaking, players: new Map(),
    hostId: null, started: false, readyIds: new Set(), createdAt: Date.now(),
    world: { stage: 1, wave: 1, gameState: 'PLAYING', won: false, enemies: [], drops: [] },
    collectedDrops: new Set(), skillPickers: new Map()
  };
  rooms.set(room.code, room);
  return room;
}
function addPlayer(room, ws, name, slot) {
  const p = {
    id: randomUUID().slice(0, 8), socket: ws, name: safeName(name), slot,
    color: COLORS[slot - 1], x: 500, y: 350, hp: 100, maxHp: 100, level: 1,
    unlockedStage: 1, selectedStage: 1, lastChatAt: 0, lastStateAt: 0, lastAttackAt: 0
  };
  room.players.set(p.id, p);
  if (!room.hostId) room.hostId = p.id;
  ws.playerRef = { room, player: p };
  ws.queueRef = null;
  return p;
}
function joinedPacket(room, p) {
  return {
    type: 'joined', roomCode: room.code, playerId: p.id, slot: p.slot,
    isHost: room.hostId === p.id, hostId: room.hostId, mode: room.mode,
    started: room.started, matchmaking: room.matchmaking,
    readyIds: [...room.readyIds], players: playerList(room)
  };
}
function removeFromQueue(ws, notify = false) {
  if (!ws.queueRef) return;
  const mode = ws.queueRef.mode;
  const q = queues[mode];
  const index = q.findIndex(entry => entry.socket === ws);
  if (index >= 0) q.splice(index, 1);
  ws.queueRef = null;
  if (notify) send(ws, { type: 'queueCancelled' });
}
function closePlayer(ws) {
  removeFromQueue(ws);
  if (!ws.playerRef) return;
  const { room, player } = ws.playerRef;
  room.players.delete(player.id);
  room.readyIds.delete(player.id);
  if (room.skillPickers.has(player.id)) endSkillPick(room, player.id, true);
  ws.playerRef = null;
  if (room.players.size === 0) {
    rooms.delete(room.code);
    return;
  }
  if (room.hostId === player.id) {
    room.hostId = [...room.players.values()].sort((a, b) => a.slot - b.slot)[0].id;
  }
  broadcast(room, { type: 'system', event: 'playerLeft', name: player.name });
  broadcastLobby(room);
}
function startMatch(room, stage = 1) {
  if (room.started) return;
  room.started = true;
  const lowestUnlocked = room.players.size ? Math.min(...[...room.players.values()].map(p => Math.max(1, Math.min(100, Math.floor(Number(p.unlockedStage) || 1))))) : 1;
  const requestedStage = Math.floor(Number(stage) || Number(room.players.get(room.hostId)?.selectedStage) || 1);
  const safeStage = Math.max(1, Math.min(100, requestedStage, lowestUnlocked));
  room.collectedDrops.clear();
  for (const entry of room.skillPickers.values()) if (entry.timer) clearTimeout(entry.timer);
  room.skillPickers.clear();
  room.world = { stage: safeStage, wave: 1, gameState: 'PLAYING', won: false, enemies: [], drops: [] };
  broadcast(room, {
    type: 'matchStart', mode: room.mode, stage: safeStage, roomCode: room.code,
    hostId: room.hostId, matchmaking: room.matchmaking, players: playerList(room)
  });
  broadcastLobby(room);
}
function tryPairQueue(mode) {
  const q = queues[mode];
  for (let i = q.length - 1; i >= 0; i--) {
    if (q[i].socket.readyState !== WebSocket.OPEN || !q[i].socket.queueRef) q.splice(i, 1);
  }
  while (q.length >= 2) {
    const first = q.splice(Math.floor(Math.random() * q.length), 1)[0];
    const second = q.splice(Math.floor(Math.random() * q.length), 1)[0];
    if (!first || !second || first.socket === second.socket) continue;
    if (first.socket.readyState !== WebSocket.OPEN || second.socket.readyState !== WebSocket.OPEN) continue;
    const room = makeRoom(mode, true);
    const a = addPlayer(room, first.socket, first.name, 1);
    const b = addPlayer(room, second.socket, second.name, 2);
    if (Math.random() < 0.5) room.hostId = b.id;
    send(a.socket, joinedPacket(room, a));
    send(b.socket, joinedPacket(room, b));
    broadcast(room, { type: 'system', event: 'matchFound' });
    broadcastLobby(room);
  }
}

const httpServer = http.createServer((req, res) => {
  const path = new URL(req.url, 'http://' + (req.headers.host || 'localhost')).pathname;
  if (path === '/qa/report') {
    const headers = { ...qaCorsHeaders(), 'access-control-allow-methods': 'GET, OPTIONS', 'content-type': 'application/json; charset=utf-8' };
    if (req.method === 'OPTIONS') { res.writeHead(204, headers); res.end(); return; }
    if (req.method !== 'GET') { res.writeHead(405, headers); res.end(JSON.stringify({ ok: false, error: 'GET required' })); return; }
    const requestedHours = Number(new URL(req.url, 'http://' + (req.headers.host || 'localhost')).searchParams.get('windowHours') || 72);
    res.writeHead(200, headers);
    res.end(JSON.stringify(makeQaReport(requestedHours)));
    return;
  }
  if (path === '/qa/telemetry') {
    const headers = qaCorsHeaders();
    if (req.method === 'OPTIONS') {
      res.writeHead(204, headers);
      res.end();
      return;
    }
    if (req.method !== 'POST') {
      res.writeHead(405, { ...headers, 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: false, error: 'POST required' }));
      return;
    }
    if (!qaTelemetryAllowed(req)) {
      res.writeHead(429, { ...headers, 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: false, error: 'Rate limited' }));
      return;
    }
    let body = '';
    let tooLarge = false;
    req.on('data', chunk => {
      body += chunk.toString('utf8');
      if (body.length > 8192) { tooLarge = true; req.destroy(); }
    });
    req.on('end', () => {
      if (tooLarge) return;
      let incoming;
      try { incoming = JSON.parse(body); } catch (_) {
        res.writeHead(400, { ...headers, 'content-type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON' }));
        return;
      }
      const event = qaCleanPayload(incoming);
      if (!event) {
        res.writeHead(400, { ...headers, 'content-type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Unsupported telemetry event' }));
        return;
      }
      // Address is used only in a short-lived rate-limit map. Never log IP, player name, chat, or room code.
      qaRecord(event);
      const { session: _session, ...logEvent } = event;
      console.log('QA_EVENT ' + JSON.stringify({ at: new Date().toISOString(), ...logEvent }));
      res.writeHead(204, headers);
      res.end();
    });
    req.on('error', () => {});
    return;
  }
  if (path === '/health') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ ok: true, service: 'sinh-ton-2d-online', rooms: rooms.size, waiting: { coop: queues.coop.length, pvp: queues.pvp.length }, uptimeSeconds: Math.floor(process.uptime()) }));
    return;
  }
  if (path === '/') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    res.end('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Sinh Ton 2D Online Server</title><body style="font:16px system-ui;background:#0f172a;color:#e2e8f0;padding:32px"><h1>Server is running</h1><p>Sinh Ton 2D Survivor matchmaking and chat service.</p><p>Health: <a style="color:#67e8f9" href="/health">/health</a></p><p>WebSocket endpoint: <code>/ws</code></p></body></html>');
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});
const wss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 });
httpServer.on('upgrade', (req, socket, head) => {
  const path = new URL(req.url, 'http://' + (req.headers.host || 'localhost')).pathname;
  if (path !== '/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (_) { fail(ws, 'Invalid message.'); return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'cancelMatchmake') {
      if (ws.playerRef) { fail(ws, 'Already matched. Leave the room to search again.'); return; }
      removeFromQueue(ws, true);
      return;
    }
    if (msg.type === 'matchmake') {
      if (ws.playerRef) { fail(ws, 'Leave your current room before matchmaking.'); return; }
      if (ws.queueRef) { send(ws, { type: 'queueWaiting', mode: ws.queueRef.mode }); return; }
      const mode = msg.mode === 'pvp' ? 'pvp' : 'coop';
      const name = safeName(msg.name);
      ws.queueRef = { mode, name, queuedAt: Date.now() };
      queues[mode].push({ socket: ws, mode, name, queuedAt: ws.queueRef.queuedAt });
      notifyDiscordMatchmaking(mode);
      send(ws, { type: 'queueWaiting', mode });
      tryPairQueue(mode);
      return;
    }
    if (msg.type === 'join') {
      if (ws.playerRef) { fail(ws, 'This connection already joined a room.'); return; }
      if (ws.queueRef) { fail(ws, 'Cancel matchmaking before joining a room.'); return; }
      const requested = String(msg.roomCode || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
      const mode = msg.mode === 'pvp' ? 'pvp' : 'coop';
      let room;
      if (!requested) room = makeRoom(mode, false);
      else {
        room = rooms.get(requested);
        if (!room) { fail(ws, 'Room not found. Check the code or create a room.'); return; }
        if (room.mode !== mode) { fail(ws, 'This room uses a different mode.'); return; }
        if (room.started) { fail(ws, 'This match has already started.'); return; }
        if (room.matchmaking) { fail(ws, 'Random matchmaking rooms are closed to additional players.'); return; }
      }
      if (room.players.size >= MAX_ROOM_PLAYERS) { fail(ws, 'Room is full (maximum 4 players).'); return; }
      const used = new Set([...room.players.values()].map(p => p.slot));
      let slot = 1;
      while (used.has(slot) && slot <= MAX_ROOM_PLAYERS) slot++;
      const p = addPlayer(room, ws, msg.name, slot);
      send(ws, joinedPacket(room, p));
      broadcastLobby(room);
      return;
    }

    if (!ws.playerRef) { fail(ws, 'Join a room or start matchmaking first.'); return; }
    const { room, player } = ws.playerRef;
    if (!room.players.has(player.id)) { fail(ws, 'You are no longer in this room.'); return; }

    if (msg.type === 'leave') {
      send(ws, { type: 'roomLeft' });
      closePlayer(ws);
      return;
    }
    if (msg.type === 'chat') {
      const now = Date.now();
      if (now - player.lastChatAt < 400) { fail(ws, 'Please wait before sending another message.'); return; }
      const text = safeChat(msg.text);
      if (!text) return;
      player.lastChatAt = now;
      broadcast(room, { type: 'chat', roomCode: room.code, playerId: player.id, name: player.name, text, at: now });
      return;
    }
    if (msg.type === 'ready') {
      if (!room.matchmaking || room.started) { fail(ws, 'Ready is only available in an unstarted matchmaking room.'); return; }
      if (msg.ready === false) room.readyIds.delete(player.id);
      else room.readyIds.add(player.id);
      broadcastLobby(room);
      if (room.players.size >= 2 && [...room.players.keys()].every(id => room.readyIds.has(id))) { const host = room.players.get(room.hostId); startMatch(room, host ? host.selectedStage : 1); }
      return;
    }
    if (msg.type === 'start') {
      if (room.matchmaking) { fail(ws, 'Both random-match players should press Ready.'); return; }
      if (room.hostId !== player.id) { fail(ws, 'Only the host can start the match.'); return; }
      startMatch(room, msg.stage);
      return;
    }
    if (msg.type === 'progress') {
      player.unlockedStage = Math.floor(clampNumber(msg.unlockedStage, 1, 100, player.unlockedStage || 1));
      player.selectedStage = Math.floor(clampNumber(msg.selectedStage, 1, player.unlockedStage, player.selectedStage || 1));
      // The teammate with more unlocked stages is the stage leader before a match starts.
      if (!room.started) {
        const leader = [...room.players.values()].sort((a, b) => b.unlockedStage - a.unlockedStage || a.slot - b.slot)[0];
        if (leader) room.hostId = leader.id;
        broadcastLobby(room);
      }
      return;
    }
    if (msg.type === 'state') {
      const now = Date.now();
      if (now - player.lastStateAt < 45) return;
      player.lastStateAt = now;
      const state = msg.state || {};
      const x = Number(state.x), y = Number(state.y);
      if (Number.isFinite(x)) player.x = Math.max(16, Math.min(984, x));
      if (Number.isFinite(y)) player.y = Math.max(16, Math.min(684, y));
      const maxHp = Number(state.maxHp);
      if (Number.isFinite(maxHp)) player.maxHp = Math.max(1, Math.min(10000, maxHp));
      const level = Number(state.level);
      if (Number.isFinite(level)) player.level = Math.max(1, Math.min(999, Math.floor(level)));
      const hp = Number(state.hp);
      if (Number.isFinite(hp)) player.hp = Math.max(0, Math.min(player.maxHp, hp));
      broadcast(room, { type: 'playerState', player: publicPlayer(player) }, ws);
      return;
    }
    if (msg.type === 'projectiles') {
      if (!room.started || !Array.isArray(msg.projectiles)) return;
      const now = Date.now();
      if (!player.projectileWindow || now - player.projectileWindow >= 1000) {
        player.projectileWindow = now;
        player.projectileCount = 0;
      }
      const projectiles = [];
      for (const shot of msg.projectiles.slice(0, 32)) {
        if (!shot || typeof shot !== 'object' || player.projectileCount >= 140) break;
        const kind = shot.kind === 'sword' ? 'sword' : 'bullet';
        const x = Number(shot.x), y = Number(shot.y), vx = Number(shot.vx), vy = Number(shot.vy);
        if (![x, y, vx, vy].every(Number.isFinite)) continue;
        if (Math.abs(vx) > 12 || Math.abs(vy) > 12) continue;
        const color = typeof shot.color === 'string' && /^#[0-9a-fA-F]{6}$/.test(shot.color)
          ? shot.color : player.color;
        projectiles.push({
          x: clampNumber(x, -100, 1100), y: clampNumber(y, -100, 800),
          vx, vy, kind, color,
          radius: clampNumber(shot.radius, 2, 14, kind === 'sword' ? 13 : 3)
        });
        player.projectileCount++;
      }
      if (projectiles.length) broadcast(room, { type: 'projectiles', playerId: player.id, projectiles }, ws);
      return;
    }
    if (msg.type === 'coopSkillPause') {
      if (room.mode !== 'coop' || !room.started) return;
      if (msg.active === true) {
        const existing = room.skillPickers.get(player.id);
        if (existing && existing.timer) clearTimeout(existing.timer);
        const entry = { playerId: player.id, slot: player.slot, deadlineAt: Date.now() + 4000, timer: null };
        entry.timer = setTimeout(() => {
          if (room.skillPickers.get(player.id) !== entry) return;
          room.skillPickers.delete(player.id);
          broadcastSkillPauseState(room, player.id, false, true);
        }, 4500);
        room.skillPickers.set(player.id, entry);
        broadcastSkillPauseState(room, player.id, true, false);
      } else {
        endSkillPick(room, player.id, false);
      }
      return;
    }
    if (msg.type === 'worldSnapshot') {
      if (room.mode !== 'coop' || !room.started || room.hostId !== player.id) return;
      const now = Date.now();
      if (now - (player.lastWorldAt || 0) < 90) return;
      player.lastWorldAt = now;
      room.world = cleanWorldSnapshot(msg, room);
      broadcast(room, { type: 'worldSnapshot', ...room.world }, ws);
      return;
    }
    if (msg.type === 'enemyDamage') {
      if (room.mode !== 'coop' || !room.started || room.hostId === player.id) return;
      const enemyId = cleanId(msg.enemyId);
      if (!enemyId || !room.world.enemies.some(e => e.id === enemyId)) return;
      const now = Date.now();
      if (!player.enemyDamageWindow || now - player.enemyDamageWindow >= 1000) {
        player.enemyDamageWindow = now;
        player.enemyDamageCount = 0;
      }
      player.enemyDamageCount = (player.enemyDamageCount || 0) + 1;
      if (player.enemyDamageCount > 30) return;
      const amount = clampNumber(msg.amount, 0, 100);
      if (amount <= 0) return;
      const host = room.players.get(room.hostId);
      if (host) send(host.socket, {
        type: 'enemyDamageRequest', enemyId, amount,
        angle: Number.isFinite(Number(msg.angle)) ? clampNumber(msg.angle, -1000, 1000) : null,
        ignoreArmor: !!msg.ignoreArmor, attackerId: player.id
      });
      return;
    }
    if (msg.type === 'coopPlayerDamage') {
      if (room.mode !== 'coop' || !room.started || room.hostId !== player.id) return;
      const target = room.players.get(String(msg.targetId || ''));
      if (!target || target.id === player.id) return;
      const amount = clampNumber(msg.amount, 0, 999);
      if (amount <= 0) return;
      send(target.socket, { type: 'playerDamage', amount });
      return;
    }
    if (msg.type === 'coopPickup') {
      if (room.mode !== 'coop' || !room.started) return;
      const dropId = cleanId(msg.dropId);
      const drop = dropId ? room.world.drops.find(d => d.id === dropId) : null;
      if (!drop || room.collectedDrops.has(dropId)) {
        send(ws, { type: 'coopPickupDenied', dropId });
        return;
      }
      // Position values are server-maintained from the player's recent state packets.
      if (Math.hypot(player.x - drop.x, player.y - drop.y) > 130) {
        send(ws, { type: 'coopPickupDenied', dropId });
        return;
      }
      room.collectedDrops.add(dropId);
      room.world.drops = room.world.drops.filter(d => d.id !== dropId);
      broadcast(room, { type: 'dropRemoved', dropId });
      send(ws, { type: 'xpAward', drop: { type: drop.t === 'E' ? 'EXP' : 'GOLD', value: drop.v } });
      return;
    }
    if (msg.type === 'pvpAttack') {
      if (room.mode !== 'pvp' || !room.started) { fail(ws, 'PvP is not active in this room.'); return; }
      const now = Date.now();
      if (now - player.lastAttackAt < 600) return;
      player.lastAttackAt = now;
      const target = room.players.get(String(msg.targetId || ''));
      if (!target || target.id === player.id || target.hp <= 0) return;
      if (Math.hypot(player.x - target.x, player.y - target.y) > 155) {
        fail(ws, 'Opponent is too far away (range 155). Move closer.');
        return;
      }
      target.hp = Math.max(0, target.hp - 14);
      broadcast(room, { type: 'pvpHit', attackerId: player.id, targetId: target.id, damage: 14, targetHp: target.hp, players: playerList(room) });
      if (target.hp <= 0) broadcast(room, { type: 'matchOver', winnerId: player.id, loserId: target.id });
      return;
    }
  });
  ws.on('close', () => closePlayer(ws));
  ws.on('error', () => closePlayer(ws));
});
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 30000);
httpServer.on('close', () => clearInterval(heartbeat));
httpServer.listen(PORT, '0.0.0.0', () => console.log('Sinh Ton 2D matchmaking/chat server listening on port ' + PORT));

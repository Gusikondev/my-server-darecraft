'use strict';

const http = require('node:http');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number.parseInt(process.env.PORT || '8080', 10);
const HOST = process.env.HOST || '0.0.0.0';
const MAX_PLAYERS = 8;
// Snapshot chunks are sent as base64 strings (60 KiB binary per game chunk).
// 256 KiB leaves protocol headroom while keeping individual frames bounded.
const MAX_MESSAGE_BYTES = 256 * 1024;
const MAX_MESSAGES_PER_SECOND = 300;
const MAX_BYTES_PER_SECOND = 12 * 1024 * 1024;
const MAX_BUFFERED_BYTES = 8 * 1024 * 1024;
const MAX_SNAPSHOT_PARTS = 10000;
const MAX_SNAPSHOT_RAW_BYTES = 160 * 1024 * 1024;
const MAX_SNAPSHOT_PART_CHARS = 90000;
const ROOM_TTL_MS = 6 * 60 * 60 * 1000;

/** @type {Map<string, {code:string, seed:number, host:any, players:Map<number, any>, nextPeerId:number, createdAt:number}>} */
const rooms = new Map();

const httpServer = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/healthz') {
    const body = JSON.stringify({
      ok: true,
      service: 'darecraft-room-relay',
      protocol: 2,
      rooms: rooms.size,
      max_players: MAX_PLAYERS,
      max_message_bytes: MAX_MESSAGE_BYTES,
      world_snapshot: true,
    });
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(body);
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('DareCraft room relay v2 is running. WebSocket endpoint: /ws\n');
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_MESSAGE_BYTES,
  perMessageDeflate: false,
});

httpServer.on('upgrade', (request, socket, head) => {
  let pathname;
  try {
    pathname = new URL(request.url || '/', 'http://localhost').pathname;
  } catch {
    socket.destroy();
    return;
  }
  if (pathname !== '/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, (ws) => wss.emit('connection', ws, request));
});

function send(ws, packet) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  if (ws.bufferedAmount > MAX_BUFFERED_BYTES) {
    try { ws.close(1013, 'Client is too slow to receive relay data'); } catch { /* ignore */ }
    return false;
  }
  try {
    ws.send(JSON.stringify(packet));
    return true;
  } catch {
    try { ws.close(1011, 'Relay send failed'); } catch { /* ignore */ }
    return false;
  }
}

function makeCode() {
  for (let i = 0; i < 100; i += 1) {
    const code = `DC1-${crypto.randomInt(10000, 100000)}`;
    if (!rooms.has(code)) return code;
  }
  throw new Error('Unable to allocate a room code');
}

function safeName(value) {
  if (typeof value !== 'string') return 'Игрок';
  const clean = value.replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 20);
  return clean || 'Игрок';
}

function removeClient(ws) {
  const code = ws.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  ws.roomCode = null;
  ws.snapshotTransfers?.clear();
  if (!room) return;

  if (ws.isHost) {
    for (const [peerId, client] of room.players) {
      if (peerId !== 1) {
        send(client, { type: 'host_left' });
        try { client.close(4001, 'Host left the room'); } catch { /* socket already closed */ }
      }
    }
    rooms.delete(code);
    return;
  }

  room.players.delete(ws.peerId);
  if (room.host?.snapshotTransfers) room.host.snapshotTransfers.delete(ws.peerId);
  for (const [peerId, client] of room.players) {
    send(client, { type: 'peer_left', peer_id: ws.peerId, count: room.players.size });
  }
}

function createRoom(ws, packet) {
  if (ws.roomCode) {
    send(ws, { type: 'error', message: 'Это соединение уже состоит в комнате.' });
    return;
  }
  const seed = Number(packet.seed);
  const standardSeed = seed >= 1 && seed <= 1235;
  const citySeed = seed >= 10001 && seed <= 10100;
  if (!Number.isInteger(seed) || (!standardSeed && !citySeed)) {
    send(ws, { type: 'error', message: 'Неверный сид мира.' });
    return;
  }
  const code = makeCode();
  const room = {
    code,
    seed,
    host: ws,
    players: new Map([[1, ws]]),
    nextPeerId: 2,
    createdAt: Date.now(),
  };
  rooms.set(code, room);
  ws.roomCode = code;
  ws.peerId = 1;
  ws.isHost = true;
  ws.playerName = safeName(packet.name);
  ws.snapshotTransfers = new Map();
  send(ws, { type: 'created', code, peer_id: 1, max_players: MAX_PLAYERS, seed });
}

function joinRoom(ws, packet) {
  if (ws.roomCode) {
    send(ws, { type: 'error', message: 'Это соединение уже состоит в комнате.' });
    return;
  }
  const code = typeof packet.code === 'string' ? packet.code.trim().toUpperCase() : '';
  const room = rooms.get(code);
  if (!room || room.host.readyState !== WebSocket.OPEN) {
    send(ws, { type: 'error', message: 'Комната не найдена или хозяин отключился.' });
    return;
  }
  if (room.players.size >= MAX_PLAYERS) {
    send(ws, { type: 'error', message: 'Комната заполнена (максимум 8 игроков).' });
    return;
  }

  let peerId = 2;
  while (room.players.has(peerId) && peerId <= MAX_PLAYERS) peerId += 1;
  if (peerId > MAX_PLAYERS) {
    send(ws, { type: 'error', message: 'Комната заполнена (максимум 8 игроков).' });
    return;
  }
  ws.roomCode = code;
  ws.peerId = peerId;
  ws.isHost = false;
  ws.playerName = safeName(packet.name);
  room.players.set(peerId, ws);

  send(ws, { type: 'joined', code, peer_id: peerId, host_peer_id: 1, seed: room.seed, max_players: MAX_PLAYERS });
  send(ws, {
    type: 'room_state',
    host_peer_id: 1,
    peers: [...room.players.entries()].map(([id, client]) => ({
      peer_id: id,
      name: client.playerName || 'Игрок',
      is_host: id === 1,
    })),
  });

  for (const [existingId, client] of room.players) {
    if (existingId !== peerId) send(client, { type: 'peer_joined', peer_id: peerId, name: ws.playerName, count: room.players.size });
  }
}

function validateSnapshotPacket(ws, payload, targetId) {
  const type = payload.type;
  // A world snapshot is authoritative: only the host may send it, and only to one guest.
  if (!ws.isHost || targetId < 2 || !Number.isInteger(targetId)) return false;
  if (type === 'world_snapshot_start') {
    const parts = Number(payload.parts);
    const rawSize = Number(payload.raw_size);
    if (!Number.isInteger(parts) || parts < 1 || parts > MAX_SNAPSHOT_PARTS) return false;
    if (!Number.isInteger(rawSize) || rawSize < 4 || rawSize > MAX_SNAPSHOT_RAW_BYTES) return false;
    ws.snapshotTransfers.set(targetId, { parts, rawSize, received: new Set() });
    return true;
  }
  const transfer = ws.snapshotTransfers.get(targetId);
  if (!transfer) return false;
  if (type === 'world_snapshot_part') {
    const index = Number(payload.index);
    const data = payload.data;
    if (!Number.isInteger(index) || index < 0 || index >= transfer.parts) return false;
    if (typeof data !== 'string' || data.length < 4 || data.length > MAX_SNAPSHOT_PART_CHARS || data.length % 4 !== 0) return false;
    if (!/^[A-Za-z0-9+/]+={0,2}$/.test(data)) return false;
    transfer.received.add(index);
    return true;
  }
  if (type === 'world_snapshot_end') {
    if (transfer.received.size !== transfer.parts) return false;
    ws.snapshotTransfers.delete(targetId);
    return true;
  }
  return false;
}

function relayData(ws, packet) {
  const room = rooms.get(ws.roomCode);
  if (!room || !room.players.has(ws.peerId)) return;
  if (!packet.payload || typeof packet.payload !== 'object' || Array.isArray(packet.payload)) return;
  if (typeof packet.payload.type !== 'string' || packet.payload.type.length > 40) return;

  const requestedTarget = Number(packet.to || 0);
  const payloadType = packet.payload.type;
  if (payloadType.startsWith('world_snapshot_')) {
    if (!validateSnapshotPacket(ws, packet.payload, requestedTarget)) {
      send(ws, { type: 'error', message: 'Некорректный пакет передачи мира.' });
      return;
    }
  }

  const envelope = { type: 'data', from: ws.peerId, payload: packet.payload };
  if (ws.isHost) {
    if (requestedTarget > 0) {
      const target = room.players.get(requestedTarget);
      if (target && requestedTarget !== ws.peerId) send(target, envelope);
    } else {
      for (const [peerId, client] of room.players) {
        if (peerId !== ws.peerId) send(client, envelope);
      }
    }
  } else {
    // Guest traffic is always routed only to the tablet/PC that hosts the world.
    send(room.host, envelope);
  }
}

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.peerId = 0;
  ws.isHost = false;
  ws.playerName = 'Игрок';
  ws.snapshotTransfers = new Map();
  ws.rateWindowStart = Date.now();
  ws.rateCount = 0;
  ws.byteWindowStart = Date.now();
  ws.byteCount = 0;

  ws.on('message', (buffer) => {
    const now = Date.now();
    if (now - ws.rateWindowStart >= 1000) {
      ws.rateWindowStart = now;
      ws.rateCount = 0;
    }
    if (now - ws.byteWindowStart >= 1000) {
      ws.byteWindowStart = now;
      ws.byteCount = 0;
    }
    ws.rateCount += 1;
    ws.byteCount += buffer.length;
    if (ws.rateCount > MAX_MESSAGES_PER_SECOND || ws.byteCount > MAX_BYTES_PER_SECOND) {
      send(ws, { type: 'error', message: 'Слишком много сетевых данных. Попробуй позже.' });
      ws.close(1008, 'Rate limit exceeded');
      return;
    }

    let packet;
    try {
      packet = JSON.parse(buffer.toString('utf8'));
    } catch {
      send(ws, { type: 'error', message: 'Некорректный формат сообщения.' });
      return;
    }
    if (!packet || typeof packet !== 'object' || Array.isArray(packet)) return;

    switch (packet.type) {
      case 'create': createRoom(ws, packet); break;
      case 'join': joinRoom(ws, packet); break;
      case 'data': relayData(ws, packet); break;
      case 'leave': removeClient(ws); break;
      default: send(ws, { type: 'error', message: 'Неизвестная команда сервера.' }); break;
    }
  });

  ws.on('close', () => removeClient(ws));
  ws.on('error', () => removeClient(ws));
});

const heartbeat = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.createdAt > ROOM_TTL_MS && room.players.size <= 1) {
      rooms.delete(code);
      continue;
    }
    for (const client of room.players.values()) {
      if (client.readyState === WebSocket.OPEN) client.ping();
    }
  }
}, 30_000);

httpServer.listen(PORT, HOST, () => {
  process.stdout.write(`DareCraft relay v2 listening on ${HOST}:${PORT}\n`);
});

function shutdown() {
  clearInterval(heartbeat);
  for (const room of rooms.values()) {
    for (const client of room.players.values()) {
      try { client.close(1001, 'Server shutting down'); } catch { /* ignore */ }
    }
  }
  httpServer.close(() => process.exit(0));
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

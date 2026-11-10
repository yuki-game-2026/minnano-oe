const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

const app = express();
app.use(express.static('public'));
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, maxPayload: 128 * 1024 });
const rooms = new Map();
const MAX_DRAWERS = 20;
const MAX_STROKES = 12000;

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function broadcast(room, data) {
  for (const member of room.members.values()) send(member.ws, data);
}
function state(room) {
  return {
    type: 'state', phase: room.phase, topic: room.topic,
    duration: room.duration, endsAt: room.endsAt,
    hostId: room.hostId, nominatedId: room.nominatedId,
    proposedTopic: room.proposedTopic,
    members: [...room.members.values()].map(m => ({
      id: m.id, name: m.name, role: m.role, raised: m.raised,
      finished: m.finished, host: m.id === room.hostId
    })),
    drawings: Object.fromEntries([...room.drawings].map(([id, strokes]) => [id, strokes]))
  };
}
function publish(room) { broadcast(room, state(room)); }
function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, {
    members: new Map(), drawings: new Map(), hostId: null,
    phase: 'lobby', topic: '', proposedTopic: '', nominatedId: null,
    duration: 300, endsAt: null, timer: null
  });
  return rooms.get(code);
}
function endRound(room) {
  if (room.phase !== 'draw') return;
  room.phase = 'gallery';
  room.endsAt = null;
  if (room.timer) clearTimeout(room.timer);
  room.timer = null;
  for (const m of room.members.values()) if (m.role === 'drawer') m.finished = true;
  publish(room);
}
function resetRound(room) {
  if (room.timer) clearTimeout(room.timer);
  room.timer = null;
  room.phase = 'lobby'; room.topic = ''; room.proposedTopic = '';
  room.nominatedId = null; room.endsAt = null;
  room.drawings.clear();
  for (const m of room.members.values()) { m.raised = false; m.finished = false; }
  publish(room);
}
function cleanText(value, max) {
  return String(value ?? '').trim().slice(0, max);
}
function validPoint(p) {
  return p && typeof p.x === 'number' && typeof p.y === 'number' &&
    Number.isFinite(p.x) && Number.isFinite(p.y) &&
    p.x >= 0 && p.x <= 1 && p.y >= 0 && p.y <= 1;
}

wss.on('connection', ws => {
  let room = null;
  let code = null;
  let member = null;
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;
    if (msg.type === 'join') {
      if (member) return;
      code = cleanText(msg.room, 32);
      const name = cleanText(msg.name, 24);
      const role = msg.role === 'drawer' ? 'drawer' : 'viewer';
      if (!/^[a-zA-Z0-9_-]{1,32}$/.test(code) || !name) {
        return send(ws, { type: 'error', message: '部屋番号と名前を確認してください' });
      }
      room = getRoom(code);
      const drawers = [...room.members.values()].filter(m => m.role === 'drawer').length;
      if (role === 'drawer' && (drawers >= MAX_DRAWERS || room.phase === 'draw')) {
        return send(ws, { type: 'error', message: '描き手は満員、または描画中です' });
      }
      member = { id: crypto.randomUUID(), ws, name, role, raised: false, finished: false };
      room.members.set(member.id, member);
      if (!room.hostId) room.hostId = member.id;
      if (role === 'drawer') room.drawings.set(member.id, []);
      send(ws, { type: 'joined', id: member.id, room: code });
      publish(room);
      return;
    }
    if (!member || !room) return;
    const host = member.id === room.hostId;
    switch (msg.type) {
      case 'raise':
        if (room.phase !== 'lobby') break;
        member.raised = !!msg.raised;
        publish(room);
        break;
      case 'nominate':
        if (!host || room.phase !== 'lobby') break;
        if (!room.members.has(msg.id) || !room.members.get(msg.id).raised) break;
        room.nominatedId = msg.id;
        room.proposedTopic = '';
        publish(room);
        break;
      case 'propose':
        if (room.phase !== 'lobby' || member.id !== room.nominatedId) break;
        room.proposedTopic = cleanText(msg.topic, 80);
        publish(room);
        break;
      case 'approve':
        if (!host || room.phase !== 'lobby' || !room.proposedTopic) break;
        room.topic = room.proposedTopic;
        publish(room);
        break;
      case 'start': {
        if (!host || room.phase !== 'lobby' || !room.topic) break;
        const seconds = Number(msg.seconds);
        if (!Number.isInteger(seconds) || seconds < 10 || seconds > 300) break;
        room.duration = seconds;
        room.phase = 'draw';
        room.endsAt = Date.now() + seconds * 1000;
        room.drawings.clear();
        for (const m of room.members.values()) {
          m.finished = false;
          if (m.role === 'drawer') room.drawings.set(m.id, []);
        }
        room.timer = setTimeout(() => endRound(room), seconds * 1000);
        publish(room);
        break;
      }
      case 'stroke': {
        if (room.phase !== 'draw' || member.role !== 'drawer' || member.finished) break;
        const s = msg.stroke;
        if (!s || !Array.isArray(s.points) || s.points.length < 1 || s.points.length > 250 ||
            !s.points.every(validPoint) || !/^#[0-9a-fA-F]{6}$/.test(s.color) ||
            !Number.isFinite(s.size) || s.size < 1 || s.size > 40) break;
        const strokes = room.drawings.get(member.id);
        if (!strokes || strokes.length >= MAX_STROKES) break;
        const stroke = { points: s.points, color: s.color, size: s.size };
        strokes.push(stroke);
        broadcast(room, { type: 'stroke', id: member.id, stroke });
        break;
      }
      case 'finish':
        if (room.phase !== 'draw' || member.role !== 'drawer') break;
        member.finished = true;
        publish(room);
        if ([...room.members.values()].filter(m => m.role === 'drawer').every(m => m.finished)) endRound(room);
        break;
      case 'chat': {
        const text = cleanText(msg.text, 300);
        if (text) broadcast(room, { type: 'chat', id: member.id, name: member.name, text });
        break;
      }
      case 'end':
        if (host) endRound(room);
        break;
      case 'reset':
        if (host) resetRound(room);
        break;
    }
  });
  ws.on('close', () => {
    if (!member || !room) return;
    room.members.delete(member.id);
    if (room.nominatedId === member.id) {
      room.nominatedId = null;
      room.proposedTopic = '';
    }
    if (room.hostId === member.id) room.hostId = room.members.keys().next().value || null;
    if (room.members.size === 0) {
      if (room.timer) clearTimeout(room.timer);
      rooms.delete(code);
    } else publish(room);
  });
});

const port = process.env.PORT || 3000;
server.listen(port, () => console.log('Drawing game listening on ' + port));

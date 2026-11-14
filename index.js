'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 3000;
const ROOT = __dirname;
const VERSION = '2026-11-14a';
const MAX_BODY = 4 * 1024 * 1024;
const BREAK_MS = 6500;
const HOST_GRACE_MS = 30000;

const rooms = new Map();

/* ================= soru kütüphanesi (kalıcı disk yoksa bellekte) ================= */
const QFILE = path.join(ROOT, 'questions.json');
const QMAX = 40;
const qLib = new Map();
let qSaveT = null;
function qStr(v, n) {
  return String(v == null ? '' : v).replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, ' ').slice(0, n);
}
function sanitizeQSet(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const name = qStr(raw.name, 60).trim();
  if (!name) return null;
  const note = qStr(raw.note, 1600);
  const qs = Array.isArray(raw.questions) ? raw.questions.slice(0, 24) : [];
  const out = [];
  qs.forEach(function (q) {
    if (!q || typeof q !== 'object') return;
    const text = qStr(q.t, 1600).trim();
    if (!text) return;
    let img = '';
    if (typeof q.img === 'string' && q.img.length < 320000 &&
        /^data:image\/(?:jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(q.img)) img = q.img;
    const mod = Math.max(1, Math.min(6, q.mod | 0 || 6));
    const type = q.type === 'num' ? 'num' : 'mc';
    const ch = Array.isArray(q.ch) ? q.ch.slice(0, 4).map(function (x) { return qStr(x, 200); }).filter(Boolean) : [];
    let ok = Math.max(0, Math.min(3, q.ok | 0));
    const ans = qStr(q.ans, 60).trim();
    if (type === 'mc' && ch.length < 2) return;
    if (type === 'mc' && ok >= ch.length) ok = 0;
    if (type === 'num' && !ans) return;
    out.push({ t: text, img: img, mod: mod, type: type, ch: ch, ok: ok, ans: ans });
  });
  if (!out.length) return null;
  return { name: name, note: note, count: Math.min(Math.max(1, raw.count | 0 || 4), out.length), questions: out };
}
function qPersist() {
  clearTimeout(qSaveT);
  qSaveT = setTimeout(function () {
    try {
      fs.writeFile(QFILE, JSON.stringify(Array.from(qLib.values())), function () {});
    } catch (e) {}
  }, 600);
}
function qLoad() {
  try {
    const a = JSON.parse(fs.readFileSync(QFILE, 'utf8'));
    if (!Array.isArray(a)) return;
    a.forEach(function (s) {
      const c = sanitizeQSet(s);
      if (!c) return;
      c.id = qStr(s.id, 24) || rid(8);
      c.owner = qStr(s.owner, 40);
      c.time = Number(s.time) || Date.now();
      qLib.set(c.id, c);
    });
  } catch (e) {}
}
qLoad();

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.webp': 'image/webp',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2'
};

function rid(n) { return crypto.randomBytes(n).toString('hex'); }
function newPin() {
  let pin;
  do { pin = String(Math.floor(100000 + Math.random() * 900000)); } while (rooms.has(pin));
  return pin;
}
function lanUrls() {
  const out = [];
  const nets = os.networkInterfaces();
  Object.keys(nets).forEach(function (name) {
    (nets[name] || []).forEach(function (n) {
      if (n.family === 'IPv4' && !n.internal) out.push('http://' + n.address + ':' + PORT);
    });
  });
  return out;
}
function clamp(v, lo, hi) { v = Math.round(Number(v)); return isNaN(v) ? lo : Math.max(lo, Math.min(hi, v)); }

function writeJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Content-Length': Buffer.byteLength(body)
  });
  res.end(body);
}
function readBody(req) {
  return new Promise(function (resolve, reject) {
    let size = 0;
    const chunks = [];
    req.on('data', function (c) {
      size += c.length;
      if (size > MAX_BODY) { reject(new Error('big')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', function () {
      if (!chunks.length) return resolve({});
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch (e) { reject(new Error('json')); }
    });
    req.on('error', reject);
  });
}

function sseOpen(res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no'
  });
  res.write('retry: 1500\n\n');
}
function sseSend(res, obj) {
  if (!res || res.writableEnded || res.destroyed) return;
  try { res.write('data: ' + JSON.stringify(obj) + '\n\n'); } catch (e) {}
}
function ssePing(res) {
  if (!res || res.writableEnded || res.destroyed) return;
  try { res.write(': ping\n\n'); } catch (e) {}
}

setInterval(function () {
  rooms.forEach(function (room) {
    if (room.host) ssePing(room.host);
    room.players.forEach(function (p) { if (p.sse) ssePing(p.sse); });
  });
  brRooms.forEach(function (room) {
    room.players.forEach(function (p) { if (p.sse) ssePing(p.sse); });
  });
}, 15000).unref();

function roomPlayers(room) {
  return Array.from(room.players.values()).sort(function (a, b) { return a.joined - b.joined; });
}
function alivePlayers(room) { return roomPlayers(room).filter(function (p) { return p.alive; }); }
function aliveN(room) { return alivePlayers(room).length; }
function safeN(room) { return roomPlayers(room).filter(function (p) { return p.res === 'safe'; }).length; }
function boomN(room) { return roomPlayers(room).filter(function (p) { return p.res === 'boom'; }).length; }

function roundDur(room) {
  return Math.max(5, room.cfg.sec - room.cfg.step * (room.round - 1));
}
function remaining(room) {
  if (room.sub === 'round') return Math.max(0, room.endAt - Date.now());
  if (room.sub === 'break') return Math.max(0, room.breakEnd - Date.now());
  return 0;
}

function hostView(room) {
  return {
    type: 'state',
    phase: room.phase,
    sub: room.sub,
    round: room.round,
    max: room.cfg.rounds,
    remaining: remaining(room),
    dur: room.sub === 'round' ? roundDur(room) * 1000 : 0,
    dino: room.dino,
    tp: room.tp,
    cfg: room.cfg,
    qsName: room.qs ? room.qs.name : '',
    n: room.players.size,
    aliveN: aliveN(room),
    safeN: safeN(room),
    boomN: boomN(room),
    correct: room.correct,
    wrong: room.wrong,
    players: roomPlayers(room).map(function (p) {
      return {
        u: p.id, name: p.name, lg: p.lg, av: p.av, ph: p.ph, j: p.joined,
        alive: p.alive, score: p.score | 0, lastPts: p.lastPts | 0, res: p.res, safe: p.safe,
        wrongQ: p.wrongQ, rank: p.rank
      };
    })
  };
}
function playerView(room, p) {
  return {
    type: 'state',
    phase: room.phase,
    sub: room.sub,
    round: room.round,
    max: room.cfg.rounds,
    remaining: remaining(room),
    dur: room.sub === 'round' ? roundDur(room) * 1000 : 0,
    dino: room.dino,
    tp: room.tp,
    qs: room.qs ? { name: room.qs.name, n: room.qs.questions.length } : null,
    n: room.players.size,
    alive: p.alive,
    score: p.score | 0,
    lastPts: p.lastPts | 0,
    res: p.res,
    safe: p.safe,
    aliveN: aliveN(room),
    safeN: safeN(room),
    boomN: boomN(room),
    correct: room.correct,
    wrong: room.wrong,
    rank: p.rank,
    top: room.top,
    name: p.name
  };
}
function push(room) {
  if (!rooms.has(room.pin)) return;
  if (room.host) sseSend(room.host, hostView(room));
  room.players.forEach(function (p) { if (p.sse) sseSend(p.sse, playerView(room, p)); });
}
function broadcastGone(room, why) {
  if (room.host) sseSend(room.host, { type: 'gone', why: why });
  room.players.forEach(function (p) { if (p.sse) sseSend(p.sse, { type: 'gone', why: why }); });
}

function stopTimers(room) {
  clearTimeout(room.tRound);
  clearTimeout(room.tBreak);
  clearTimeout(room.tClose);
  room.tRound = room.tBreak = room.tClose = null;
}
function dropRoom(room, why) {
  stopTimers(room);
  broadcastGone(room, why || 'closed');
  if (room.host) { try { room.host.end(); } catch (e) {} }
  room.players.forEach(function (p) { if (p.sse) { try { p.sse.end(); } catch (e) {} } });
  rooms.delete(room.pin);
}

function playerById(room, pid) { return room.players.get(String(pid || '')); }

function startGame(room, cfg, tp, dino) {
  room.cfg = {
    sec: clamp(cfg.sec, 10, 60),
    step: clamp(cfg.step, 0, 5),
    rounds: clamp(cfg.rounds, 1, 20)
  };
  room.tp = Array.isArray(tp) ? tp.slice(0, 20) : [];
  room.dino = !!dino;
  room.phase = 'play';
  room.round = 0;
  room.sub = 'round';
  room.correct = [];
  room.wrong = [];
  room.top = [];
  room.players.forEach(function (p) {
    p.res = null; p.wrongQ = null; p.safe = 0; p.score = 0; p.lastPts = 0;
    p.alive = true; p.rank = 0;
  });
  nextRound(room);
}

function nextRound(room) {
  if (!rooms.has(room.pin)) return;
  if (room.round >= room.cfg.rounds) { finishGame(room); return; }
  room.round++;
  room.sub = 'round';
  room.correct = [];
  room.wrong = [];
  room.players.forEach(function (p) {
    p.res = null;
    p.wrongQ = null;
    p.rank = 0;
    p.lastPts = 0;
  });
  const dur = roundDur(room);
  room.endAt = Date.now() + dur * 1000;
  room.breakEnd = 0;
  clearTimeout(room.tRound);
  clearTimeout(room.tBreak);
  push(room);
  room.tRound = setTimeout(function () { timeUp(room); }, dur * 1000);
}

function timeUp(room) {
  if (!rooms.has(room.pin) || room.phase !== 'play' || room.sub !== 'round') return;
  room.players.forEach(function (p) {
    if (!p.alive || p.res) return;
    p.res = 'boom';
    p.wrongQ = { mod: 0, q: '__time__' };
    p.lastPts = 0;
  });
  finishRound(room);
}

function finishRound(room) {
  if (!rooms.has(room.pin)) return;
  clearTimeout(room.tRound);
  if (room.phase !== 'play' || room.sub !== 'round') return;
  room.sub = 'break';
  const ok = [];
  const no = [];
  roomPlayers(room).forEach(function (p) {
    if (p.res === 'safe') ok.push({ name: p.name, u: p.id });
    else if (p.res === 'boom') no.push({ name: p.name, u: p.id, mod: p.wrongQ ? p.wrongQ.mod : 0, q: p.wrongQ ? p.wrongQ.q : '' });
  });
  room.correct = ok;
  room.wrong = no;
  room.breakEnd = Date.now() + BREAK_MS;
  push(room);
  room.tBreak = setTimeout(function () {
    if (!rooms.has(room.pin) || room.phase !== 'play' || room.sub !== 'break') return;
    if (room.round >= room.cfg.rounds) finishGame(room);
    else nextRound(room);
  }, BREAK_MS);
}

function finishGame(room) {
  if (!rooms.has(room.pin)) return;
  clearTimeout(room.tRound);
  clearTimeout(room.tBreak);
  room.phase = 'done';
  room.sub = 'done';
  room.breakEnd = 0;
  room.endAt = 0;
  const ranked = roomPlayers(room).sort(function (a, b) {
    if (b.score !== a.score) return b.score - a.score;
    if (b.safe !== a.safe) return b.safe - a.safe;
    return a.joined - b.joined;
  });
  room.top = ranked.slice(0, 5).map(function (p, i) {
    return { name: p.name, score: p.score | 0, safe: p.safe, alive: true, rank: i + 1 };
  });
  ranked.forEach(function (p, i) { p.rank = i + 1; });
  push(room);
}

function checkRoundDone(room) {
  const act = alivePlayers(room);
  if (act.length && act.every(function (p) { return p.res; })) finishRound(room);
}

function api(req, res, url) {
  const parts = url.pathname.split('/').filter(Boolean);
  const name = parts[1] || '';

  if (req.method === 'GET' && name === 'photo') {
    const room = rooms.get(parts[2]);
    const p = room && playerById(room, parts[3]);
    if (!p || !p.ph) { res.writeHead(404); res.end(); return; }
    const m = /^data:(image\/(?:jpeg|png|webp));base64,(.+)$/.exec(p.ph);
    if (!m) { res.writeHead(404); res.end(); return; }
    const buf = Buffer.from(m[2], 'base64');
    res.writeHead(200, { 'Content-Type': m[1], 'Cache-Control': 'no-store', 'Content-Length': buf.length });
    res.end(buf);
    return;
  }

  if (req.method !== 'POST') { res.writeHead(405); res.end(); return; }

  readBody(req).then(function (b) {
    if (name === 'create') {
      const room = {
        pin: newPin(),
        key: rid(12),
        phase: 'lobby', sub: 'lobby', round: 0,
        cfg: { sec: 20, step: 2, rounds: 7 },
        tp: [], dino: false, qs: null,
        players: new Map(),
        host: null,
        endAt: 0, breakEnd: 0,
        correct: [], wrong: [], top: [],
        tRound: null, tBreak: null, tClose: null,
        created: Date.now()
      };
      rooms.set(room.pin, room);
      writeJson(res, 200, { pin: room.pin, key: room.key, lan: lanUrls() });
      return;
    }

    const room = rooms.get(String(b.pin || ''));
    if (!room) return writeJson(res, 404, { err: 'noroom' });

    if (name === 'join') {
      if (room.phase !== 'lobby') return writeJson(res, 400, { err: 'started' });
      const nm = String(b.name || '').trim().slice(0, 14);
      if (!nm) return writeJson(res, 400, { err: 'name' });
      if (room.players.size >= 60) return writeJson(res, 400, { err: 'full' });
      let ph = null;
      if (typeof b.ph === 'string' && /^data:image\/(?:jpeg|png|webp);base64,/.test(b.ph) && b.ph.length < 400000) ph = b.ph;
      const p = {
        id: rid(8),
        name: nm,
        lg: b.lg === 'en' ? 'en' : 'tr',
        av: b.av && typeof b.av === 'object' ? { k: b.av.k | 0, c: b.av.c | 0 } : { k: 0, c: 1 },
        ph: ph,
        joined: Date.now() + room.players.size,
        res: null, wrongQ: null, safe: 0, score: 0, lastPts: 0,
        alive: true, rank: 0, sse: null
      };
      room.players.set(p.id, p);
      push(room);
      return writeJson(res, 200, { pid: p.id, name: p.name });
    }

    if (name === 'host' || name === 'start' || name === 'end' || name === 'reset' ||
        name === 'close' || name === 'kick') {
      if (b.key !== room.key) return writeJson(res, 403, { err: 'key' });
    }

    if (name === 'avatar') {
      const p = playerById(room, b.pid);
      if (!p) return writeJson(res, 404, { err: 'noroom' });
      if (b.av && typeof b.av === 'object') p.av = { k: b.av.k | 0, c: b.av.c | 0 };
      if (b.lg === 'en' || b.lg === 'tr') p.lg = b.lg;
      if (b.ph === null) p.ph = null;
      else if (typeof b.ph === 'string' && /^data:image\/(?:jpeg|png|webp);base64,/.test(b.ph) && b.ph.length < 400000) p.ph = b.ph;
      push(room);
      return writeJson(res, 200, { ok: true });
    }

    if (name === 'leave') {
      const p = playerById(room, b.pid);
      if (p) {
        if (p.sse) { try { p.sse.end(); } catch (e) {} }
        room.players.delete(p.id);
        if (room.phase === 'play' && room.sub === 'round') checkRoundDone(room);
        push(room);
      }
      return writeJson(res, 200, { ok: true });
    }

    if (name === 'start') {
      if (room.phase !== 'lobby') return writeJson(res, 400, { err: 'started' });
      if (!room.players.size) return writeJson(res, 400, { err: 'noplayers' });
      room.qs = sanitizeQSet(b.qs);
      startGame(room, b, b.tp, b.dino);
      return writeJson(res, 200, { ok: true, qs: !!room.qs });
    }

    if (name === 'solve') {
      const p = playerById(room, b.pid);
      if (!p) return writeJson(res, 404, { err: 'noroom' });
      if (room.phase !== 'play' || room.sub !== 'round' || !p.alive || p.res) return writeJson(res, 200, { ok: false });
      const dur = Math.max(1, roundDur(room) * 1000);
      const left = Math.max(0, room.endAt - Date.now());
      const pts = 100 + Math.round(900 * Math.min(1, left / dur));
      p.res = 'safe';
      p.safe++;
      p.score = (p.score | 0) + pts;
      p.lastPts = pts;
      p.wrongQ = null;
      push(room);
      checkRoundDone(room);
      return writeJson(res, 200, { ok: true, safe: p.safe, pts: pts, score: p.score });
    }

    if (name === 'fail') {
      const p = playerById(room, b.pid);
      if (!p) return writeJson(res, 404, { err: 'noroom' });
      if (room.phase !== 'play' || room.sub !== 'round' || !p.alive || p.res) return writeJson(res, 200, { ok: false });
      p.res = 'boom';
      p.wrongQ = { mod: b.mod | 0, q: String(b.q || '').slice(0, 180) };
      p.lastPts = 0;
      push(room);
      checkRoundDone(room);
      return writeJson(res, 200, { ok: true, score: p.score | 0 });
    }

    if (name === 'kick') {
      const p = playerById(room, b.uid);
      if (p) {
        if (p.sse) { sseSend(p.sse, { type: 'gone', why: 'kick' }); try { p.sse.end(); } catch (e) {} }
        room.players.delete(p.id);
        if (room.phase === 'play' && room.sub === 'round') checkRoundDone(room);
        push(room);
      }
      return writeJson(res, 200, { ok: true });
    }

    if (name === 'end') {
      finishGame(room);
      return writeJson(res, 200, { ok: true });
    }

    if (name === 'reset') {
      stopTimers(room);
      room.phase = 'lobby';
      room.sub = 'lobby';
      room.round = 0;
      room.correct = [];
      room.wrong = [];
      room.top = [];
      room.endAt = 0;
      room.breakEnd = 0;
      room.players.forEach(function (p) {
        p.res = null; p.wrongQ = null; p.safe = 0; p.score = 0; p.lastPts = 0;
        p.alive = true; p.rank = 0;
      });
      push(room);
      return writeJson(res, 200, { ok: true });
    }

    if (name === 'close') {
      dropRoom(room, 'closed');
      return writeJson(res, 200, { ok: true });
    }

    writeJson(res, 404, { err: 'api' });
  }).catch(function (e) {
    if (!res.headersSent) writeJson(res, 400, { err: e.message === 'big' ? 'big' : 'net' });
  });
}

/* ================= BACKROOMS: 2 kişilik, kör canavar ================= */
const brRooms = new Map();
const BR_MS = 75000;
const BR_TICK = 50;
const BR_SEND = 66;
const BR_R = 0.28;

function newBrPin() {
  let pin;
  do { pin = String(Math.floor(100000 + Math.random() * 900000)); } while (brRooms.has(pin));
  return pin;
}
function brPlayers(room) {
  return Array.from(room.players.values()).sort(function (a, b) { return a.joined - b.joined; });
}
function brPlayer(name, role) {
  return {
    id: rid(8), name: name, role: role, x: 0, y: 0, sse: null,
    input: { mx: 0, my: 0, run: false },
    stam: 100, noiseT: 0, joined: Date.now(), lastSeen: Date.now(), tGone: null
  };
}
function brMakeMap() {
  const w = 23, h = 17;
  const g = [];
  for (let y = 0; y < h; y++) { const row = []; for (let x = 0; x < w; x++) row.push(1); g.push(row); }
  let s = (Math.random() * 4294967296) >>> 0;
  function rnd() { s = (Math.imul(s, 1664525) + 1013904223) >>> 0; return s / 4294967296; }
  const dirs = [[0, -2], [2, 0], [0, 2], [-2, 0]];
  const stack = [[1, 1]]; g[1][1] = 0;
  while (stack.length) {
    const c = stack[stack.length - 1], x = c[0], y = c[1];
    const opts = [];
    for (let i = 0; i < 4; i++) {
      const nx = x + dirs[i][0], ny = y + dirs[i][1];
      if (nx > 0 && ny > 0 && nx < w - 1 && ny < h - 1 && g[ny][nx] === 1) {
        opts.push([nx, ny, dirs[i][0] / 2, dirs[i][1] / 2]);
      }
    }
    if (!opts.length) { stack.pop(); continue; }
    const o = opts[Math.floor(rnd() * opts.length)];
    g[y + o[3]][x + o[2]] = 0; g[o[1]][o[0]] = 0; stack.push([o[0], o[1]]);
  }
  for (let y = 1; y < h - 1; y++) {
    for (let x = 1; x < w - 1; x++) if (g[y][x] === 1 && rnd() < 0.22) g[y][x] = 0;
  }
  for (let k = 0; k < 4; k++) {
    const rx = 2 + Math.floor(rnd() * (w - 6)), ry = 2 + Math.floor(rnd() * (h - 5));
    for (let y = ry; y < ry + 2; y++) {
      for (let x = rx; x < rx + 3; x++) if (x > 0 && y > 0 && x < w - 1 && y < h - 1) g[y][x] = 0;
    }
  }
  for (let x = 0; x < w; x++) { g[0][x] = 1; g[h - 1][x] = 1; }
  for (let y = 0; y < h; y++) { g[y][0] = 1; g[y][w - 1] = 1; }
  const floors = [];
  for (let y = 1; y < h - 1; y++) for (let x = 1; x < w - 1; x++) if (g[y][x] === 0) floors.push([x, y]);
  function bfs(sx, sy) {
    const d = [];
    for (let y = 0; y < h; y++) { const row = []; for (let x = 0; x < w; x++) row.push(-1); d.push(row); }
    const q = [[sx, sy]]; d[sy][sx] = 0; let qi = 0;
    const steps = [[1, 0], [-1, 0], [0, 1], [0, -1]];
    while (qi < q.length) {
      const cur = q[qi++], x = cur[0], y = cur[1];
      for (let i = 0; i < 4; i++) {
        const nx = x + steps[i][0], ny = y + steps[i][1];
        if (nx >= 0 && ny >= 0 && nx < w && ny < h && g[ny][nx] === 0 && d[ny][nx] < 0) {
          d[ny][nx] = d[y][x] + 1; q.push([nx, ny]);
        }
      }
    }
    return d;
  }
  const start = floors[Math.floor(rnd() * floors.length)];
  const d1 = bfs(start[0], start[1]);
  let m = start, md = -1;
  floors.forEach(function (c) { if (d1[c[1]][c[0]] > md) { md = d1[c[1]][c[0]]; m = c; } });
  const d2 = bfs(m[0], m[1]);
  let hu = m, hd = -1;
  floors.forEach(function (c) { if (d2[c[1]][c[0]] > hd) { hd = d2[c[1]][c[0]]; hu = c; } });
  return {
    w: w, h: h,
    grid: g.map(function (row) { return row.join(''); }),
    spawns: { monster: { x: m[0] + 0.5, y: m[1] + 0.5 }, human: { x: hu[0] + 0.5, y: hu[1] + 0.5 } }
  };
}
function brProps(room) {
  const cells = [];
  for (let y = 1; y < room.map.h - 1; y++) {
    for (let x = 1; x < room.map.w - 1; x++) if (room.map.grid[y].charAt(x) === '0') cells.push([x, y]);
  }
  const out = [];
  const used = {};
  for (let i = 0; i < 10 && cells.length; i++) {
    for (let t = 0; t < 40; t++) {
      const c = cells[Math.floor(Math.random() * cells.length)];
      const key = c[0] + ',' + c[1];
      if (used[key]) continue;
      used[key] = 1;
      out.push({
        x: c[0] + 0.1 + Math.random() * 0.2, y: c[1] + 0.1 + Math.random() * 0.2,
        w: 0.5 + Math.random() * 0.3, h: 0.42 + Math.random() * 0.3, k: i % 4
      });
      break;
    }
  }
  return out;
}
function brPositions(room) {
  brPlayers(room).forEach(function (p) {
    const sp = p.role === 'monster' ? room.map.spawns.monster : room.map.spawns.human;
    p.x = sp.x; p.y = sp.y; p.stam = 100; p.noiseT = 0;
    p.input = { mx: 0, my: 0, run: false };
  });
}
function brFree(room, x, y) {
  const w = room.map.w, h = room.map.h;
  const x0 = Math.floor(x - BR_R), x1 = Math.floor(x + BR_R);
  const y0 = Math.floor(y - BR_R), y1 = Math.floor(y + BR_R);
  for (let cy = y0; cy <= y1; cy++) {
    for (let cx = x0; cx <= x1; cx++) {
      if (cx < 0 || cy < 0 || cx >= w || cy >= h) return false;
      if (room.map.grid[cy].charAt(cx) === '1') {
        const nx = Math.max(cx, Math.min(x, cx + 1));
        const ny = Math.max(cy, Math.min(y, cy + 1));
        const dx = x - nx, dy = y - ny;
        if (dx * dx + dy * dy < BR_R * BR_R) return false;
      }
    }
  }
  return true;
}
function brMove(room, p, dt, speed) {
  const mx = p.input.mx, my = p.input.my;
  if (Math.hypot(mx, my) < 0.05) return false;
  const nx = p.x + mx * speed * dt, ny = p.y + my * speed * dt;
  if (brFree(room, nx, p.y)) p.x = nx;
  if (brFree(room, p.x, ny)) p.y = ny;
  return true;
}
function brOpen(room, cx, cy) {
  return cx >= 0 && cy >= 0 && cx < room.map.w && cy < room.map.h && room.map.grid[cy].charAt(cx) !== '1';
}
function brPath(room, fx, fy, tx, ty) {
  const w = room.map.w, h = room.map.h;
  const sx = Math.floor(fx), sy = Math.floor(fy);
  const ex = Math.floor(tx), ey = Math.floor(ty);
  if (!brOpen(room, sx, sy) || !brOpen(room, ex, ey)) return [];
  const start = sy * w + sx, goal = ey * w + ex;
  if (start === goal) return [[tx, ty]];
  const prev = new Array(w * h).fill(-1);
  prev[start] = start;
  const q = [start];
  let head = 0;
  while (head < q.length) {
    const cur = q[head++];
    if (cur === goal) break;
    const cx = cur % w, cy = (cur - cx) / w;
    const nb = [[cx + 1, cy], [cx - 1, cy], [cx, cy + 1], [cx, cy - 1]];
    for (let i = 0; i < 4; i++) {
      const nx = nb[i][0], ny = nb[i][1];
      if (!brOpen(room, nx, ny)) continue;
      const k = ny * w + nx;
      if (prev[k] !== -1) continue;
      prev[k] = cur;
      q.push(k);
    }
  }
  if (prev[goal] === -1) return [];
  const out = [];
  let k = goal, guard = 0;
  while (k !== start && guard++ < w * h) {
    const cx = k % w, cy = (k - cx) / w;
    out.push([cx + 0.5, cy + 0.5]);
    k = prev[k];
  }
  out.reverse();
  if (out.length) out[out.length - 1] = [tx, ty];
  return out;
}
function brAiSpawnNear(room, x, y, minD, maxD) {
  const w = room.map.w, h = room.map.h;
  const sx = Math.floor(x), sy = Math.floor(y);
  if (!brOpen(room, sx, sy)) return null;
  const start = sy * w + sx;
  const dist = new Array(w * h).fill(-1);
  dist[start] = 0;
  const q = [start];
  let head = 0;
  const cand = [];
  while (head < q.length) {
    const cur = q[head++];
    const cx = cur % w, cy = (cur - cx) / w;
    const d = dist[cur];
    if (d >= minD && d <= maxD) cand.push([cx + 0.5, cy + 0.5]);
    if (d >= maxD) continue;
    const nb = [[cx + 1, cy], [cx - 1, cy], [cx, cy + 1], [cx, cy - 1]];
    for (let i = 0; i < 4; i++) {
      const nx = nb[i][0], ny = nb[i][1];
      if (!brOpen(room, nx, ny)) continue;
      const k = ny * w + nx;
      if (dist[k] !== -1) continue;
      dist[k] = d + 1;
      q.push(k);
    }
  }
  if (!cand.length) return null;
  return cand[Math.floor(Math.random() * cand.length)];
}
function brAiMake(room, role, from) {
  const sp = role === 'monster' ? room.map.spawns.monster : room.map.spawns.human;
  const pos = (from && brAiSpawnNear(room, from.x, from.y, 18, 24)) || [sp.x, sp.y];
  return {
    id: 'ai', name: role === 'monster' ? 'GÖLGE' : 'SES', role: role, ai: true,
    x: pos[0], y: pos[1], stam: 100, path: [], pi: 0, retarget: 0,
    noiseT: 0, moving: false, sniff: 0, seenX: 0, seenY: 0, seenAt: 0
  };
}
function brAiWander(room, x, y) {
  const w = room.map.w, h = room.map.h;
  for (let i = 0; i < 50; i++) {
    const cx = 1 + Math.floor(Math.random() * (w - 2));
    const cy = 1 + Math.floor(Math.random() * (h - 2));
    if (!brOpen(room, cx, cy)) continue;
    if (Math.hypot(cx + 0.5 - x, cy + 0.5 - y) < 4) continue;
    return [cx + 0.5, cy + 0.5];
  }
  return [room.map.spawns.human.x, room.map.spawns.human.y];
}
function brAiFlee(room, mon) {
  const w = room.map.w, h = room.map.h;
  let best = null, bestD = -1;
  for (let i = 0; i < 30; i++) {
    const cx = 1 + Math.floor(Math.random() * (w - 2));
    const cy = 1 + Math.floor(Math.random() * (h - 2));
    if (!brOpen(room, cx, cy)) continue;
    const d = Math.hypot(cx + 0.5 - mon.x, cy + 0.5 - mon.y);
    if (d > bestD) { bestD = d; best = [cx + 0.5, cy + 0.5]; }
  }
  return best;
}
function brAiWalk(room, ai, dt, speed) {
  if (!ai.path.length || ai.pi >= ai.path.length) return false;
  const wp = ai.path[ai.pi];
  const dx = wp[0] - ai.x, dy = wp[1] - ai.y;
  const d = Math.hypot(dx, dy);
  if (d < 0.08) {
    ai.pi++;
    if (ai.pi >= ai.path.length) { ai.path = []; ai.pi = 0; }
    return true;
  }
  const step = Math.min(d, speed * dt);
  const nx = ai.x + (dx / d) * step, ny = ai.y + (dy / d) * step;
  if (brFree(room, nx, ai.y)) ai.x = nx;
  if (brFree(room, ai.x, ny)) ai.y = ny;
  return true;
}
function brAiSet(room, ai, goal) {
  ai.path = goal ? brPath(room, ai.x, ai.y, goal[0], goal[1]) : [];
  ai.pi = 0;
  if (!ai.path.length) ai.retarget = 0;
}
function brAiStep(room, ai, foe, dt, now) {
  if (ai.role === 'monster') {
    const dist = foe ? Math.hypot(foe.x - ai.x, foe.y - ai.y) : 99;
    ai.sniff = (ai.sniff || 0) + dt;
    const grace = now - (room.startAt || 0) < 5500;
    if (foe && !grace) {
      const moving = Math.hypot(foe.input.mx, foe.input.my) > 0.12;
      const reach = foe.input.run ? 22 : 16;
      if ((moving && dist <= reach) || dist <= 7.5) {
        ai.seenX = foe.x; ai.seenY = foe.y; ai.seenAt = now;
        ai.sniff = 0;
      } else if (ai.sniff >= 2.5) {
        ai.sniff = 0;
        ai.seenX = foe.x; ai.seenY = foe.y; ai.seenAt = now;
      }
    }
    const fresh = now - ai.seenAt < 2600;
    if (now >= ai.retarget || (!ai.path.length && !fresh)) {
      ai.retarget = now + 700;
      brAiSet(room, ai, fresh ? [ai.seenX, ai.seenY] : brAiWander(room, ai.x, ai.y));
    }
    ai.moving = brAiWalk(room, ai, dt, fresh ? 2.35 : 1.7);
    ai.stam = 100;
    return;
  }
  const dist = foe ? Math.hypot(foe.x - ai.x, foe.y - ai.y) : 99;
  const fleeing = !!foe && dist < 6.5;
  if (now >= ai.retarget || !ai.path.length) {
    ai.retarget = now + (fleeing ? 800 : 2600 + Math.random() * 1800);
    brAiSet(room, ai, fleeing ? brAiFlee(room, foe) : brAiWander(room, ai.x, ai.y));
  }
  ai.moving = brAiWalk(room, ai, dt, fleeing ? 2.3 : 1.55);
  ai.stam = 100;
  ai.noiseT += dt;
  if (foe && ai.moving && ai.noiseT >= 0.75) {
    ai.noiseT = 0;
    if (dist <= 22) {
      room.pings.push({ x: ai.x, y: ai.y, run: fleeing, t: now });
      if (room.pings.length > 10) room.pings.shift();
    }
  }
}
function brViewPlayers(room, viewer) {
  const list = brPlayers(room).map(function (q) {
    const hide = viewer.role === 'monster' && room.phase === 'play' && q.role !== 'monster';
    return {
      id: q.id, name: q.name, role: q.role,
      x: hide ? null : q.x, y: hide ? null : q.y,
      stam: q.role === 'human' ? Math.round(q.stam) : null
    };
  });
  if (room.ai) {
    const q = room.ai;
    const hide = viewer.role === 'monster' && room.phase === 'play' && q.role !== 'monster';
    list.push({
      id: 'ai', name: q.name, role: q.role, ai: true,
      x: hide ? null : q.x, y: hide ? null : q.y,
      stam: q.role === 'human' ? 100 : null
    });
  }
  return list;
}
function brState(room, p) {
  const now = Date.now();
  return {
    type: 'tick',
    phase: room.phase,
    t: Math.max(0, room.endAt - now),
    role: p.role,
    owner: room.owner === p.id,
    me: { x: p.x, y: p.y, stam: p.role === 'human' ? Math.round(p.stam) : null },
    players: brViewPlayers(room, p),
    n: room.players.size,
    winner: room.winner,
    reason: room.reason,
    pings: p.role === 'monster' ? room.pings.filter(function (q) { return now - q.t < 1700; }) : []
  };
}
function brPush(room) {
  room.lastActive = Date.now();
  room.players.forEach(function (p) { if (p.sse) sseSend(p.sse, brState(room, p)); });
}
function brInit(room, p) {
  return {
    type: 'init', pin: room.pin, pid: p.id, role: p.role,
    owner: room.owner === p.id, phase: room.phase,
    map: { w: room.map.w, h: room.map.h, grid: room.map.grid },
    props: room.props,
    players: brPlayers(room).map(function (q) { return { id: q.id, name: q.name, role: q.role }; })
  };
}
function brFinish(room, winner, reason) {
  if (room.phase !== 'play') return;
  room.phase = 'done';
  room.winner = winner;
  room.reason = reason;
  room.endAt = 0;
  room.pings = [];
  room.players.forEach(function (p) { p.input = { mx: 0, my: 0, run: false }; });
  brPush(room);
}
function brDrop(room) {
  room.players.forEach(function (p) {
    clearTimeout(p.tGone);
    if (p.sse) { try { p.sse.end(); } catch (e) {} }
  });
  brRooms.delete(room.pin);
}
function brRemove(room, p) {
  clearTimeout(p.tGone);
  if (p.sse) { try { p.sse.end(); } catch (e) {} }
  room.players.delete(p.id);
  if (room.owner === p.id) {
    const rest = brPlayers(room);
    room.owner = rest.length ? rest[0].id : null;
  }
  if (!room.players.size) { brDrop(room); return; }
  if (room.phase === 'play') {
    const rest = brPlayers(room);
    brFinish(room, rest[0].role === 'monster' ? 'monster' : 'human', 'left');
  } else brPush(room);
}
function brStart(room) {
  room.phase = 'play';
  room.winner = null;
  room.reason = '';
  room.pings = [];
  room.endAt = Date.now() + BR_MS;
  room.startAt = Date.now();
  room.last = Date.now();
  room.lastSend = 0;
  brPositions(room);
  const solo = brPlayers(room);
  room.ai = solo.length === 1
    ? brAiMake(room, solo[0].role === 'monster' ? 'human' : 'monster', solo[0])
    : null;
  brPush(room);
}

setInterval(function () {
  const now = Date.now();
  brRooms.forEach(function (room) {
    if (room.phase !== 'play') return;
    const dt = Math.min(0.1, Math.max(0.01, (now - (room.last || now)) / 1000));
    room.last = now;
    const ps = brPlayers(room);
    const monP = ps.filter(function (p) { return p.role === 'monster'; })[0];
    const humP = ps.filter(function (p) { return p.role === 'human'; })[0];
    const ai = room.ai;
    const mon = monP || (ai && ai.role === 'monster' ? ai : null);
    const hum = humP || (ai && ai.role === 'human' ? ai : null);
    if (now >= room.endAt) { brFinish(room, hum ? 'human' : 'monster', 'time'); return; }
    ps.forEach(function (p) {
      if (now - p.lastSeen > 1500) p.input = { mx: 0, my: 0, run: false };
      if (p.role === 'monster') {
        brMove(room, p, dt, 2.55);
        p.stam = 100;
        return;
      }
      const moving = Math.hypot(p.input.mx, p.input.my) > 0.1;
      const canRun = p.input.run && moving && p.stam > 1;
      brMove(room, p, dt, canRun ? 3.5 : 2.15);
      p.stam = Math.max(0, Math.min(100, p.stam + (canRun ? -26 * dt : 14 * dt)));
      p.noiseT = (p.noiseT || 0) + dt;
      p.breathT = (p.breathT || 0) + dt;
      if (!mon) { p.noiseT = 0; p.breathT = 0; return; }
      const runNoise = canRun;
      const every = runNoise ? 0.3 : 0.5;
      const dist = Math.hypot(mon.x - p.x, mon.y - p.y);
      if (moving && p.noiseT >= every) {
        p.noiseT = 0;
        if (dist <= (runNoise ? 20 : 12)) {
          room.pings.push({ x: p.x, y: p.y, run: runNoise, t: now });
          if (room.pings.length > 10) room.pings.shift();
        }
      }
      if (!moving && dist <= 3.6 && p.breathT >= 1.8) {
        p.breathT = 0;
        room.pings.push({ x: p.x, y: p.y, run: false, t: now, br: 1 });
        if (room.pings.length > 10) room.pings.shift();
      }
    });
    if (ai) brAiStep(room, ai, ai.role === 'monster' ? humP : monP, dt, now);
    if (hum && mon && Math.hypot(mon.x - hum.x, mon.y - hum.y) < 0.58) {
      brFinish(room, 'monster', 'caught');
      return;
    }
    if (now - (room.lastSend || 0) >= BR_SEND) { room.lastSend = now; brPush(room); }
  });
}, BR_TICK).unref();

setInterval(function () {
  const now = Date.now();
  brRooms.forEach(function (room) {
    brPlayers(room).forEach(function (p) {
      if (!p.sse && now - p.lastSeen > 25000 && room.phase !== 'play') brRemove(room, p);
    });
    if ((!room.players.size && now - room.lastActive > 120000) || now - room.created > 6 * 3600000) {
      brDrop(room);
    }
  });
}, 30000).unref();

function brApi(req, res, url) {
  const name = url.pathname.split('/').filter(Boolean)[2] || '';
  if (name === 'ping') return writeJson(res, 200, { ok: true, rooms: brRooms.size, lan: lanUrls(), v: VERSION });
  if (req.method !== 'POST') { res.writeHead(405); res.end(); return; }
  readBody(req).then(function (b) {
    if (name === 'create') {
      if (brRooms.size > 300) return writeJson(res, 400, { err: 'busy' });
      const nm = String(b.name || '').trim().slice(0, 14);
      if (!nm) return writeJson(res, 400, { err: 'name' });
      const room = {
        pin: newBrPin(), map: null, props: [], phase: 'lobby',
        players: new Map(), owner: null, endAt: 0, ai: null,
        winner: null, reason: '', pings: [],
        last: 0, lastSend: 0, lastActive: Date.now(), created: Date.now()
      };
      room.map = brMakeMap();
      room.props = brProps(room);
      const role = b.role === 'human' ? 'human' : 'monster';
      const p = brPlayer(nm, role);
      room.players.set(p.id, p);
      room.owner = p.id;
      brRooms.set(room.pin, room);
      return writeJson(res, 200, { pin: room.pin, pid: p.id, role: role, lan: lanUrls() });
    }

    const room = brRooms.get(String(b.pin || ''));
    if (!room) return writeJson(res, 404, { err: 'noroom' });
    room.lastActive = Date.now();

    if (name === 'join') {
      if (room.phase !== 'lobby') return writeJson(res, 400, { err: 'started' });
      if (room.players.size >= 2) return writeJson(res, 400, { err: 'full' });
      const nm = String(b.name || '').trim().slice(0, 14);
      if (!nm) return writeJson(res, 400, { err: 'name' });
      const hasM = brPlayers(room).some(function (p) { return p.role === 'monster'; });
      const hasH = brPlayers(room).some(function (p) { return p.role === 'human'; });
      const role = hasM ? 'human' : (hasH ? 'monster' : 'human');
      const p = brPlayer(nm, role);
      if (room.phase === 'lobby') {
        p.x = role === 'monster' ? room.map.spawns.monster.x : room.map.spawns.human.x;
        p.y = role === 'monster' ? room.map.spawns.monster.y : room.map.spawns.human.y;
      }
      room.players.set(p.id, p);
      brPush(room);
      return writeJson(res, 200, { pid: p.id, role: role, name: nm });
    }

    const p = room.players.get(String(b.pid || ''));
    if (!p) return writeJson(res, 404, { err: 'noroom' });
    room.lastActive = Date.now();
    p.lastSeen = Date.now();

    if (name === 'input') {
      let mx = Number(b.mx), my = Number(b.my);
      if (!isFinite(mx)) mx = 0;
      if (!isFinite(my)) my = 0;
      mx = Math.max(-1, Math.min(1, mx));
      my = Math.max(-1, Math.min(1, my));
      const mag = Math.hypot(mx, my);
      if (mag > 1) { mx /= mag; my /= mag; }
      p.input = { mx: mx, my: my, run: !!b.run };
      return writeJson(res, 200, { ok: true });
    }

    if (name === 'start' || name === 'again' || name === 'close' || name === 'kick') {
      if (room.owner !== p.id) return writeJson(res, 403, { err: 'owner' });
    }
    if (name === 'start') {
      if (room.phase === 'play') return writeJson(res, 400, { err: 'started' });
      if (!room.players.size) return writeJson(res, 400, { err: 'noplayers' });
      brStart(room);
      return writeJson(res, 200, { ok: true });
    }
    if (name === 'again') {
      room.phase = 'lobby';
      room.winner = null;
      room.reason = '';
      room.endAt = 0;
      room.pings = [];
      room.ai = null;
      brPush(room);
      return writeJson(res, 200, { ok: true });
    }
    if (name === 'switch') {
      const other = brPlayers(room).filter(function (q) { return q.id !== p.id; })[0];
      if (other) {
        const tr = p.role;
        p.role = other.role;
        other.role = tr;
        const tx = p.x, ty = p.y;
        p.x = other.x; p.y = other.y; other.x = tx; other.y = ty;
      } else {
        p.role = p.role === 'monster' ? 'human' : 'monster';
        if (room.ai) room.ai = brAiMake(room, p.role === 'monster' ? 'human' : 'monster', p);
      }
      room.pings = [];
      room.winner = null;
      room.reason = '';
      p.stam = 100;
      brPush(room);
      return writeJson(res, 200, { role: p.role });
    }
    if (name === 'leave') {
      brRemove(room, p);
      return writeJson(res, 200, { ok: true });
    }
    if (name === 'close') {
      brDrop(room);
      return writeJson(res, 200, { ok: true });
    }
    if (name === 'kick') {
      const target = room.players.get(String(b.uid || ''));
      if (target) brRemove(room, target);
      return writeJson(res, 200, { ok: true });
    }
    writeJson(res, 404, { err: 'api' });
  }).catch(function (e) {
    if (!res.headersSent) writeJson(res, 400, { err: e.message === 'big' ? 'big' : 'net' });
  });
}

function serveFile(req, res, pathname) {
  let file;
  if (pathname === '/' || pathname === '/bomtik.html') file = 'bomtik.html';
  else if (pathname === '/eski.html') file = 'eski.html';
  else if (pathname === '/backrooms' || pathname === '/backrooms.html') file = 'backrooms.html';
  else if (pathname === '/three.min.js') file = 'three.min.js';
  else if (/^\/vendor\/[\w.\-/]+$/.test(pathname)) file = pathname.slice(1);
  else { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found'); return; }
  const full = path.join(ROOT, file);
  if (!full.startsWith(ROOT)) { res.writeHead(403); res.end(); return; }
  fs.readFile(full, function (err, buf) {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found'); return; }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(full).toLowerCase()] || 'application/octet-stream',
      'Cache-Control': 'no-cache'
    });
    res.end(buf);
  });
}

const server = http.createServer(function (req, res) {
  const url = new URL(req.url, 'http://localhost');
  const pathname = decodeURIComponent(url.pathname);

  if (pathname === '/api/host' || pathname.indexOf('/api/host/') === 0) {
    const seg = pathname.split('/').filter(Boolean);
    const room = rooms.get(seg[2]);
    if (!room || url.searchParams.get('key') !== room.key) { res.writeHead(404); res.end(); return; }
    sseOpen(res);
    clearTimeout(room.tClose);
    room.tClose = null;
    if (room.host && room.host !== res) { try { room.host.end(); } catch (e) {} }
    room.host = res;
    sseSend(res, hostView(room));
    req.on('close', function () {
      if (room.host === res) room.host = null;
      if (!rooms.has(room.pin)) return;
      clearTimeout(room.tClose);
      room.tClose = setTimeout(function () {
        if (!room.host && rooms.get(room.pin) === room) dropRoom(room, 'closed');
      }, HOST_GRACE_MS);
    });
    return;
  }

  if (pathname.indexOf('/api/br/live/') === 0) {
    const seg = pathname.split('/').filter(Boolean);
    const room = brRooms.get(seg[3]);
    const p = room && room.players.get(seg[4]);
    if (!p) { res.writeHead(404); res.end(); return; }
    sseOpen(res);
    clearTimeout(p.tGone);
    if (p.sse && p.sse !== res) { try { p.sse.end(); } catch (e) {} }
    p.sse = res;
    p.lastSeen = Date.now();
    sseSend(res, brInit(room, p));
    sseSend(res, brState(room, p));
    req.on('close', function () {
      if (p.sse === res) p.sse = null;
      p.lastSeen = Date.now();
      if (room.phase === 'play' && room.players.get(p.id) === p) {
        clearTimeout(p.tGone);
        p.tGone = setTimeout(function () {
          if (!p.sse && room.players.get(p.id) === p) brRemove(room, p);
        }, 8000);
      }
    });
    return;
  }

  if (pathname.indexOf('/api/br/') === 0) { brApi(req, res, url); return; }

  if (pathname.indexOf('/api/play/') === 0) {
    const seg = pathname.split('/').filter(Boolean);
    const room = rooms.get(seg[2]);
    const p = room && playerById(room, seg[3]);
    if (!room || !p) { res.writeHead(404); res.end(); return; }
    sseOpen(res);
    if (p.sse && p.sse !== res) { try { p.sse.end(); } catch (e) {} }
    p.sse = res;
    sseSend(res, playerView(room, p));
    req.on('close', function () {
      if (p.sse === res) p.sse = null;
    });
    return;
  }

  if (pathname === '/api/ver') { writeJson(res, 200, { v: VERSION }); return; }

  if (pathname === '/api/q/list' && req.method === 'GET') {
    const sets = Array.from(qLib.values()).map(function (s) {
      return {
        id: s.id, name: s.name, n: s.questions.length, count: s.count,
        time: s.time, note: (s.note || '').slice(0, 120)
      };
    }).sort(function (a, b) { return b.time - a.time; });
    writeJson(res, 200, { v: VERSION, sets: sets });
    return;
  }

  if (pathname.indexOf('/api/q/room/') === 0 && req.method === 'GET') {
    const seg = pathname.split('/').filter(Boolean);
    const room = rooms.get(seg[3]);
    const p = room && playerById(room, seg[4]);
    if (!p || !room.qs) return writeJson(res, 404, { err: 'noq' });
    writeJson(res, 200, { set: room.qs });
    return;
  }

  if (pathname.indexOf('/api/q/get/') === 0 && req.method === 'GET') {
    const id = decodeURIComponent(pathname.split('/')[4] || '');
    const s = qLib.get(id);
    if (!s) return writeJson(res, 404, { err: 'noq' });
    writeJson(res, 200, { set: s });
    return;
  }

  if (pathname === '/api/q/save' && req.method === 'POST') {
    readBody(req).then(function (b) {
      const c = sanitizeQSet(b && b.set);
      if (!c) return writeJson(res, 400, { err: 'badset' });
      let id = qStr(b && b.id, 24), owner = qStr(b && b.owner, 40);
      const old = id && qLib.get(id);
      if (old && old.owner && old.owner !== owner) return writeJson(res, 403, { err: 'key' });
      if (!old) { id = rid(8); owner = rid(10); }
      c.id = id; c.owner = owner; c.time = Date.now();
      qLib.set(id, c);
      while (qLib.size > QMAX) {
        const first = qLib.keys().next().value;
        qLib.delete(first);
      }
      qPersist();
      writeJson(res, 200, { ok: true, id: id, owner: owner });
    }).catch(function () { writeJson(res, 400, { err: 'net' }); });
    return;
  }

  if (pathname === '/api/q/del' && req.method === 'POST') {
    readBody(req).then(function (b) {
      const s = qLib.get(qStr(b && b.id, 24));
      if (s && s.owner && s.owner !== qStr(b && b.owner, 40)) return writeJson(res, 403, { err: 'key' });
      if (s) { qLib.delete(s.id); qPersist(); }
      writeJson(res, 200, { ok: true });
    }).catch(function () { writeJson(res, 400, { err: 'net' }); });
    return;
  }

  if (pathname.indexOf('/api/') === 0) { api(req, res, url); return; }

  serveFile(req, res, pathname);
});

server.listen(PORT, '0.0.0.0', function () {
  const lan = lanUrls();
  console.log('');
  console.log('  BOMTIK sunucusu hazır  ·  v' + VERSION);
  console.log('  Bilgisayar : http://localhost:' + PORT);
  lan.forEach(function (u) { console.log('  Telefon    : ' + u); });
  console.log('  Aynı Wi-Fi\'deki cihazlar telefon adresini kullanır.');
  console.log('  Kapatmak için Ctrl+C.');
  console.log('');
});

process.on('SIGINT', function () {
  rooms.forEach(function (room) { dropRoom(room, 'closed'); });
  brRooms.forEach(function (room) { brDrop(room); });
  server.close(function () { process.exit(0); });
});

'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 3000;
const ROOT = __dirname;
const MAX_BODY = 4 * 1024 * 1024;
const BREAK_MS = 6500;
const HOST_GRACE_MS = 30000;

const rooms = new Map();

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
    livesMax: room.cfg.lives,
    dino: room.dino,
    tp: room.tp,
    cfg: room.cfg,
    n: room.players.size,
    aliveN: aliveN(room),
    safeN: safeN(room),
    boomN: boomN(room),
    correct: room.correct,
    wrong: room.wrong,
    players: roomPlayers(room).map(function (p) {
      return {
        u: p.id, name: p.name, lg: p.lg, av: p.av, ph: p.ph, j: p.joined,
        alive: p.alive, lives: p.lives, res: p.res, safe: p.safe,
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
    livesMax: room.cfg.lives,
    dino: room.dino,
    tp: room.tp,
    n: room.players.size,
    alive: p.alive,
    lives: p.lives,
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
    rounds: clamp(cfg.rounds, 1, 20),
    lives: clamp(cfg.lives, 1, 5)
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
    p.res = null; p.wrongQ = null; p.lives = room.cfg.lives; p.safe = 0;
    p.alive = true; p.rank = 0;
  });
  nextRound(room);
}

function nextRound(room) {
  if (!rooms.has(room.pin)) return;
  if (room.round >= room.cfg.rounds || (room.players.size > 1 && aliveN(room) <= 1)) { finishGame(room); return; }
  room.round++;
  room.sub = 'round';
  room.correct = [];
  room.wrong = [];
  room.players.forEach(function (p) {
    p.res = null;
    p.wrongQ = null;
    p.rank = 0;
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
    p.lives = Math.max(0, p.lives - 1);
    if (p.lives === 0) p.alive = false;
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
    if (room.round >= room.cfg.rounds || (room.players.size > 1 && aliveN(room) <= 1)) finishGame(room);
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
    if (a.alive !== b.alive) return a.alive ? -1 : 1;
    if (b.safe !== a.safe) return b.safe - a.safe;
    if (b.lives !== a.lives) return b.lives - a.lives;
    return a.joined - b.joined;
  });
  room.top = ranked.slice(0, 5).map(function (p, i) {
    return { name: p.name, safe: p.safe, alive: p.alive, rank: i + 1 };
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
        cfg: { sec: 20, step: 2, rounds: 7, lives: 2 },
        tp: [], dino: false,
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
        res: null, wrongQ: null, lives: room.cfg.lives, safe: 0,
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
      startGame(room, b, b.tp, b.dino);
      return writeJson(res, 200, { ok: true });
    }

    if (name === 'solve') {
      const p = playerById(room, b.pid);
      if (!p) return writeJson(res, 404, { err: 'noroom' });
      if (room.phase !== 'play' || room.sub !== 'round' || !p.alive || p.res) return writeJson(res, 200, { ok: false });
      p.res = 'safe';
      p.safe++;
      p.wrongQ = null;
      push(room);
      checkRoundDone(room);
      return writeJson(res, 200, { ok: true, safe: p.safe });
    }

    if (name === 'fail') {
      const p = playerById(room, b.pid);
      if (!p) return writeJson(res, 404, { err: 'noroom' });
      if (room.phase !== 'play' || room.sub !== 'round' || !p.alive || p.res) return writeJson(res, 200, { ok: false });
      p.res = 'boom';
      p.wrongQ = { mod: b.mod | 0, q: String(b.q || '').slice(0, 180) };
      p.lives = Math.max(0, p.lives - 1);
      if (p.lives === 0) p.alive = false;
      push(room);
      checkRoundDone(room);
      return writeJson(res, 200, { ok: true, lives: p.lives });
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
        p.res = null; p.wrongQ = null; p.lives = room.cfg.lives; p.safe = 0;
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

function serveFile(req, res, pathname) {
  let file;
  if (pathname === '/' || pathname === '/bomtik.html') file = 'bomtik.html';
  else if (pathname === '/eski.html') file = 'eski.html';
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

  if (pathname.indexOf('/api/') === 0) { api(req, res, url); return; }

  serveFile(req, res, pathname);
});

server.listen(PORT, '0.0.0.0', function () {
  const lan = lanUrls();
  console.log('');
  console.log('  BOMTIK sunucusu hazır');
  console.log('  Bilgisayar : http://localhost:' + PORT);
  lan.forEach(function (u) { console.log('  Telefon    : ' + u); });
  console.log('  Aynı Wi-Fi\'deki cihazlar telefon adresini kullanır.');
  console.log('  Kapatmak için Ctrl+C.');
  console.log('');
});

process.on('SIGINT', function () {
  rooms.forEach(function (room) { dropRoom(room, 'closed'); });
  server.close(function () { process.exit(0); });
});

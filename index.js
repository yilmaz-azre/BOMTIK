'use strict';
/*
 * Bomtik sunucusu — tek dosya, paket kurulumu gerektirmez.
 * Çalıştır:  node index.js     (varsayılan port 3000, PORT ile değiştirilebilir)
 * index.html bu dosyanın yanında durmalıdır; sunucu onu sunar.
 *
 * Sınıf oyunu (tur sistemi):
 *  - Tahta (öğretmen) /api/create ile 6 haneli kodlu oda açar.
 *  - Öğrenciler /api/join ile kodu ve takma adı gönderir.
 *  - Oyun başlayınca her tur 10 saniyedir. Herkes kendi bombasını çözer.
 *    Çözen "bu tur korundu"; çözemeyen (yanlış cevap ya da süre dolması) bir can kaybeder.
 *  - Canı biten elenir. Tek kişi kalınca ya da tur sayısı dolunca oyun biter.
 *  - Canlı güncellemeler Server-Sent Events (SSE) ile gider.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 3000;
const HTML = path.join(__dirname, 'index.html');
const TOPICS = ['int', 'rat', 'pat', 'bin', 'ratio', 'pct', 'alg', 'ang', 'circ', 'poly', 'data'];
const MAX_PLAYERS = 100, MAX_ROOMS = 500, ROOM_TTL = 4 * 3600 * 1000;
const ROUND_MS = 10000;   // her tur 10 saniye
const BREAK_MS = 4000;    // turlar arası sonuç bekleme
const GRACE_MS = 800;     // ağ gecikmesi payı

const rooms = new Map();

/* ---------- yardımcılar ---------- */
const send = (res, code, obj) => {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
};
const readBody = (req) => new Promise((resolve, reject) => {
  let s = '';
  req.on('data', (c) => { s += c; if (s.length > 4096) { reject(new Error('big')); req.destroy(); } });
  req.on('end', () => { try { resolve(s ? JSON.parse(s) : {}); } catch (e) { reject(e); } });
  req.on('error', reject);
});
const sse = (res, obj) => { try { res.write('data: ' + JSON.stringify(obj) + '\n\n'); } catch (e) { /* kapalı bağlantı */ } };
const openSSE = (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-store, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no'
  });
  res.write('retry: 2000\n\n');
};
const newPin = () => {
  for (let i = 0; i < 200; i++) {
    const p = String(crypto.randomInt(100000, 1000000));
    if (!rooms.has(p)) return p;
  }
  return null;
};
const cleanName = (s) => String(s || '').replace(/[\u0000-\u001f<>&"'`]/g, '').replace(/\s+/g, ' ').trim().slice(0, 14);
const lanUrls = () => {
  const out = [];
  Object.values(os.networkInterfaces()).forEach((l) => (l || []).forEach((i) => {
    if (i.family === 'IPv4' && !i.internal) out.push('http://' + i.address + ':' + PORT);
  }));
  return out;
};
const clamp = (v, a, b, d) => { v = Math.floor(Number(v)); return Number.isFinite(v) ? Math.min(b, Math.max(a, v)) : d; };

/* ---------- oda durumu ---------- */
// Sıralama: hayatta olanlar önde; sonra can, korunan tur sayısı, toplam çözme süresi (az olan iyi).
// Elenenler: geç elenen önde.
const rankSort = (a, b) => {
  if (a.alive !== b.alive) return a.alive ? -1 : 1;
  if (!a.alive && a.out !== b.out) return b.out - a.out;
  return b.lives - a.lives || b.safe - a.safe || a.ms - b.ms || a.joined - b.joined;
};
const ranking = (room) => [...room.players.values()].sort(rankSort);

function flush(room) {
  clearTimeout(room.flushT); room.flushT = null;
  const r = ranking(room), now = Date.now();
  let rem = 0;
  if (room.phase === 'play') rem = Math.max(0, (room.sub === 'round' ? room.roundEnd : room.breakEnd) - now);
  const aliveN = r.filter((p) => p.alive).length;
  const safeN = r.filter((p) => p.res === 'safe').length;
  const boomN = r.filter((p) => p.res === 'boom').length;
  const base = {
    type: 'state', phase: room.phase, sub: room.sub, round: room.round, max: room.max,
    remaining: rem, n: r.length, aliveN, safeN, boomN, livesMax: room.livesInit
  };
  const hostMsg = Object.assign({ pin: room.pin }, base, {
    players: r.map((p) => ({ name: p.name, lives: p.lives, alive: p.alive, res: p.res, safe: p.safe, ms: p.ms, j: p.joined }))
  });
  room.hosts.forEach((res) => sse(res, hostMsg));
  const top = r.slice(0, 3).map((p) => ({ name: p.name, safe: p.safe, lives: p.lives, alive: p.alive }));
  r.forEach((p, i) => {
    if (p.sse) sse(p.sse, Object.assign({}, base, {
      tp: room.tp, rank: i + 1, lives: p.lives, alive: p.alive, res: p.res, safe: p.safe, top
    }));
  });
}
const schedule = (room) => { if (!room.flushT) room.flushT = setTimeout(() => flush(room), 250); };

function resetPlayers(room) {
  room.players.forEach((q) => { q.lives = room.livesInit; q.alive = true; q.safe = 0; q.ms = 0; q.res = null; q.out = 0; });
}
function lose(room, q) {
  q.res = 'boom';
  q.lives = Math.max(0, q.lives - 1);
  if (q.lives <= 0) { q.alive = false; q.out = room.round; }
}
function finish(room) {
  clearTimeout(room.timer);
  if (room.phase === 'lobby') return;
  room.phase = 'done';
  room.sub = null;
  flush(room);
}
function nextRound(room) {
  clearTimeout(room.timer);
  room.round++;
  room.sub = 'round';
  room.roundStart = Date.now();
  room.roundEnd = room.roundStart + ROUND_MS;
  room.players.forEach((q) => { q.res = null; });
  room.timer = setTimeout(() => endRound(room), ROUND_MS + GRACE_MS);
  flush(room);
}
function endRound(room) {
  if (room.phase !== 'play' || room.sub !== 'round') return;
  clearTimeout(room.timer);
  room.players.forEach((q) => { if (q.alive && !q.res) lose(room, q); });
  const alive = [...room.players.values()].filter((q) => q.alive).length;
  const over = (room.n0 > 1 ? alive <= 1 : alive === 0) || room.round >= room.max;
  room.sub = 'break';
  room.breakEnd = Date.now() + BREAK_MS;
  room.timer = setTimeout(() => { if (over) finish(room); else nextRound(room); }, BREAK_MS);
  flush(room);
}
// bir oyuncunun turu bitti mi? herkes bitirdiyse turu erken kapat
const allResolved = (room) => [...room.players.values()].every((q) => !q.alive || q.res);
function resolve(room, p, ok) {
  if (room.phase !== 'play' || room.sub !== 'round' || !p.alive || p.res) return false;
  if (ok) { p.res = 'safe'; p.safe++; p.ms += Math.min(Date.now() - room.roundStart, ROUND_MS); } else lose(room, p);
  if (allResolved(room)) endRound(room); else schedule(room);
  return true;
}
function destroy(room) {
  clearTimeout(room.timer); clearTimeout(room.flushT);
  room.hosts.forEach((res) => { sse(res, { type: 'gone' }); res.end(); });
  room.players.forEach((p) => { if (p.sse) { sse(p.sse, { type: 'gone' }); p.sse.end(); } });
  rooms.delete(room.pin);
}

/* ---------- yönlendirme ---------- */
async function handle(req, res) {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;

  if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
    fs.readFile(HTML, (err, buf) => {
      if (err) { res.writeHead(500); res.end('index.html bulunamadi (index.js ile ayni klasorde olmali).'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      res.end(buf);
    });
    return;
  }
  if (req.method === 'GET' && p === '/healthz') return send(res, 200, { ok: true, rooms: rooms.size });

  /* SSE: tahta */
  let m = p.match(/^\/api\/host\/(\d{6})$/);
  if (req.method === 'GET' && m) {
    const room = rooms.get(m[1]);
    openSSE(req, res);
    if (!room || room.key !== url.searchParams.get('key')) { sse(res, { type: 'gone' }); return res.end(); }
    room.hosts.add(res); room.last = Date.now();
    req.on('close', () => room.hosts.delete(res));
    return flush(room);
  }
  /* SSE: öğrenci */
  m = p.match(/^\/api\/play\/(\d{6})\/([0-9a-f]{16})$/);
  if (req.method === 'GET' && m) {
    const room = rooms.get(m[1]);
    const pl = room && room.players.get(m[2]);
    openSSE(req, res);
    if (!pl) { sse(res, { type: 'gone' }); return res.end(); }
    if (pl.sse && pl.sse !== res) { try { pl.sse.end(); } catch (e) { /* */ } }
    pl.sse = res; room.last = Date.now();
    req.on('close', () => { if (pl.sse === res) pl.sse = null; });
    return flush(room);
  }

  if (req.method !== 'POST' || !p.startsWith('/api/')) { res.writeHead(404); return res.end('404'); }

  let b;
  try { b = await readBody(req); } catch (e) { return send(res, 400, { err: 'bad' }); }
  if (!b || typeof b !== 'object') return send(res, 400, { err: 'bad' });

  if (p === '/api/create') {
    if (rooms.size >= MAX_ROOMS) return send(res, 503, { err: 'busy' });
    const pin = newPin();
    if (!pin) return send(res, 503, { err: 'busy' });
    const room = {
      pin, key: crypto.randomBytes(8).toString('hex'), phase: 'lobby', sub: null, round: 0, max: 15, livesInit: 2, n0: 0,
      players: new Map(), hosts: new Set(), roundStart: 0, roundEnd: 0, breakEnd: 0,
      tp: TOPICS.slice(), timer: null, flushT: null, last: Date.now()
    };
    rooms.set(pin, room);
    return send(res, 200, { pin, key: room.key, lan: lanUrls() });
  }

  const pin = String(b.pin || '');
  const room = rooms.get(pin);
  if (!room) return send(res, 404, { err: 'noroom' });
  room.last = Date.now();

  if (p === '/api/join') {
    if (room.phase !== 'lobby') return send(res, 409, { err: 'started' });
    const name = cleanName(b.name);
    if (!name) return send(res, 400, { err: 'name' });
    if (room.players.size >= MAX_PLAYERS) return send(res, 409, { err: 'full' });
    let nm = name, k = 2;
    const taken = (x) => [...room.players.values()].some((q) => q.name.toLowerCase() === x.toLowerCase());
    while (taken(nm)) nm = name.slice(0, 12) + k++;
    const id = crypto.randomBytes(8).toString('hex');
    room.players.set(id, {
      id, name: nm, lives: room.livesInit, alive: true, safe: 0, ms: 0, res: null, out: 0, joined: Date.now(), sse: null
    });
    schedule(room);
    return send(res, 200, { pid: id, name: nm });
  }
  if (p === '/api/leave') {
    const pl = room.players.get(String(b.pid || ''));
    if (pl) {
      if (pl.sse) { try { pl.sse.end(); } catch (e) { /* */ } }
      room.players.delete(pl.id);
      if (room.phase === 'play' && room.sub === 'round' && room.players.size && allResolved(room)) endRound(room);
      else schedule(room);
    }
    return send(res, 200, { ok: true });
  }
  if (p === '/api/solve' || p === '/api/fail') {
    const pl = room.players.get(String(b.pid || ''));
    if (!pl) return send(res, 404, { err: 'noplayer' });
    if (Number(b.round) === room.round) resolve(room, pl, p === '/api/solve');
    return send(res, 200, { ok: true });
  }

  /* buradan sonrası sadece tahta (öğretmen) için */
  if (b.key !== room.key) return send(res, 403, { err: 'key' });

  if (p === '/api/start') {
    if (room.phase === 'play') return send(res, 200, { ok: true });
    if (!room.players.size) return send(res, 409, { err: 'noplayers' });
    const tp = Array.isArray(b.tp) ? b.tp.filter((x) => TOPICS.includes(x)) : [];
    room.tp = tp.length ? tp : TOPICS.slice();
    room.max = clamp(b.rounds, 3, 30, 15);
    room.livesInit = clamp(b.lives, 1, 5, 2);
    room.n0 = room.players.size;
    room.round = 0;
    room.phase = 'play';
    resetPlayers(room);
    nextRound(room);
    return send(res, 200, { ok: true });
  }
  if (p === '/api/end') { finish(room); return send(res, 200, { ok: true }); }
  if (p === '/api/reset') {
    clearTimeout(room.timer);
    room.phase = 'lobby'; room.sub = null; room.round = 0;
    resetPlayers(room);
    flush(room);
    return send(res, 200, { ok: true });
  }
  if (p === '/api/close') { destroy(room); return send(res, 200, { ok: true }); }

  return send(res, 404, { err: 'nf' });
}

const server = http.createServer((req, res) => {
  handle(req, res).catch(() => { try { send(res, 500, { err: 'server' }); } catch (e) { /* */ } });
});

/* bağlantıları canlı tut, eski odaları temizle */
setInterval(() => {
  const now = Date.now();
  rooms.forEach((room) => {
    if (now - room.last > ROOM_TTL) return destroy(room);
    room.hosts.forEach((r) => { try { r.write(': ka\n\n'); } catch (e) { /* */ } });
    room.players.forEach((q) => { if (q.sse) { try { q.sse.write(': ka\n\n'); } catch (e) { /* */ } } });
  });
}, 20000).unref();

server.listen(PORT, '0.0.0.0', () => {
  console.log('Bomtik calisiyor:');
  console.log('  Bu bilgisayar : http://localhost:' + PORT);
  lanUrls().forEach((u) => console.log('  Telefonlar    : ' + u));
});
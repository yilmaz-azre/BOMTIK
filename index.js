'use strict';
/*
 * Bomtik sunucusu — tek dosya, paket kurulumu gerektirmez.
 * Çalıştır:  node index.js     (varsayılan port 3000, PORT ile değiştirilebilir)
 * index.html bu dosyanın yanında durmalıdır; sunucu onu sunar.
 *
 * Akış:
 *  - Tahta (öğretmen) /api/create ile 6 haneli kodlu oda açar.
 *  - Öğrenciler /api/join ile kodu ve takma adı gönderir.
 *  - Tahta oyunu başlatır; herkes aynı süre boyunca oynar, puan = etkisiz bomba sayısı.
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

/* ---------- oda durumu ---------- */
const ranking = (room) => [...room.players.values()].sort((a, b) => b.bombs - a.bombs || a.miss - b.miss || a.joined - b.joined);
const remaining = (room) => (room.phase === 'play' ? Math.max(0, room.endAt - Date.now()) : 0);

function flush(room) {
  clearTimeout(room.flushT); room.flushT = null;
  const r = ranking(room), rem = remaining(room);
  const hostMsg = {
    type: 'state', phase: room.phase, pin: room.pin, n: r.length, remaining: rem, duration: room.duration,
    players: r.map((p) => ({ name: p.name, bombs: p.bombs, miss: p.miss }))
  };
  room.hosts.forEach((res) => sse(res, hostMsg));
  const top = r.slice(0, 3).map((p) => ({ name: p.name, bombs: p.bombs }));
  r.forEach((p, i) => {
    if (p.res) sse(p.res, {
      type: 'state', phase: room.phase, n: r.length, remaining: rem, tp: room.tp,
      rank: i + 1, bombs: p.bombs, miss: p.miss, top
    });
  });
}
const schedule = (room) => { if (!room.flushT) room.flushT = setTimeout(() => flush(room), 250); };

function finish(room) {
  if (room.phase !== 'play') return;
  clearTimeout(room.timer);
  room.phase = 'done';
  flush(room);
}
function destroy(room) {
  clearTimeout(room.timer); clearTimeout(room.flushT);
  room.hosts.forEach((res) => { sse(res, { type: 'gone' }); res.end(); });
  room.players.forEach((p) => { if (p.res) { sse(p.res, { type: 'gone' }); p.res.end(); } });
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
    if (pl.res && pl.res !== res) { try { pl.res.end(); } catch (e) { /* */ } }
    pl.res = res; room.last = Date.now();
    req.on('close', () => { if (pl.res === res) pl.res = null; });
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
      pin, key: crypto.randomBytes(8).toString('hex'), phase: 'lobby', players: new Map(), hosts: new Set(),
      endAt: 0, duration: 300000, tp: TOPICS.slice(), timer: null, flushT: null, last: Date.now()
    };
    rooms.set(pin, room);
    return send(res, 200, { pin, key: room.key, lan: lanUrls() });
  }

  const pin = String(b.pin || '');
  const room = rooms.get(pin);
  if (!room) return send(res, 404, { err: 'noroom' });
  room.last = Date.now();

  if (p === '/api/join') {
    const name = cleanName(b.name);
    if (!name) return send(res, 400, { err: 'name' });
    if (room.players.size >= MAX_PLAYERS) return send(res, 409, { err: 'full' });
    let nm = name, k = 2;
    const taken = (x) => [...room.players.values()].some((q) => q.name.toLowerCase() === x.toLowerCase());
    while (taken(nm)) nm = name.slice(0, 12) + k++;
    const id = crypto.randomBytes(8).toString('hex');
    room.players.set(id, { id, name: nm, bombs: 0, miss: 0, lastB: 0, joined: Date.now(), res: null });
    schedule(room);
    return send(res, 200, { pid: id, name: nm });
  }
  if (p === '/api/leave') {
    const pl = room.players.get(String(b.pid || ''));
    if (pl) { if (pl.res) { try { pl.res.end(); } catch (e) { /* */ } } room.players.delete(pl.id); schedule(room); }
    return send(res, 200, { ok: true });
  }
  if (p === '/api/progress') {
    const pl = room.players.get(String(b.pid || ''));
    if (!pl) return send(res, 404, { err: 'noplayer' });
    if (room.phase !== 'play') return send(res, 200, { ok: true });
    const bombs = Math.floor(Number(b.bombs)), miss = Math.floor(Number(b.miss)), now = Date.now();
    if (bombs === pl.bombs + 1 && now - pl.lastB >= 2000) { pl.bombs = bombs; pl.lastB = now; }
    if (miss === pl.miss + 1) pl.miss = miss;
    schedule(room);
    return send(res, 200, { ok: true });
  }

  /* buradan sonrası sadece tahta (öğretmen) için */
  if (b.key !== room.key) return send(res, 403, { err: 'key' });

  if (p === '/api/start') {
    if (room.phase === 'play') return send(res, 200, { ok: true });
    if (!room.players.size) return send(res, 409, { err: 'noplayers' });
    const min = Math.min(30, Math.max(1, Math.floor(Number(b.minutes)) || 5));
    const tp = Array.isArray(b.tp) ? b.tp.filter((x) => TOPICS.includes(x)) : [];
    room.tp = tp.length ? tp : TOPICS.slice();
    room.duration = min * 60000;
    room.players.forEach((q) => { q.bombs = 0; q.miss = 0; q.lastB = 0; });
    room.phase = 'play';
    room.endAt = Date.now() + room.duration;
    clearTimeout(room.timer);
    room.timer = setTimeout(() => finish(room), room.duration + 500);
    flush(room);
    return send(res, 200, { ok: true });
  }
  if (p === '/api/end') { finish(room); return send(res, 200, { ok: true }); }
  if (p === '/api/reset') {
    clearTimeout(room.timer);
    room.phase = 'lobby';
    room.players.forEach((q) => { q.bombs = 0; q.miss = 0; q.lastB = 0; });
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
    room.players.forEach((q) => { if (q.res) { try { q.res.write(': ka\n\n'); } catch (e) { /* */ } } });
  });
}, 20000).unref();

server.listen(PORT, '0.0.0.0', () => {
  console.log('Bomtik calisiyor:');
  console.log('  Bu bilgisayar : http://localhost:' + PORT);
  lanUrls().forEach((u) => console.log('  Telefonlar    : ' + u));
});
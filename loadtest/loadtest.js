#!/usr/bin/env node
// FPS server load test: simulates real players (create room -> join -> start match -> 20Hz state).
// Measures: concurrent connections, retention (unexpected disconnects), join latency,
// input->broadcast latency (via yaw marker), snapshot interval jitter, HTTP API latency.
//
// Usage: node loadtest.js --rooms 5 --per-room 2 --hold 60 [--ramp-ms 300] [--base https://host/fps/]
const WebSocket = require('ws');
const { HttpsProxyAgent } = require('https-proxy-agent');

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, arr) => {
  if (v.startsWith('--')) a.push([v.slice(2), arr[i + 1] && !arr[i + 1].startsWith('--') ? arr[i + 1] : true]);
  return a;
}, []));
const BASE = args.base || 'https://kkh029.duckdns.org/fps/';
const WS_URL = BASE.replace(/^http/, 'ws') + 'ws';
const ROOMS = +(args.rooms || 2);
const PER_ROOM = +(args['per-room'] || 2);
const HOLD = +(args.hold || 30) * 1000;
const RAMP_MS = +(args['ramp-ms'] || 300);
const MAP = args.map || 'arena';
const CV = 'baseline-content-v1';
const agent = process.env.HTTPS_PROXY ? new HttpsProxyAgent(process.env.HTTPS_PROXY) : undefined;
if (process.env.HTTPS_PROXY && !process.env.NODE_USE_ENV_PROXY) {
  console.error('set NODE_USE_ENV_PROXY=1 so fetch() uses the proxy'); process.exit(1);
}

const now = () => performance.now();
const M = {
  roomCreate: [], roomCreateFail: {}, wsOpen: [], join: [], joinFail: {},
  http: [], httpFail: 0, echo: [], snapGap: [], gapsOver150: 0, gapsOver500: 0,
  snaps: 0, snapBytes: 0, unexpectedClose: [], peakConnected: 0, connected: 0,
};
const t0 = now();
const pct = (a, p) => { if (!a.length) return NaN; const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(p / 100 * s.length))]; };
const stat = a => a.length ? `n=${a.length} avg=${(a.reduce((x, y) => x + y, 0) / a.length).toFixed(1)} p50=${pct(a, 50).toFixed(1)} p95=${pct(a, 95).toFixed(1)} p99=${pct(a, 99).toFixed(1)} max=${a.reduce((x, y) => (y > x ? y : x), -Infinity).toFixed(1)}` : 'n=0';
const bump = (o, k) => { o[k] = (o[k] || 0) + 1; };
const sleep = ms => new Promise(r => setTimeout(r, ms));
let stopping = false;
const clients = [];

async function createRoom() {
  const s = now();
  try {
    const r = await fetch(BASE + 'api/rooms', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ map: MAP, difficulty: 'NORMAL', fill: 'map', visibility: 'private', name: '' }) });
    const j = await r.json().catch(() => null);
    if (!r.ok || !j?.code) { bump(M.roomCreateFail, 'http' + r.status); return null; }
    M.roomCreate.push(now() - s); return j.code;
  } catch (e) { bump(M.roomCreateFail, e.cause?.code || e.message); return null; }
}

function player(code, name, isHost) {
  return new Promise(resolve => {
    const c = { name, ws: null, joined: false, id: null, timer: null, lastSnap: 0, pending: new Map(), seq: 0, inMatch: false };
    clients.push(c);
    const s = now();
    const ws = new WebSocket(WS_URL, { agent, headers: { Origin: new URL(BASE).origin }, handshakeTimeout: 20000 });
    c.ws = ws;
    const send = o => ws.readyState === 1 && ws.send(JSON.stringify(o));
    c.send = send;
    let done = false; const fin = v => { if (!done) { done = true; resolve(v); } };
    ws.on('open', () => { M.wsOpen.push(now() - s); send({ t: 'join', room: code, name, readinessProtocol: 1, protocol: 1 }); });
    ws.on('message', raw => {
      let j; try { j = JSON.parse(raw); } catch { return; }
      if (j.t === 'welcome') {
        c.joined = true; c.id = j.you; M.join.push(now() - s); M.connected++; M.peakConnected = Math.max(M.peakConnected, M.connected);
        send({ t: 'preparing', preparationId: 1 });
        send({ t: 'ready', map: j.map, contentVersion: CV, preparationId: 1 });
        if (!isHost) send({ t: 'lobbyready', on: true });
        fin(c);
      } else if (j.t === 'error' && !c.joined) { bump(M.joinFail, j.code || 'error'); fin(null); }
      else if (j.t === 'load') { send({ t: 'preparing', preparationId: 2 }); send({ t: 'ready', map: j.map, contentVersion: CV, preparationId: 2 }); }
      else if (j.t === 'countdown' && !c.entered) { c.entered = true; send({ t: 'entered' }); }
      else if (j.t === 'spawn') { c.spawn = j.spawn; c.life = j.life; c.inMatch = true; startState(c); }
      else if (j.t === 'end') { c.inMatch = false; if (isHost && !stopping) setTimeout(() => { send({ t: 'lobby' }); setTimeout(() => send({ t: 'start' }), 1500); }, 1000); }
      else if (j.t === 'snap') {
        const t = now(); M.snaps++; M.snapBytes += raw.length;
        if (c.lastSnap && c.inMatch) { const g = t - c.lastSnap; M.snapGap.push(g); if (g > 150) M.gapsOver150++; if (g > 500) M.gapsOver500++; }
        c.lastSnap = t;
        const me = j.s?.find(r => r[0] === c.id);
        if (me && c.pending.size) {
          const key = me[4].toFixed(2);
          const sent = c.pending.get(key);
          if (sent !== undefined) { M.echo.push(t - sent); c.pending.clear(); }
        }
      }
    });
    ws.on('error', e => { if (!c.joined) { bump(M.joinFail, e.message.slice(0, 60)); fin(null); } });
    ws.on('unexpected-response', (q, r) => { bump(M.joinFail, 'http' + r.statusCode); fin(null); });
    ws.on('close', code => {
      clearInterval(c.timer);
      if (c.joined) { M.connected--; if (!stopping) M.unexpectedClose.push({ name, code, at: ((now() - t0) / 1000).toFixed(1) }); }
      fin(null);
    });
  });
}

function startState(c) {
  clearInterval(c.timer);
  const p = c.spawn;
  // 20Hz like the real client; yaw doubles as a latency marker (one marker every 500ms).
  c.timer = setInterval(() => {
    c.seq++;
    let yaw = c.yaw ?? 0;
    if (c.seq % 10 === 0) {
      yaw = ((c.seq / 10) % 600) / 100 - 3; // distinct 2-decimal value in [-3,3)
      c.yaw = yaw;
      c.pending.clear(); c.pending.set(yaw.toFixed(2), now());
    }
    c.send({ t: 'state', p: [p.x, p.y, p.z], yaw, pitch: 0, life: c.life, pose: 0 });
  }, 50);
}

async function httpProbeLoop() {
  while (!stopping) {
    const s = now();
    try { const r = await fetch(BASE + 'api/rooms', { cache: 'no-store' }); await r.text(); if (r.ok) M.http.push(now() - s); else M.httpFail++; }
    catch { M.httpFail++; }
    await sleep(1000);
  }
}

function report(label) {
  const el = ((now() - t0) / 1000).toFixed(0);
  console.log(`\n=== ${label} (t=${el}s) ===`);
  console.log(`connected now=${M.connected} peak=${M.peakConnected} target=${ROOMS * PER_ROOM}`);
  console.log(`room create ms: ${stat(M.roomCreate)} fail=${JSON.stringify(M.roomCreateFail)}`);
  console.log(`ws open ms:     ${stat(M.wsOpen)}`);
  console.log(`join ms:        ${stat(M.join)} fail=${JSON.stringify(M.joinFail)}`);
  console.log(`HTTP GET ms:    ${stat(M.http)} fail=${M.httpFail}`);
  console.log(`input->snap ms: ${stat(M.echo)}`);
  console.log(`snap gap ms:    ${stat(M.snapGap)} >150ms=${M.gapsOver150} >500ms=${M.gapsOver500}`);
  console.log(`snaps=${M.snaps} rx=${(M.snapBytes / 1e6).toFixed(1)}MB unexpectedClose=${M.unexpectedClose.length}`);
}

(async () => {
  console.log(`target ${WS_URL}: rooms=${ROOMS} perRoom=${PER_ROOM} hold=${HOLD / 1000}s`);
  const probe = httpProbeLoop();
  const hosts = [];
  for (let r = 0; r < ROOMS; r++) {
    const code = await createRoom();
    if (!code) { await sleep(RAMP_MS); continue; }
    const host = await player(code, `lt${r}h`, true);
    for (let i = 1; i < PER_ROOM; i++) { await sleep(RAMP_MS); await player(code, `lt${r}g${i}`, false); }
    if (host) { hosts.push(host); setTimeout(() => host.send({ t: 'start' }), 1500); }
    await sleep(RAMP_MS);
  }
  report('ramp complete');
  const iv = setInterval(() => report('hold'), Math.max(10000, HOLD / 4));
  await sleep(HOLD);
  clearInterval(iv);
  report('FINAL');
  if (M.unexpectedClose.length) console.log('unexpected closes:', JSON.stringify(M.unexpectedClose.slice(0, 20)));
  stopping = true;
  for (const c of clients) try { c.ws.close(); } catch {}
  if (args.json) require('fs').writeFileSync(args.json, JSON.stringify({ args, M: { ...M, snapGap: undefined, echo: undefined, http: undefined, join: undefined, wsOpen: undefined, roomCreate: undefined },
    join: stat(M.join), http: stat(M.http), echo: stat(M.echo), snapGap: stat(M.snapGap), wsOpen: stat(M.wsOpen),
    raw: { join: M.join, http: M.http, echo: M.echo, snapGapP: [50, 90, 95, 99, 99.9].map(p => pct(M.snapGap, p)) } }));
  await probe.catch(() => {});
  setTimeout(() => process.exit(0), 500);
})();

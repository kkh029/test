#!/usr/bin/env node
// HTTP throughput/latency at fixed concurrency levels.
// Usage: NODE_USE_ENV_PROXY=1 node httpburst.js [--path api/rooms] [--levels 10,25,50] [--secs 15]
const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, arr) => (v.startsWith('--') && a.push([v.slice(2), arr[i + 1]]), a), []));
const BASE = args.base || 'https://kkh029.duckdns.org/fps/';
const URL_ = BASE + (args.path ?? 'api/rooms');
const LEVELS = (args.levels || '10,25,50').split(',').map(Number);
const SECS = +(args.secs || 15);
const pct = (s, p) => s[Math.min(s.length - 1, Math.floor(p / 100 * s.length))];

(async () => {
  console.log(`GET ${URL_}`);
  for (const c of LEVELS) {
    const lat = []; const codes = {}; let errs = 0; const end = performance.now() + SECS * 1000;
    await Promise.all(Array.from({ length: c }, async () => {
      while (performance.now() < end) {
        const s = performance.now();
        try { const r = await fetch(URL_, { cache: 'no-store' }); await r.arrayBuffer(); codes[r.status] = (codes[r.status] || 0) + 1; if (r.ok) lat.push(performance.now() - s); }
        catch { errs++; }
      }
    }));
    lat.sort((a, b) => a - b);
    console.log(`c=${c}: ${(lat.length / SECS).toFixed(1)} req/s ok=${lat.length} codes=${JSON.stringify(codes)} errs=${errs} ` +
      `p50=${pct(lat, 50)?.toFixed(0)} p95=${pct(lat, 95)?.toFixed(0)} p99=${pct(lat, 99)?.toFixed(0)} max=${lat.at(-1)?.toFixed(0)} ms`);
    await new Promise(r => setTimeout(r, 3000));
  }
})();

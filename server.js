// Sentinel-Node dashboard server: ingest API, detection/alerts, passenger reports, live SSE stream.
const express = require('express');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { DatabaseSync } = require('node:sqlite');

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.SENTINEL_API_KEY || ''; // if set, ESP32 must send header x-api-key
const SIMULATE = process.env.SIMULATE !== '0';
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'sentinel.db');
const UPLOAD_DIR = path.join(__dirname, 'uploads');
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ---------- database ----------
const db = new DatabaseSync(DB_FILE);
db.exec(`
CREATE TABLE IF NOT EXISTS nodes (
  id TEXT PRIMARY KEY, name TEXT, lat REAL, lng REAL, last_seen INTEGER
);
CREATE TABLE IF NOT EXISTS readings (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, node_id TEXT NOT NULL,
  label TEXT NOT NULL, confidence REAL, anomaly REAL, rms REAL, peak REAL, dominant_hz REAL
);
CREATE INDEX IF NOT EXISTS idx_readings_node_ts ON readings(node_id, ts);
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT NOT NULL, started INTEGER NOT NULL,
  ended INTEGER, peak_anomaly REAL, status TEXT NOT NULL DEFAULT 'active'
);
CREATE TABLE IF NOT EXISTS reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, lat REAL NOT NULL, lng REAL NOT NULL,
  type TEXT NOT NULL, severity TEXT NOT NULL, description TEXT, photo TEXT, contact TEXT,
  status TEXT NOT NULL DEFAULT 'new', nearest_node TEXT, nearest_m REAL
);
`);

const NODES = [
  { id: 'SN-01', name: 'Gate A Access Road', lat: 3.0738, lng: 101.5183 },
  { id: 'SN-02', name: 'Drainage Culvert B', lat: 3.0752, lng: 101.5201 },
  { id: 'SN-03', name: 'Car Park Ramp C', lat: 3.0721, lng: 101.5214 },
];
const upsertNode = db.prepare(`INSERT INTO nodes(id,name,lat,lng,last_seen) VALUES(?,?,?,?,NULL)
  ON CONFLICT(id) DO UPDATE SET name=excluded.name, lat=excluded.lat, lng=excluded.lng`);
for (const n of NODES) upsertNode.run(n.id, n.name, n.lat, n.lng);

// ---------- helpers ----------
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
function haversine(lat1, lng1, lat2, lng2) {
  const R = 6371000, r = (d) => (d * Math.PI) / 180;
  const a = Math.sin(r(lat2 - lat1) / 2) ** 2 + Math.cos(r(lat1)) * Math.cos(r(lat2)) * Math.sin(r(lng2 - lng1) / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

// ---------- live stream (SSE) ----------
const clients = new Set();
function broadcast(event, data) {
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of clients) res.write(msg);
}

// ---------- detection / alerting ----------
const ALERT_ANOMALY = 0.7;   // anomaly score threshold
const ALERT_CONSECUTIVE = 3; // readings in a row to raise an alert (debounce)
const CLEAR_CONSECUTIVE = 5; // safe readings in a row to clear
const streaks = new Map();   // node_id -> {bad, good}
const insReading = db.prepare(`INSERT INTO readings(ts,node_id,label,confidence,anomaly,rms,peak,dominant_hz) VALUES(?,?,?,?,?,?,?,?)`);
const touchNode = db.prepare(`UPDATE nodes SET last_seen=? WHERE id=?`);
const activeAlert = db.prepare(`SELECT * FROM alerts WHERE node_id=? AND status='active'`);
const insAlert = db.prepare(`INSERT INTO alerts(node_id,started,peak_anomaly) VALUES(?,?,?)`);
const bumpAlert = db.prepare(`UPDATE alerts SET peak_anomaly=MAX(peak_anomaly,?) WHERE id=?`);
const endAlert = db.prepare(`UPDATE alerts SET status='cleared', ended=? WHERE id=?`);

function ingest(r) {
  const ts = Date.now();
  insReading.run(ts, r.node_id, r.label, r.confidence, r.anomaly, r.rms, r.peak, r.dominant_hz);
  touchNode.run(ts, r.node_id);
  const reading = { ...r, ts };
  broadcast('reading', { ...reading, spectrum: r.spectrum, waveform: r.waveform });

  const s = streaks.get(r.node_id) || { bad: 0, good: 0 };
  const isBad = r.label === 'CRITICAL_VOID' && r.anomaly >= ALERT_ANOMALY;
  if (isBad) { s.bad++; s.good = 0; } else { s.good++; s.bad = 0; }
  streaks.set(r.node_id, s);

  const active = activeAlert.get(r.node_id);
  if (!active && s.bad >= ALERT_CONSECUTIVE) {
    const id = Number(insAlert.run(r.node_id, ts, r.anomaly).lastInsertRowid);
    broadcast('alert', { id, node_id: r.node_id, started: ts, peak_anomaly: r.anomaly, status: 'active' });
  } else if (active) {
    if (isBad) bumpAlert.run(r.anomaly, active.id);
    if (s.good >= CLEAR_CONSECUTIVE) {
      endAlert.run(ts, active.id);
      broadcast('alert', { ...active, ended: ts, status: 'cleared' });
    }
  }
}

function validateReading(b) {
  if (!b || typeof b.node_id !== 'string' || !/^[\w-]{1,32}$/.test(b.node_id)) return 'node_id required';
  if (!['SAFE_SOIL', 'CRITICAL_VOID'].includes(b.label)) return 'label must be SAFE_SOIL or CRITICAL_VOID';
  return null;
}
function normalizeReading(b) {
  const confidence = clamp(num(b.confidence) ?? 0, 0, 1);
  // Edge Impulse anomaly score may be absent; fall back to void confidence.
  const anomaly = clamp(num(b.anomaly) ?? (b.label === 'CRITICAL_VOID' ? confidence : 1 - confidence), 0, 1);
  return {
    node_id: b.node_id, label: b.label, confidence, anomaly,
    rms: num(b.rms) ?? 0, peak: num(b.peak) ?? 0, dominant_hz: num(b.dominant_hz) ?? 0,
    spectrum: Array.isArray(b.spectrum) ? b.spectrum.slice(0, 64).map(Number) : undefined,
    waveform: Array.isArray(b.waveform) ? b.waveform.slice(0, 256).map(Number) : undefined,
  };
}

// ---------- app ----------
const app = express();
app.use(express.json({ limit: '8mb' }));
app.use('/uploads', express.static(UPLOAD_DIR, { setHeaders: (res) => res.set('X-Content-Type-Options', 'nosniff') }));
app.use(express.static(path.join(__dirname, 'public')));

function requireKey(req, res, next) {
  if (API_KEY && req.get('x-api-key') !== API_KEY) return res.status(401).json({ error: 'bad api key' });
  next();
}

// ESP32 ingest
app.post('/api/readings', requireKey, (req, res) => {
  const err = validateReading(req.body);
  if (err) return res.status(400).json({ error: err });
  const known = db.prepare('SELECT 1 FROM nodes WHERE id=?').get(req.body.node_id);
  if (!known) return res.status(404).json({ error: 'unknown node_id; register via POST /api/nodes' });
  ingest(normalizeReading(req.body));
  res.status(201).json({ ok: true });
});

app.post('/api/nodes', requireKey, (req, res) => {
  const { id, name, lat, lng } = req.body || {};
  if (typeof id !== 'string' || !/^[\w-]{1,32}$/.test(id) || num(lat) === null || num(lng) === null)
    return res.status(400).json({ error: 'id, lat, lng required' });
  upsertNode.run(id, String(name || id).slice(0, 80), lat, lng);
  res.status(201).json({ ok: true });
});

app.get('/api/nodes', (req, res) => {
  const rows = db.prepare('SELECT * FROM nodes ORDER BY id').all();
  const latest = db.prepare('SELECT * FROM readings WHERE node_id=? ORDER BY ts DESC LIMIT 1');
  const act = db.prepare(`SELECT id FROM alerts WHERE node_id=? AND status='active'`);
  res.json(rows.map((n) => ({ ...n, latest: latest.get(n.id) || null, alert: !!act.get(n.id) })));
});

app.get('/api/readings', (req, res) => {
  const limit = clamp(parseInt(req.query.limit) || 300, 1, 5000);
  const node = req.query.node;
  const rows = node
    ? db.prepare('SELECT * FROM readings WHERE node_id=? ORDER BY ts DESC LIMIT ?').all(String(node), limit)
    : db.prepare('SELECT * FROM readings ORDER BY ts DESC LIMIT ?').all(limit);
  res.json(rows.reverse());
});

app.get('/api/alerts', (req, res) => {
  res.json(db.prepare('SELECT * FROM alerts ORDER BY started DESC LIMIT 100').all());
});

// Passenger reports
const TYPES = ['crack', 'dip', 'hole', 'water_pooling', 'other'];
const SEVERITIES = ['low', 'medium', 'high'];
const STATUSES = ['new', 'verified', 'resolved'];
const MIME_EXT = { 'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp' };
const recentByIp = new Map();

app.post('/api/reports', (req, res) => {
  const b = req.body || {};
  // basic rate limit: 5 reports / 10 min per IP
  const now = Date.now();
  const hits = (recentByIp.get(req.ip) || []).filter((t) => now - t < 600000);
  if (hits.length >= 5) return res.status(429).json({ error: 'Too many reports, try again later.' });

  if (num(b.lat) === null || num(b.lng) === null || Math.abs(b.lat) > 90 || Math.abs(b.lng) > 180)
    return res.status(400).json({ error: 'A valid location is required.' });
  if (!TYPES.includes(b.type)) return res.status(400).json({ error: 'Invalid type.' });
  if (!SEVERITIES.includes(b.severity)) return res.status(400).json({ error: 'Invalid severity.' });
  const description = String(b.description || '').slice(0, 1000);
  const contact = String(b.contact || '').slice(0, 120);

  let photo = null;
  if (b.photo) {
    const m = /^data:(image\/(?:jpeg|png|webp));base64,([A-Za-z0-9+/=]+)$/.exec(b.photo);
    if (!m) return res.status(400).json({ error: 'Photo must be a JPEG, PNG or WebP image.' });
    const buf = Buffer.from(m[2], 'base64');
    if (buf.length > 5 * 1024 * 1024) return res.status(413).json({ error: 'Photo too large (max 5MB).' });
    photo = `${crypto.randomUUID()}.${MIME_EXT[m[1]]}`;
    fs.writeFileSync(path.join(UPLOAD_DIR, photo), buf);
  }

  let nearest = null, nearestM = null;
  for (const n of db.prepare('SELECT * FROM nodes').all()) {
    const d = haversine(b.lat, b.lng, n.lat, n.lng);
    if (nearestM === null || d < nearestM) { nearest = n.id; nearestM = d; }
  }
  const id = Number(db.prepare(`INSERT INTO reports(ts,lat,lng,type,severity,description,photo,contact,nearest_node,nearest_m)
    VALUES(?,?,?,?,?,?,?,?,?,?)`).run(now, b.lat, b.lng, b.type, b.severity, description, photo, contact, nearest, nearestM).lastInsertRowid);
  hits.push(now); recentByIp.set(req.ip, hits);
  const report = db.prepare('SELECT * FROM reports WHERE id=?').get(id);
  broadcast('report', report);
  res.status(201).json(report);
});

app.get('/api/reports', (req, res) => {
  res.json(db.prepare('SELECT * FROM reports ORDER BY ts DESC LIMIT 200').all());
});

app.patch('/api/reports/:id', (req, res) => {
  const status = req.body && req.body.status;
  if (!STATUSES.includes(status)) return res.status(400).json({ error: 'Invalid status.' });
  const r = db.prepare('UPDATE reports SET status=? WHERE id=?').run(status, parseInt(req.params.id));
  if (!r.changes) return res.status(404).json({ error: 'Not found' });
  const report = db.prepare('SELECT * FROM reports WHERE id=?').get(parseInt(req.params.id));
  broadcast('report', report);
  res.json(report);
});

app.delete('/api/reports/:id', (req, res) => {
  const id = parseInt(req.params.id);
  const r = db.prepare('SELECT * FROM reports WHERE id=?').get(id);
  if (!r) return res.status(404).json({ error: 'Not found' });
  db.prepare('DELETE FROM reports WHERE id=?').run(id);
  if (r.photo) fs.rmSync(path.join(UPLOAD_DIR, path.basename(r.photo)), { force: true });
  broadcast('report_deleted', { id });
  res.json({ ok: true });
});

// Only cleared alerts can be deleted; an active alert must resolve first.
app.delete('/api/alerts/:id', (req, res) => {
  const id = parseInt(req.params.id);
  const a = db.prepare('SELECT * FROM alerts WHERE id=?').get(id);
  if (!a) return res.status(404).json({ error: 'Not found' });
  if (a.status === 'active') return res.status(409).json({ error: 'Active alerts cannot be deleted.' });
  db.prepare('DELETE FROM alerts WHERE id=?').run(id);
  broadcast('alert_deleted', { id });
  res.json({ ok: true });
});

// Live stream
app.get('/api/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders();
  res.write('retry: 2000\n\n');
  clients.add(res);
  req.on('close', () => clients.delete(res));
});
setInterval(() => { for (const res of clients) res.write(': ping\n\n'); }, 25000).unref();

// ---------- simulator ----------
const simState = new Map(NODES.map((n) => [n.id, { void: false, until: 0, phase: Math.random() * 10 }]));
const rnd = (a, b) => a + Math.random() * (b - a);

function simulateOne(id) {
  const st = simState.get(id);
  const now = Date.now();
  if (st.void && st.until && now > st.until) st.void = false;
  const isVoid = st.void;
  // Hollow void => resonant low-frequency peak with high energy; packed soil => dull, broadband, low energy.
  const peakBin = isVoid ? 3 + Math.floor(rnd(0, 2)) : 9 + Math.floor(rnd(0, 4));
  const spectrum = Array.from({ length: 16 }, (_, i) => {
    const base = isVoid ? 0.15 : 0.1 + 0.25 * Math.exp(-i / 6);
    const peak = Math.exp(-((i - peakBin) ** 2) / (isVoid ? 1.2 : 8)) * (isVoid ? 0.85 : 0.3);
    return clamp(base + peak + rnd(-0.05, 0.05), 0, 1);
  });
  st.phase += 0.4;
  const amp = isVoid ? rnd(0.5, 0.9) : rnd(0.08, 0.25);
  const f = isVoid ? 0.9 : 3.2;
  const waveform = Array.from({ length: 96 }, (_, i) => clamp(amp * Math.sin(st.phase + i * f * 0.12) * Math.cos(i * 0.05) + rnd(-0.08, 0.08), -1, 1));
  const rms = Math.sqrt(waveform.reduce((a, v) => a + v * v, 0) / waveform.length);
  const pk = Math.max(...waveform.map(Math.abs));
  const confidence = isVoid ? rnd(0.82, 0.98) : rnd(0.85, 0.99);
  const anomaly = isVoid ? rnd(0.72, 0.97) : rnd(0.02, 0.25);
  ingest({
    node_id: id, label: isVoid ? 'CRITICAL_VOID' : 'SAFE_SOIL', confidence, anomaly,
    rms, peak: pk, dominant_hz: Math.round(peakBin * 500 + rnd(-80, 80)), spectrum, waveform,
  });
}

if (SIMULATE) {
  setInterval(() => { for (const n of NODES) simulateOne(n.id); }, 1000).unref();
}
app.post('/api/sim/trigger', (req, res) => {
  const { node_id, void: v = true, seconds = 20 } = req.body || {};
  const st = simState.get(node_id);
  if (!st) return res.status(404).json({ error: 'unknown simulated node' });
  st.void = !!v;
  st.until = v ? Date.now() + clamp(Number(seconds) || 20, 3, 300) * 1000 : 0;
  res.json({ ok: true, simulating: SIMULATE });
});

if (require.main === module) {
  app.listen(PORT, () => console.log(`Sentinel-Node dashboard: http://localhost:${PORT}  (simulator ${SIMULATE ? 'ON' : 'OFF'})`));
}
module.exports = { app, db, ingest };

// Operator dashboard: live SSE feed, canvas charts, map, alerts, report triage.
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const STALE_MS = 6000;
const THRESH = 0.7;

const nodes = new Map();       // id -> node info + latest
const live = new Map();        // id -> {spectrum, waveform}
const hist = new Map();        // id -> [{ts, anomaly}]
let selected = null;
let alerts = [];
let reports = [];
let map, nodeMarkers = {}, reportMarkers = {};

// ---------- data load ----------
async function j(url, opt) { const r = await fetch(url, opt); if (!r.ok) throw new Error(url + ' ' + r.status); return r.json(); }

async function init() {
  for (const n of await j('/api/nodes')) nodes.set(n.id, n);
  selected = [...nodes.keys()][0] || null;
  for (const id of nodes.keys()) {
    const rows = await j('/api/readings?node=' + encodeURIComponent(id) + '&limit=180');
    hist.set(id, rows.map((r) => ({ ts: r.ts, anomaly: r.anomaly })));
  }
  alerts = await j('/api/alerts');
  reports = await j('/api/reports');
  initMap();
  renderAll();
  connect();
  setInterval(renderCards, 2000); // refresh stale state
}

function connect() {
  const es = new EventSource('/api/stream');
  es.onopen = () => { $('conn').textContent = 'live'; $('conn').className = 'conn on'; };
  es.onerror = () => { $('conn').textContent = 'reconnecting…'; $('conn').className = 'conn'; };
  es.addEventListener('reading', (e) => onReading(JSON.parse(e.data)));
  es.addEventListener('alert', (e) => onAlert(JSON.parse(e.data)));
  es.addEventListener('report', (e) => onReport(JSON.parse(e.data)));
  es.addEventListener('report_deleted', (e) => {
    const { id } = JSON.parse(e.data);
    reports = reports.filter((r) => r.id !== id);
    if (reportMarkers[id]) { map.removeLayer(reportMarkers[id]); delete reportMarkers[id]; }
    renderReports();
  });
  es.addEventListener('alert_deleted', (e) => { const { id } = JSON.parse(e.data); alerts = alerts.filter((a) => a.id !== id); renderAlerts(); });
}

async function del(url, what) {
  if (!confirm(`Delete this ${what}? This cannot be undone.`)) return;
  try { await j(url, { method: 'DELETE' }); } catch { alert(`Could not delete the ${what}.`); }
}

function onReading(r) {
  const n = nodes.get(r.node_id);
  if (!n) return;
  n.latest = r; n.last_seen = r.ts;
  live.set(r.node_id, { spectrum: r.spectrum, waveform: r.waveform });
  const h = hist.get(r.node_id) || [];
  h.push({ ts: r.ts, anomaly: r.anomaly });
  if (h.length > 180) h.shift();
  hist.set(r.node_id, h);
  renderCards();
  if (r.node_id === selected) drawCharts();
}

function onAlert(a) {
  const i = alerts.findIndex((x) => x.id === a.id);
  if (i >= 0) alerts[i] = { ...alerts[i], ...a }; else alerts.unshift(a);
  const n = nodes.get(a.node_id);
  if (n) n.alert = a.status === 'active';
  renderAlerts(); renderCards();
  if (a.status === 'active' && i < 0) beep();
}

function onReport(r) {
  const i = reports.findIndex((x) => x.id === r.id);
  if (i >= 0) reports[i] = r; else reports.unshift(r);
  renderReports(); drawReportMarkers();
}

// ---------- rendering ----------
function renderAll() { renderCards(); renderAlerts(); renderReports(); drawCharts(); drawReportMarkers(); }

function isStale(n) { return !n.last_seen || Date.now() - n.last_seen > STALE_MS; }

function renderCards() {
  const active = [...nodes.values()].filter((n) => n.alert);
  const b = $('banner');
  if (active.length) {
    b.className = 'banner alert';
    b.textContent = '⚠ POSSIBLE VOID DETECTED: ' + active.map((n) => `${n.name} (${n.id})`).join(', ');
  } else {
    const offline = [...nodes.values()].filter(isStale).length;
    b.className = 'banner';
    b.textContent = '✓ All monitored sections normal' + (offline ? ` — ${offline} sensor(s) offline` : '');
  }
  $('cards').innerHTML = [...nodes.values()].map((n) => {
    const l = n.latest, stale = isStale(n), isVoid = n.alert || (!stale && l && l.label === 'CRITICAL_VOID');
    return `<div class="panel node ${isVoid ? 'void' : ''} ${stale ? 'stale' : ''} ${n.id === selected ? 'sel' : ''}" data-id="${esc(n.id)}">
      <div class="name">${esc(n.name)}</div><div class="id">${esc(n.id)}</div>
      <div class="state">${stale ? 'OFFLINE' : esc(l.label.replace('_', ' '))}</div>
      <div class="kv">${l && !stale ? `<span>Confidence <b>${(l.confidence * 100).toFixed(0)}%</b></span>
        <span>Anomaly <b>${l.anomaly.toFixed(2)}</b></span><span>RMS <b>${l.rms.toFixed(2)}</b></span>
        <span>Peak <b>${(l.dominant_hz / 1000).toFixed(1)} kHz</b></span>` : '<span>No recent data</span>'}</div></div>`;
  }).join('');
  document.querySelectorAll('.node').forEach((el) => (el.onclick = () => { selected = el.dataset.id; renderCards(); drawCharts(); }));
  const sel = nodes.get(selected); $('selName').textContent = sel ? sel.name : '–';
  for (const [id, n] of nodes) setNodeMarker(id, n);
}

function renderAlerts() {
  $('alerts').innerHTML = alerts.length ? `<table><tr><th>Node</th><th>Started</th><th>Peak</th><th></th><th></th></tr>${alerts.map((a) =>
    `<tr><td>${esc(a.node_id)}</td><td>${new Date(a.started).toLocaleTimeString()}</td><td>${(a.peak_anomaly ?? 0).toFixed(2)}</td><td><span class="tag ${esc(a.status)}">${esc(a.status)}</span></td>
    <td>${a.status === 'active' ? '' : `<button class="sm ghost" data-da="${a.id}" title="Delete alert">🗑</button>`}</td></tr>`).join('')}</table>`
    : '<div class="empty">No alerts yet.</div>';
  document.querySelectorAll('#alerts button[data-da]').forEach((b) => (b.onclick = () => del('/api/alerts/' + b.dataset.da, 'alert')));
}

function renderReports() {
  $('reports').innerHTML = reports.length ? `<table><tr><th>When</th><th>Report</th><th>Status</th></tr>${reports.map((r) => `<tr>
    <td>${new Date(r.ts).toLocaleString()}</td>
    <td>${r.photo ? `<a href="/uploads/${esc(r.photo)}" target="_blank" rel="noopener"><img class="thumb" src="/uploads/${esc(r.photo)}" alt=""></a> ` : ''}
      <b>${esc(r.type.replace('_', ' '))}</b> <span class="tag ${esc(r.severity)}">${esc(r.severity)}</span><br>
      ${esc(r.description)}<br><span class="hint">${r.nearest_node ? `Nearest sensor ${esc(r.nearest_node)} (${Math.round(r.nearest_m)} m)` : ''}${r.contact ? ' · ' + esc(r.contact) : ''}</span></td>
    <td><span class="tag ${esc(r.status)}">${esc(r.status)}</span><br>
      ${['new', 'verified', 'resolved'].filter((s) => s !== r.status).map((s) => `<button class="sm ghost" data-r="${r.id}" data-s="${s}">${s}</button>`).join(' ')}
      <button class="sm ghost" data-dr="${r.id}" title="Delete report">🗑</button></td></tr>`).join('')}</table>`
    : '<div class="empty">No passenger reports yet.</div>';
  document.querySelectorAll('#reports button[data-dr]').forEach((b) => (b.onclick = () => del('/api/reports/' + b.dataset.dr, 'report')));
  document.querySelectorAll('#reports button[data-r]').forEach((b) => (b.onclick = async () => {
    b.disabled = true;
    try { await j('/api/reports/' + b.dataset.r, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ status: b.dataset.s }) }); }
    catch { b.disabled = false; alert('Could not update report'); }
  }));
}

// ---------- map ----------
function initMap() {
  const first = [...nodes.values()][0];
  if (typeof L === 'undefined') { $('map').innerHTML = '<div class="empty">Map library failed to load (offline?). Sensor data still works.</div>'; return; }
  map = L.map('map').setView(first ? [first.lat, first.lng] : [3.07, 101.52], 16);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19, attribution: '© OpenStreetMap' }).addTo(map);
}
function setNodeMarker(id, n) {
  if (!map) return;
  const isVoid = n.alert, stale = isStale(n);
  const color = isVoid ? '#ef4444' : stale ? '#8fa0c0' : '#2fbf71';
  if (!nodeMarkers[id]) {
    nodeMarkers[id] = L.circleMarker([n.lat, n.lng], { radius: 11, weight: 3, fillOpacity: .8 }).addTo(map).bindTooltip(`${n.name} (${id})`);
  }
  nodeMarkers[id].setStyle({ color, fillColor: color });
}
function drawReportMarkers() {
  if (!map) return;
  const sevColor = { low: '#3b82f6', medium: '#f59e0b', high: '#ef4444' };
  for (const r of reports) {
    if (reportMarkers[r.id]) map.removeLayer(reportMarkers[r.id]);
    if (r.status === 'resolved') continue;
    const m = L.marker([r.lat, r.lng], { icon: L.divIcon({ className: '', html: `<div style="width:16px;height:16px;transform:rotate(45deg);background:${sevColor[r.severity]};border:2px solid #fff"></div>`, iconSize: [16, 16] }) })
      .addTo(map);
    m.bindPopup(`<b>${esc(r.type)}</b> (${esc(r.severity)}, ${esc(r.status)})<br>${esc(r.description)}`);
    reportMarkers[r.id] = m;
  }
}

// ---------- charts ----------
function setup(canvas) {
  // Logical height is captured once; the height attribute is later overwritten with the scaled pixel size.
  if (!canvas.dataset.h) canvas.dataset.h = canvas.getAttribute('height');
  const dpr = window.devicePixelRatio || 1, w = canvas.clientWidth, h = +canvas.dataset.h;
  canvas.style.height = h + 'px';
  if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) { canvas.width = Math.round(w * dpr); canvas.height = Math.round(h * dpr); }
  const c = canvas.getContext('2d'); c.setTransform(dpr, 0, 0, dpr, 0, 0); c.clearRect(0, 0, w, h);
  return { c, w, h };
}
function drawCharts() {
  const d = live.get(selected);
  { const { c, w, h } = setup($('wave')); // waveform
    c.strokeStyle = '#243556'; c.beginPath(); c.moveTo(0, h / 2); c.lineTo(w, h / 2); c.stroke();
    if (d && d.waveform) { const n = nodes.get(selected); c.strokeStyle = n && n.alert ? '#ef4444' : '#f97316'; c.lineWidth = 1.5; c.beginPath();
      d.waveform.forEach((v, i) => { const x = (i / (d.waveform.length - 1)) * w, y = h / 2 - v * h / 2 * 0.95; i ? c.lineTo(x, y) : c.moveTo(x, y); }); c.stroke(); } }
  { const { c, w, h } = setup($('spec')); // spectrum
    if (d && d.spectrum) { const bw = w / d.spectrum.length; d.spectrum.forEach((v, i) => { c.fillStyle = '#3b82f6'; c.fillRect(i * bw + 1, h - v * h, bw - 2, v * h); }); } }
  { const { c, w, h } = setup($('hist')); // anomaly history
    const t = Date.now(), span = 180000, y = (v) => h - 4 - v * (h - 8);
    c.strokeStyle = '#ef4444'; c.setLineDash([4, 4]); c.beginPath(); c.moveTo(0, y(THRESH)); c.lineTo(w, y(THRESH)); c.stroke(); c.setLineDash([]);
    const pts = (hist.get(selected) || []).filter((p) => t - p.ts <= span);
    c.strokeStyle = '#2fbf71'; c.lineWidth = 1.5; c.beginPath();
    pts.forEach((p, i) => { const x = w - ((t - p.ts) / span) * w; i ? c.lineTo(x, y(p.anomaly)) : c.moveTo(x, y(p.anomaly)); }); c.stroke(); }
}
window.addEventListener('resize', drawCharts);

// ---------- demo controls & alert sound ----------
async function sim(v) {
  try { await j('/api/sim/trigger', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ node_id: selected, void: v, seconds: 20 }) }); }
  catch { alert('Simulator not available for this node.'); }
}
$('simVoid').onclick = () => sim(true);
$('simSafe').onclick = () => sim(false);
let audio;
function beep() {
  try { audio = audio || new (window.AudioContext || window.webkitAudioContext)(); const o = audio.createOscillator(); o.frequency.value = 880; o.connect(audio.destination); o.start(); o.stop(audio.currentTime + 0.25); } catch {}
}

init().catch((e) => { $('banner').className = 'banner alert'; $('banner').textContent = 'Failed to load: ' + e.message; });

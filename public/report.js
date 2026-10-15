// Passenger report form: pick location (map or GPS), optional photo, submit.
const $ = (id) => document.getElementById(id);
let pin = null, map, marker;

function setPin(lat, lng) {
  pin = { lat, lng };
  $('locHint').textContent = `Pin set at ${lat.toFixed(5)}, ${lng.toFixed(5)}`;
  if (map) { if (marker) marker.setLatLng([lat, lng]); else marker = L.marker([lat, lng]).addTo(map); }
}

(async function init() {
  let center = [3.0738, 101.5183];
  try { const ns = await (await fetch('/api/nodes')).json(); if (ns[0]) center = [ns[0].lat, ns[0].lng]; } catch {}
  if (typeof L === 'undefined') { $('pickmap').innerHTML = '<div class="empty">Map unavailable — use “Use my current location”.</div>'; return; }
  map = L.map('pickmap').setView(center, 16);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19, attribution: '© OpenStreetMap' }).addTo(map);
  map.on('click', (e) => setPin(e.latlng.lat, e.latlng.lng));
  new ResizeObserver(() => map.invalidateSize()).observe($('pickmap'));
  setTimeout(() => map.invalidateSize(), 300);
})();

$('gps').onclick = () => {
  if (!navigator.geolocation) return show('err', 'Location is not available on this device. Tap the map instead.');
  navigator.geolocation.getCurrentPosition(
    (p) => { setPin(p.coords.latitude, p.coords.longitude); if (map) map.setView([p.coords.latitude, p.coords.longitude], 18); },
    () => show('err', 'Could not get your location. Tap the map instead.'), { enableHighAccuracy: true, timeout: 10000 });
};

function show(kind, text) { const m = $('msg'); m.className = 'msg ' + kind; m.textContent = text; }

// Downscale large phone photos before upload.
function readPhoto(file) {
  return new Promise((resolve, reject) => {
    if (!file) return resolve(null);
    if (file.size > 25 * 1024 * 1024) return reject(new Error('Photo is too large.'));
    const img = new Image(), url = URL.createObjectURL(file);
    img.onload = () => {
      const s = Math.min(1, 1600 / Math.max(img.width, img.height)), c = document.createElement('canvas');
      c.width = Math.round(img.width * s); c.height = Math.round(img.height * s);
      c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
      URL.revokeObjectURL(url); resolve(c.toDataURL('image/jpeg', 0.85));
    };
    img.onerror = () => reject(new Error('Could not read that photo.'));
    img.src = url;
  });
}

$('f').onsubmit = async (e) => {
  e.preventDefault();
  if (!pin) return show('err', 'Please set a location first (tap the map or use GPS).');
  $('send').disabled = true;
  try {
    const photo = await readPhoto($('photo').files[0]);
    const r = await fetch('/api/reports', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
      ...pin, type: $('type').value, severity: $('severity').value, description: $('description').value, contact: $('contact').value, photo }) });
    const body = await r.json();
    if (!r.ok) throw new Error(body.error || 'Submit failed');
    show('ok', 'Thank you — your report was sent to the operations team.');
    $('f').reset(); pin = null; $('locHint').textContent = 'Tap the map to drop a pin.'; if (marker) { map.removeLayer(marker); marker = null; }
  } catch (err) { show('err', err.message); }
  $('send').disabled = false;
};

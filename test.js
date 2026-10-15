// End-to-end API test against a throwaway DB. Run: npm test
process.env.SIMULATE = '0';
process.env.DB_FILE = require('path').join(require('os').tmpdir(), `sentinel-test-${Date.now()}.db`);
const assert = require('node:assert');
const { app } = require('./server');

const srv = app.listen(0, async () => {
  const base = `http://localhost:${srv.address().port}`;
  const post = (p, b, m = 'POST') => fetch(base + p, { method: m, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(b) });
  try {
    const read = (label, anomaly) => ({ node_id: 'SN-01', label, confidence: 0.9, anomaly, rms: 0.4, peak: 0.8, dominant_hz: 2000 });

    assert.equal((await post('/api/readings', { node_id: 'SN-01', label: 'BOGUS' })).status, 400);
    assert.equal((await post('/api/readings', { ...read('SAFE_SOIL', 0.1), node_id: 'NOPE' })).status, 404);

    for (let i = 0; i < 2; i++) await post('/api/readings', read('CRITICAL_VOID', 0.9));
    assert.equal((await (await fetch(base + '/api/alerts')).json()).length, 0, 'no alert before 3 readings');
    await post('/api/readings', read('CRITICAL_VOID', 0.9));
    let alerts = await (await fetch(base + '/api/alerts')).json();
    assert.equal(alerts.length, 1); assert.equal(alerts[0].status, 'active');
    for (let i = 0; i < 5; i++) await post('/api/readings', read('SAFE_SOIL', 0.05));
    alerts = await (await fetch(base + '/api/alerts')).json();
    assert.equal(alerts[0].status, 'cleared');

    const good = { lat: 3.0739, lng: 101.5184, type: 'hole', severity: 'high', description: '<b>x</b>' };
    assert.equal((await post('/api/reports', { ...good, lat: 999 })).status, 400);
    assert.equal((await post('/api/reports', { ...good, type: 'zzz' })).status, 400);
    assert.equal((await post('/api/reports', { ...good, photo: 'data:text/html;base64,AAAA' })).status, 400);
    const png = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
    const ok = await post('/api/reports', { ...good, photo: png });
    assert.equal(ok.status, 201);
    const rep = await ok.json();
    assert.equal(rep.nearest_node, 'SN-01'); assert.ok(rep.photo.endsWith('.png'));
    assert.equal((await fetch(`${base}/uploads/${rep.photo}`)).status, 200);
    assert.equal((await post(`/api/reports/${rep.id}`, { status: 'verified' }, 'PATCH')).status, 200);
    assert.equal((await post(`/api/reports/${rep.id}`, { status: 'nope' }, 'PATCH')).status, 400);
    console.log('All API tests passed');
  } catch (e) { console.error('FAIL', e); process.exitCode = 1; }
  srv.close(); setTimeout(() => process.exit(), 100);
});

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');
const { CoachStore } = require('../dist/store');
const { createApp } = require('../dist/app');
const { CoachWorker, sendNotification } = require('../dist/services/worker');
const { buildPrompt, swimEvidence } = require('../dist/services/analysis');
const { validateBundle } = require('../dist/types/workout');
const { automaticSwim, readDiaryHtml, rankSwims, diaryUrl } = require('../dist/services/swimNotes');

function bundle(id = 1000000000001, sport = 'swimming') {
  return {
    schema_version: 1,
    activity: { id, name: 'Synthetic workout', sport, type: {swimming:'Swim',cycling:'Ride',running:'Run'}[sport],
      sport_type: {swimming:'Swim',cycling:'Ride',running:'Run'}[sport], distance: 1000, moving_time: 1200,
      elapsed_time: 1800, average_speed: 1000/1200, max_speed: 2, average_heartrate: null,
      max_heartrate: null, start_date_local: '2026-01-04T07:00:00', is_indoor: false,
      device_name: null, total_elevation_gain: null },
    metrics: {np_computed:null,intensity_factor:null,tss:null,ftp_watts:null,ftp_date:null,swim_rest_included:true},
    laps: [], streams: {}, recent: [], stream_summary: {},
    provenance:{pipeline_run:'synthetic-run',exported_at:'2026-01-04T01:00:00Z',quality_checked:true,source:'trisplit-gold',limitations:[]},
  };
}
function memory() { return new CoachStore(':memory:', 42); }

test('all three sports accept the shared contract; GPS and future context are rejected', () => {
  for (const sport of ['swimming','running','cycling']) validateBundle(bundle(1,sport));
  const gps = bundle(); gps.activity.start_latlng = [37,127]; assert.throws(()=>validateBundle(gps));
  const future = bundle(); future.recent=[{id:2,sport:'running',date:'2026-01-05T07:00:00',distance:1000,moving_time:300}];
  assert.throws(()=>validateBundle(future));
  const hidden = bundle(); hidden.streams.latlng=[]; assert.throws(()=>validateBundle(hidden));
  const unchecked = bundle(); unchecked.provenance.quality_checked=false; assert.throws(()=>validateBundle(unchecked));
});

test('weekly windows use all recorded sports and swim comparisons require compatible pool evidence', () => {
  const b = bundle();
  b.activity.start_date_local = '2026-02-04T07:00:00';
  b.activity.type = b.activity.sport_type = 'OpenWaterSwim';
  b.metrics.pool_length_m = 25;
  const swim = {sport:'swimming', type:'Swim', distance:1000, moving_time:1200,
    average_speed:0.8, average_heartrate:null, is_indoor:false, swim_rest_included:true};
  b.recent = [
    {...swim, id:1, date:'2026-01-28T07:00:00', pool_length_m:25},
    {...swim, id:2, date:'2026-01-28T06:59:59', pool_length_m:50},
    {...swim, id:3, date:'2026-01-07T07:00:00', pool_length_m:25, swim_rest_included:false},
    {...swim, id:4, date:'2026-01-26T07:00:00', sport:'running', type:'Run'},
  ];
  validateBundle(b);
  const evidence = JSON.parse(buildPrompt(b).split('\n관측 자료 JSON:\n')[1]);
  assert.deepEqual(evidence.recorded_volume_by_7_day_window.map(w => w.swimming.sessions), [1,1,0,1]);
  assert.equal(evidence.recorded_volume_by_7_day_window[1].running.moving_seconds, 1200);
  assert.deepEqual(evidence.comparable_sessions.map(r => r.id), [1]);
  b.recent[2].date = '2026-01-07T06:59:59';
  assert.throws(() => validateBundle(b), /historical context/);
});

test('cycling compares measured session NP and time only with available heart rate', () => {
  const b = bundle(10, 'cycling');
  b.metrics.np_computed = 200;
  b.activity.average_heartrate = 140;
  const ride = {sport:'cycling', type:'Ride', distance:5000, moving_time:1200,
    date:'2026-01-03T07:00:00', average_speed:4, average_heartrate:145, is_indoor:false, swim_rest_included:false};
  b.recent = [{...ride,id:1,np_computed:205}, {...ride,id:2,np_computed:null},
    {...ride,id:3,np_computed:195,average_heartrate:null}, {...ride,id:4,np_computed:150}];
  validateBundle(b);
  const evidence = JSON.parse(buildPrompt(b).split('\n관측 자료 JSON:\n')[1]);
  assert.deepEqual(evidence.comparable_sessions.map(r => r.id), [1]);
  assert.equal(evidence.recorded_volume_by_7_day_window[0].cycling.sessions, 4);
});

test('legacy analyses from multiple owners and processed-only activities survive migration without resending', () => {
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'coach-migration-')); const file=path.join(dir,'copy.db');
  const legacy=new Database(file);
  legacy.exec(`CREATE TABLE swim_analyses (activity_id INTEGER UNIQUE,athlete_id INTEGER,activity_name TEXT,activity_date TEXT,distance INTEGER,analysis TEXT,processed_at TEXT);
    CREATE TABLE processed_activities (id INTEGER PRIMARY KEY,activity_id INTEGER UNIQUE,athlete_id INTEGER,processed_at TEXT);
    INSERT INTO swim_analyses VALUES (1,42,'Old swim','2025-01-01',500,'original text','2025-01-01');
    INSERT INTO swim_analyses VALUES (2,99,'Other owner','2025-01-02',600,'second text','2025-01-02');
    INSERT INTO processed_activities VALUES (1,3,42,'2025-01-03');`); legacy.close();
  const store=new CoachStore(file,42);
  try {
    assert.equal(store.analyses().length,1); assert.equal(store.analyses()[0].analysis,'original text');
    assert.equal(store.db.prepare('SELECT count(*) n FROM activity_analyses').get().n,2);
    for(const id of [1,2,3]) assert.equal(store.enqueue(bundle(id)),'already_processed');
    assert.equal(store.nextDelivery(),undefined);
    assert.equal(store.db.prepare('SELECT count(*) n FROM swim_analyses').get().n,2);
  } finally {store.close();fs.rmSync(dir,{recursive:true,force:true});}
});

test('repeated ingestion and worker ticks produce one model call and one notification per activity', async () => {
  const store=memory();let calls=0,sends=0;
  try {
    for(const [id,sport] of [[1,'swimming'],[2,'running'],[3,'cycling']]) {
      assert.equal(store.enqueue(bundle(id,sport)),'queued');assert.equal(store.enqueue(bundle(id,sport)),'already_queued');
    }
    const worker=new CoachWorker(store,async()=>{calls++;return 'Observed evidence.';},async()=>{sends++;return 'sent';});
    for(let i=0;i<5;i++) await worker.tick();
    assert.equal(calls,3);assert.equal(sends,3);assert.equal(store.analyses().length,3);
    assert.equal(store.enqueue(bundle(1)),'already_processed');
    assert.deepEqual(new Set(store.analyses().map(a=>a.sport)),new Set(['swimming','running','cycling']));
  } finally {store.close();}
});

test('failed model calls remain unprocessed and page only once; empty output cannot become processed', async () => {
  const store=memory();let alerts=0;
  try {
    store.enqueue(bundle(1));
    const worker=new CoachWorker(store,async()=>{throw new Error('SECRET_REQUEST');},async()=> 'sent',async()=>{alerts++;return 'sent';});
    for(let i=0;i<5;i++) {store.db.prepare('UPDATE coach_jobs SET next_attempt=0').run();await worker.tick();}
    assert.equal(alerts,1);assert.equal(store.analyses().length,0);
    assert.equal(store.db.prepare('SELECT count(*) n FROM processed_activities').get().n,0);
    assert.equal(store.db.prepare('SELECT state FROM coach_jobs').get().state,'failed');
    assert.ok(!JSON.stringify(store.status()).includes('SECRET_REQUEST'));
    assert.equal(store.retry(1,'analysis'),true);
    await new CoachWorker(store,async()=> '').tick();assert.equal(store.analyses().length,0);
  } finally {store.close();}
});

test('delivery uncertainty is durable across restart and does not automatically resend', async () => {
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'coach-delivery-')); const file=path.join(dir,'copy.db');
  let store=new CoachStore(file,42);store.enqueue(bundle(1));
  const job=store.nextJob();store.save(job,'Saved before delivery',true);store.nextDelivery();store.close();
  store=new CoachStore(file,42);let sends=0;
  try {
    await new CoachWorker(store,async()=>{throw new Error('must not analyze');},async()=>{sends++;return 'sent';}).tick();
    assert.equal(sends,0);assert.equal(store.db.prepare('SELECT state FROM coach_deliveries').get().state,'unknown');
    assert.equal(store.enqueue(bundle(1)),'already_processed');
    assert.equal(store.retry(1,'delivery'),true);
    await new CoachWorker(store,async()=> '',async()=>{sends++;return 'sent';}).tick();assert.equal(sends,1);
  } finally {store.close();fs.rmSync(dir,{recursive:true,force:true});}
});

test('swim wall-rest reconstruction retains distance and refuses sensor gaps', () => {
  const b=bundle();b.activity.distance=50;
  b.streams={time:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17],
    distance:[0,5,10,15,20,24.8,24.9,25,25,25,25,25,25,25,25,25,35,50]};
  const evidence=swimEvidence(b);assert.match(evidence.timing_basis,/stream-derived/);assert.ok(evidence.rest_seconds>=10);
  assert.equal(evidence.laps.reduce((s,l)=>s+l.distance,0),50);
  b.streams.time[12]=100;assert.doesNotMatch(swimEvidence(b).timing_basis,/stream-derived/);
  const prompt=buildPrompt(bundle(1,'running'));assert.match(prompt,/증상 자료는 제공되지/);assert.match(prompt,/Training Companion/);
});

test('API authentication, cutover, signed sessions and HTML escaping protect private history', async () => {
  const store=memory();store.enqueue(bundle(1));store.save(store.nextJob(),'<script>secret()</script>',false);
  const key='a'.repeat(32),secret='b'.repeat(32);
  const app=createApp(store,{ingestToken:key,adminToken:key,sessionSecret:secret,secureCookie:false,since:'2026-01-01',reportUrl:''});
  const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));
  const url=`http://127.0.0.1:${server.address().port}`;
  try {
    assert.equal((await fetch(url+'/api/status')).status,403);
    assert.equal((await fetch(url+'/dashboard',{headers:{Cookie:'athlete_id=42'},redirect:'manual'})).status,302);
    const auth={Authorization:`Bearer ${key}`,'Content-Type':'application/json'};
    const before=bundle(4);before.activity.start_date_local='2025-12-31T07:00:00';
    assert.equal((await (await fetch(url+'/api/workouts',{method:'POST',headers:auth,body:JSON.stringify(before)})).json()).status,'before_cutover');
    const login=await fetch(url+'/login',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:`token=${key}`,redirect:'manual'});
    assert.equal(login.status,302);const cookie=login.headers.get('set-cookie').split(';')[0];
    const html=await (await fetch(url+'/dashboard',{headers:{Cookie:cookie}})).text();
    assert.ok(html.includes('&lt;script&gt;secret()&lt;/script&gt;'));assert.ok(!html.includes('<script>secret()'));
    const tampered=cookie.replace('42','99');assert.equal((await fetch(url+'/dashboard',{headers:{Cookie:tampered},redirect:'manual'})).status,302);
    assert.equal((await fetch(url+'/api/retry/delivery/1',{method:'POST',headers:auth,body:'{}'})).status,400);

    const historical = {activity_id:8,start_local:'2025-10-27T07:03:00',distance:1500,moving_time:2700};
    const importCatalog = records => fetch(url+'/api/swim-catalog',{method:'POST',headers:auth,body:JSON.stringify(records)});
    assert.equal((await importCatalog([{...historical,lat:50}])).status,400);
    assert.equal((await importCatalog([historical])).status,200);
    const deepLink = await fetch(url+'/swim-notes/8',{redirect:'manual'});
    assert.equal(deepLink.headers.get('location'),'/login?next=%2Fswim-notes%2F8');
    const signed = {Cookie:cookie,'Content-Type':'application/x-www-form-urlencoded'};
    const post = (route,values) => fetch(url+route,{method:'POST',headers:signed,body:new URLSearchParams(values),redirect:'manual'});
    const detail = await (await fetch(url+'/swim-notes/8',{headers:signed})).text();
    const csrf = /name="csrf" value="([a-f0-9]+)"/.exec(detail)[1];
    assert.equal((await post('/swim-notes/8',{body:'forged'})).status,403);
    const noteUrl = 'https://example.test/swim';
    const preview = await (await post('/swim-notes/preview',{links:noteUrl,date:'2025-10-27',csrf})).text();
    assert.match(preview,/2025-10-27 07:03 · 1,500m/);
    assert.equal(store.swimNote(8),undefined);
    assert.equal((await post('/swim-notes/8/link',{url:noteUrl,csrf})).status,303);
    assert.equal((await post('/swim-notes/1/link',{url:noteUrl,csrf})).status,409);
    assert.equal((await post('/swim-notes/8',{url:noteUrl,body:'<script>private</script>',csrf})).status,303);
    assert.match(await (await fetch(url+'/swim-notes/8',{headers:signed})).text(),/&lt;script&gt;private&lt;\/script&gt;/);
    assert.equal((await post('/swim-notes/8/remove',{csrf})).status,303);
    assert.equal(store.swimNote(8),undefined);
    const firstImport = await (await post('/swim-notes/import',{links:noteUrl,date:'2025-10-27',csrf})).text();
    assert.match(firstImport,/자동 연결됨/);
    assert.equal(store.swimNote(8).url,noteUrl);
    const repeatImport = await (await post('/swim-notes/import',{links:noteUrl,csrf})).text();
    assert.match(repeatImport,/이미 연결됨/);
    assert.doesNotMatch(repeatImport,/\/8\/link/);
    assert.match(await (await fetch(url+'/swim-notes',{headers:signed})).text(),/최근 연결한 후기/);
    assert.equal((await post('/swim-notes/8/link',{url:noteUrl,csrf})).status,303);
    assert.equal((await importCatalog([historical,{...historical,activity_id:9}])).status,200);
    const anotherUrl = 'https://example.test/other';
    const ambiguous = await (await post('/swim-notes/import',{links:anotherUrl,date:'2025-10-27',csrf})).text();
    assert.match(ambiguous,/다른 후기 연결됨/);
    assert.match(ambiguous,/\/9\/link/);
    assert.equal(store.swimNote(9),undefined);
    assert.equal(store.analyses().length,1);
  } finally {await new Promise(r=>server.close(r));store.close();}
});

test('diary suggestions prefer an explicit earlier swim date and rank compatible distance and time', () => {
  const html = '<meta property="og:title" content="강습완 - 수영일기 미니 갤러리">'
    + '<span class="gall_date" title="2026-02-05 10:20:00"></span>'
    + '<div class="write_div"><p>2026-02-04 강습</p><p>총 거리 1.5 km</p><p>운동 시간 45분</p></div>';
  const post = readDiaryHtml(html);
  assert.equal(post.date,'2026-02-04');
  assert.equal(post.dateSource,'text');
  const r = {activity_id:1,start_local:'2026-02-04T07:03:00',distance:1500,moving_time:2700};
  assert.deepEqual(rankSwims([r,{...r,activity_id:2,start_local:'2026-02-04T19:00:00',distance:1000}],post)
    .map(r => r.activity_id),[1,2]);
  assert.equal(automaticSwim([r,{...r,activity_id:2,distance:1000}],false,post)?.activity_id,1);
  const shortPost = {title:'강습완',date:'2026-02-04',dateSource:'published',published:'2026-02-04T09:00:00'};
  assert.equal(automaticSwim([r],false,shortPost)?.activity_id,1);
  assert.equal(automaticSwim([r],false,{...shortPost,title:'밀린 강습일기'}),undefined);
  assert.equal(automaticSwim([r],false,{...shortPost,dateSource:'ambiguous'}),undefined);
  assert.equal(diaryUrl('https://gall.dcinside.com/mini/board/view/?id=swimmingdiary&no=12&page=2'),
    'https://gall.dcinside.com/mini/board/view/?id=swimmingdiary&no=12');
  assert.equal(diaryUrl('https://gall.dcinside.com.example.com/mini/board/view/?id=swimmingdiary&no=12'),undefined);
});

test('notification errors have explicit failure states and redirects do not leak webhook requests', async () => {
  const original=global.fetch;const a={sport:'running',activity_name:'Test',activity_date:'2026-01-01',distance:5000,analysis:'example'};
  try {
    global.fetch=async(_url,opts)=>{assert.equal(opts.redirect,'error');return {ok:false,status:500};};
    assert.equal(await sendNotification('http://example.invalid',a),'unknown');
    global.fetch=async()=>({ok:false,status:429});assert.equal(await sendNotification('http://example.invalid',a),'failed');
    global.fetch=async(_url,opts)=>{
      assert.match(JSON.parse(opts.body).text,/<https:\/\/coach.example\/swim-notes\/5\|후기 연결>/);
      return {ok:true};
    };
    assert.equal(await sendNotification('http://example.invalid',{...a,activity_id:5,sport:'swimming'},'https://coach.example'),'sent');
    global.fetch=async()=>{throw new Error('timeout');};assert.equal(await sendNotification('http://example.invalid',a),'unknown');
  } finally {global.fetch=original;}
});

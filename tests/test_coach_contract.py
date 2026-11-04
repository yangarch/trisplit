import importlib.util
import gzip
import json
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from coach_contract import build_bundle, summarize_streams
from export_coach import atomic_json
from publish_coach import publish

spec = importlib.util.spec_from_file_location('intervals_fetch', ROOT / 'ingest/intervals/fetch.py')
fetch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch)


def row(id=1, sport='swimming', when='2026-01-04T07:00:00Z'):
    return dict(activity_id=id, sport=sport, type={'swimming':'Swim','cycling':'Ride','running':'Run'}[sport],
                name='Synthetic', distance_km=1, moving_time=1200, elapsed_time=1800,
                is_indoor=False, average_speed=1, raw={'id':id,'start_date_local':when})


class CoachContractTests(unittest.TestCase):
    def test_swim_detail_upgrade_runs_once_without_recollecting_other_sports(self):
        activity = {'id':'i123','name':'Synthetic','type':'Swim','start_date_local':'2026-01-04T07:00:00',
                    'start_date':'2026-01-03T22:00:00Z','distance':50,'moving_time':60,'elapsed_time':100}
        calls=[]
        def get(url):
            calls.append(url)
            if '/athlete/0/activities?' in url: return [activity]
            if '/activity/i123?' in url: return {**activity,'icu_intervals':[{'distance':25,'moving_time':30},{'distance':0,'moving_time':40},{'distance':25,'moving_time':30}]}
            raise AssertionError(url)
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            with patch.multiple(fetch,RAW_DIR=root/'raw',NORM_DIR=root/'norm',STREAMS_DIR=root/'streams',STATE_PATH=root/'state.json',CUTOVER='2026-01-01'), patch.object(fetch,'get',side_effect=get), patch.object(fetch.time,'sleep'):
                state={'seen':{'i123':fetch.fingerprint(activity)}}
                fetch.save_state(state)
                fetch.cmd_sync(SimpleNamespace(refresh=False,dry=False))
                output=json.loads(next((root/'norm').glob('*.json')).read_text())
                self.assertEqual([l['distance'] for l in output['laps']],[25,0,25])
                self.assertEqual(fetch.load_state()['seen']['i123'],fetch.fingerprint(activity))
                fetch.cmd_sync(SimpleNamespace(refresh=False,dry=False))
                self.assertEqual(sum('/activity/i123?' in url for url in calls),1)

    def test_laps_preserve_zero_distance_rests_and_null_speed_fallback(self):
        laps = fetch.normalize_laps([{'distance':25,'elapsed_time':30,'average_speed':None}, {'distance':0,'elapsed_time':45}])
        self.assertEqual([l['distance'] for l in laps], [25,0])
        self.assertEqual(laps[0]['average_speed'], 25/30)
        self.assertEqual(laps[1]['moving_time'], 45)

    def test_no_gps_tokens_or_future_activity_reaches_bundle(self):
        r=row();raw={**r['raw'],'start_latlng':[37,127],'description':'secret','token':'secret'}
        history=[row(2,'running','2026-01-03T07:00:00Z'),row(3,'cycling','2026-01-05T07:00:00Z'),row(4,'swimming','2025-11-01T07:00:00Z')]
        points=[dict(point_idx=i,time_offset_s=i,distance_m=i,lat=37,lon=127,watts=100) for i in range(3)]
        b=build_bundle(r,raw,history,points,run_id='test',exported_at='2026-01-04T01:00:00Z')
        self.assertEqual([h['id'] for h in b['recent']], [2])
        text=json.dumps(b)
        for word in ['latlng','"lat"','"lon"','description','secret']:
            self.assertNotIn(word,text)
        self.assertEqual(b['activity']['start_date_local'],'2026-01-04T07:00:00')
        self.assertTrue(b['provenance']['quality_checked'])

    def test_stream_gaps_and_missing_power_are_not_imputed_zero(self):
        points=[dict(time_offset_s=0,watts=0,speed_ms=2),dict(time_offset_s=1,watts=None,speed_ms=2),
                dict(time_offset_s=2,watts=200,speed_ms=2),dict(time_offset_s=20,watts=300,speed_ms=2)]
        s=summarize_streams(points,20)
        self.assertEqual(s['recorded_seconds'],2)
        self.assertEqual(s['first_watts'],0)
        self.assertEqual(s['first_watts_seconds'],1)
        self.assertIsNone(s['second_watts'])
        self.assertEqual(s['time_coverage'],0.1)

    def test_flagged_power_dropouts_withhold_metrics_instead_of_claiming_fatigue(self):
        r=row(1,'cycling');r.update(power_dropout=True,np_computed=200,tss=100,intensity_factor=0.8)
        b=build_bundle(r,r['raw'],[],[],run_id='test',exported_at='test')
        self.assertIsNone(b['metrics']['np_computed']);self.assertIsNone(b['stream_summary']['first_watts'])
        self.assertTrue(any('dropout' in s for s in b['provenance']['limitations']))

    def test_publisher_retry_after_lost_ack_does_not_duplicate_queued_id(self):
        received=set();calls=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
                data=self.rfile.read(int(self.headers['Content-Length']))
                self.server.test_case.assertEqual(self.headers.get('Content-Encoding'),'gzip')
                b=json.loads(gzip.decompress(data))
                aid=b['activity']['id'];calls.append(aid);already=aid in received;received.add(aid)
                self.send_response(200);self.end_headers();self.wfile.write(json.dumps({'status':'already_queued' if already else 'queued'}).encode())
        server=HTTPServer(('127.0.0.1',0),Handler);server.test_case=self
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            with tempfile.TemporaryDirectory() as temp:
                out=Path(temp);r=row();b=build_bundle(r,r['raw'],[],[],run_id='run',exported_at='test')
                atomic_json(out/'1.json',b);atomic_json(out/'manifest.json',{'schema_version':1,'pipeline_run':'run','files':['1.json']})
                url=f'http://127.0.0.1:{server.server_port}/api/workouts'
                self.assertEqual(publish(out,url,'a'*32),1)
                (out/'published.json').unlink() # Simulate client dying before persisting acknowledgement.
                self.assertEqual(publish(out,url,'a'*32),1)
                self.assertEqual(publish(out,url,'a'*32),0)
                self.assertEqual(received,{1});self.assertEqual(calls,[1,1])
        finally:
            server.shutdown();server.server_close();thread.join()


if __name__ == '__main__': unittest.main()

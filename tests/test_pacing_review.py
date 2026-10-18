"""Synthetic inputs only. python3 -m unittest discover -s tests -v"""
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import pacing_review as p
import review_pacing as source


def meta(identifier="1", day="2026-10-09", **extra):
    return {"activity_id": identifier, "date": day, "start_utc": day+"T00:00:00+00:00",
            "name": "synthetic", "moving_time": 10800, "elapsed_time": 10800,
            "gear_id": "bike-a", "is_indoor": False, "device_watts": True, **extra}


def point(t, watts=200, **extra):
    return {"t": t, "watts": watts, "cadence": 90, "moving": True, "speed_ms": 8,
            "lat": 37+t*.00005, "lon": 127, "distance_m": t*8, "ele_m": 100,
            "heartrate": 140, "temp_c": 20, **extra}


def segment(identifier, start=0, watts=200, prior_kj=0, **extra):
    return {"activity_id": identifier, "start_s": start, "end_s": start+300,
            "duration_s": 300, "avg_watts": watts, "distance_m": 2400,
            "grade_pct": 0, "anchors": [(37+i*.003, 127) for i in range(5)],
            "temp_c": 20, "prior_recorded_kj": prior_kj, "prior_unknown_s": 0,
            "prior_unknown_fraction": 0, "prior_active_s": start, "active_fraction": .7,
            "prior_above_anchor_s": 120, "prior_surge_count": 2, "heartrate": 140,
            "cadence": 90, "coverage": 1, "moving_fraction": 1, "pedaling_fraction": 1, **extra}


def ride(m, windows):
    return {"meta": m, "windows": windows, "rejections": {}}


class TimingAndQuality(unittest.TestCase):
    def test_irregular_samples_are_time_weighted(self):
        # 100W for one second, 300W for four seconds, repeated for five minutes.
        points = [point(t, 100 if t % 5 == 0 else 300) for t in range(301) if t % 5 in (0, 1)]
        r = p.prepare(meta(), points)
        self.assertAlmostEqual(r["windows"][0]["avg_watts"], 260)
        self.assertEqual(r["quality"]["recorded_power_s"], 300)

    def test_real_zero_is_valid_but_missing_is_not(self):
        points = [point(t, 0 if t < 30 else 200) for t in range(301)]
        r = p.prepare(meta(), points)
        self.assertEqual(r["windows"][0]["coverage"], 1)
        self.assertAlmostEqual(r["windows"][0]["avg_watts"], 180)
        for t in range(30):
            points[t]["watts"] = None
        r = p.prepare(meta(), points)
        self.assertEqual(r["windows"], [])
        self.assertEqual(r["rejections"]["power_gap"], 1)

    def test_recording_gaps_and_initial_missing_time_not_filled(self):
        r = p.prepare(meta(), [point(t) for t in range(60, 601) if not 240 <= t < 300])
        self.assertEqual(r["quality"]["elapsed_s"], 600)
        self.assertLess(r["quality"]["power_coverage"], .81)
        self.assertTrue(all(w["start_s"] >= 300 for w in r["windows"]))

    def test_conflicting_duplicate_timestamp_invalidates_interval(self):
        points = [point(t) for t in range(301)] + [point(t, 400) for t in range(10)]
        r = p.prepare(meta(), points)
        self.assertEqual(r["quality"]["duplicate_timestamps"], 10)
        self.assertEqual(r["windows"], [])

    def test_stops_coasting_and_descent_excluded(self):
        for change, reason in (({"moving": False}, "stops_or_unknown_motion"),
                               ({"cadence": 0}, "coasting_or_missing_cadence")):
            r = p.prepare(meta(), [point(t, **change) for t in range(301)])
            self.assertEqual(r["rejections"], {reason: 1})
        r = p.prepare(meta(), [point(t, ele_m=200-t*.2) for t in range(301)])
        self.assertEqual(r["rejections"], {"descent": 1})

    def test_estimated_power_and_indoor_never_enter_comparisons(self):
        self.assertEqual(p.prepare(meta(device_watts=False), [])['rejections'], {"estimated_power": 1})
        self.assertEqual(p.prepare(meta(is_indoor=True), [])['rejections'], {"indoor": 1})

    def test_sustained_early_events_and_unset_anchor(self):
        points = [point(t, 350 if 60 <= t < 180 else 100) for t in range(601)]
        r = p.prepare(meta(), points, 280)
        self.assertEqual(len(r["early_efforts"]["sustained_events"]), 1)
        self.assertGreater(r["early_efforts"]["above_anchor_s"], 60)
        self.assertEqual(p.prepare(meta(), points)["early_efforts"]["sustained_events"], [])


class Matching(unittest.TestCase):
    def setUp(self):
        self.target = ride(meta("2"), [segment("2", 6000, 180, 1200)])
        self.ref = ride(meta("1", "2026-10-01"), [segment("1", 0, 200)])

    def result(self, *refs):
        return p.review([self.target, *(refs or [self.ref])], "2", 283)

    def test_observed_output_change_with_auditable_reference(self):
        d = self.result()
        self.assertEqual(d["comparison_count"], 1)
        self.assertAlmostEqual(d["comparisons"][0]["observed_change_pct"], -10)
        self.assertEqual(d["comparisons"][0]["references"][0]["segment"]["activity_id"], "1")
        self.assertNotIn('"anchors"', json.dumps(d))
        self.assertNotIn('"lat"', json.dumps(d))

    def test_other_bike_route_direction_and_future_are_rejected(self):
        wrong_bike = copy.deepcopy(self.ref)
        wrong_bike["meta"]["gear_id"] = "bike-b"
        reverse = copy.deepcopy(self.ref)
        reverse["windows"][0]["anchors"].reverse()
        future = copy.deepcopy(self.ref)
        future["meta"].update(date="2026-10-09", start_utc="2026-10-09T10:00:00+00:00")
        self.assertEqual(self.result(wrong_bike, reverse, future)["comparison_count"], 0)

    def test_missing_gear_allows_only_same_activity_earlier_loop(self):
        self.target["meta"]["gear_id"] = None
        self.assertEqual(self.result()["comparison_count"], 0)
        self.target["windows"].insert(0, segment("2", 0, 200))
        self.assertEqual(self.result()["comparison_count"], 1)

    def test_large_prior_missingness_disqualifies_comparison(self):
        self.ref["windows"][0]["prior_unknown_fraction"] = .3
        self.assertEqual(self.result()["comparison_count"], 0)

    def test_selection_uses_geometry_not_highest_reference_power(self):
        self.ref["windows"].append(segment("1", 300, 500, anchors=[(37+i*.003+.0001, 127) for i in range(5)]))
        d = self.result()
        self.assertEqual(d["comparisons"][0]["reference_median_watts"], 200)

    def test_target_windows_do_not_overlap(self):
        self.target["windows"] += [segment("2", 6060, 180, 1250), segment("2", 6300, 180, 1300)]
        d = self.result()
        self.assertEqual([x["segment"]["start_s"] for x in d["comparisons"]], [6000, 6300])

    def test_html_escapes_names_and_empty_states_render(self):
        self.target["meta"]["name"] = '<script>alert(1)</script>'
        rendered = p.html(self.result())
        self.assertNotIn('<script>', rendered)
        self.assertIn('&lt;script&gt;', rendered)
        self.assertIn('<table>', rendered)
        d = p.review([], ftp=None)
        self.assertIn('3시간', p.markdown(d))
        self.assertIn('장거리 힘 배분', p.html(d))


class Sources(unittest.TestCase):
    def test_spark_adapter_streams_one_query_and_matches_source_fields(self):
        class Row(dict):
            def asDict(self):
                return dict(self)
        class Frame:
            def __init__(self, rows):
                self.rows = rows
            def collect(self):
                return self.rows
            def toLocalIterator(self):
                return iter(self.rows)
        epoch = int(source.utc("2026-10-09T00:00:00Z").timestamp())
        class Spark:
            def __init__(self):
                self.calls = []
            def sql(self, sql):
                self.calls.append(sql)
                if 'FROM tc.silver.activities' in sql:
                    return Frame([Row(activity_id=1, name='synthetic', start_date_key='2026-10-09',
                                      start_epoch_s=epoch, moving_time=10800, elapsed_time=600,
                                      gear_id='bike-a', is_indoor=False, device_watts=True)])
                return Frame([Row(activity_id=1, epoch_s=epoch+t,
                                  **{k:v for k,v in point(t).items() if k!='t'}) for t in range(601)])
        spark = Spark()
        d = source.from_spark(spark, 283)
        self.assertEqual(d['target']['start_utc'], '2026-10-09T00:00:00+00:00')
        self.assertEqual(d['quality']['recorded_power_s'], 600)
        self.assertEqual(d['early_efforts']['recorded_kj'], 120)
        self.assertEqual(len(spark.calls), 2)
        self.assertNotIn('"anchors"', json.dumps(d))

    def test_transitive_overlap_keeps_longest_record(self):
        a = meta("1", elapsed_time=100, start_utc="2026-10-09T00:00:00Z")
        b = meta("2", elapsed_time=110, start_utc="2026-10-09T00:01:00Z")
        c = meta("3", elapsed_time=100, start_utc="2026-10-09T00:02:00Z")
        kept, excluded = source.deduplicate([c, a, b])
        self.assertEqual([m["activity_id"] for m in kept], ["2"])
        self.assertEqual(set(excluded), {"1", "3"})

    def test_lookback_anchored_to_selected_activity_not_today(self):
        metas = [meta("1", "2026-01-01"), meta("2", "2026-10-09"), meta("3", "2026-10-10")]
        identifier, selected = source.select_metadata(metas, "2")
        self.assertEqual(identifier, "2")
        self.assertEqual([m["activity_id"] for m in selected], ["2"])

    def test_short_or_absent_stream_arrays_stay_missing(self):
        stream = {"streams": {"time": {"data": [0, 1, 2]}, "watts": {"data": [0, 200]}}}
        points = list(source.stream_points(stream))
        self.assertEqual([r['watts'] for r in points], [0, 200, None])


if __name__ == "__main__":
    unittest.main()

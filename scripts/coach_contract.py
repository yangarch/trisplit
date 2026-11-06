"""Pure, GPS-free handoff from the validated lakehouse to the coaching app."""
from __future__ import annotations

import math
from datetime import datetime, timedelta

SPORTS = {"cycling", "running", "swimming"}
LAP_FIELDS = ("id", "name", "distance", "moving_time", "elapsed_time", "average_speed",
              "max_speed", "average_heartrate", "lap_index")


def finite(value):
    return float(value) if isinstance(value, (int, float)) and math.isfinite(value) else None


def pool_length_m(value):
    length = finite(value) if not isinstance(value, bool) else None
    return length if length is not None and length > 0 else None


def power_unreliable(row: dict) -> bool:
    return bool(row.get("power_dropout")) or row.get("device_watts") is False


def local_time(raw: dict) -> str:
    # Strava-style local timestamps carry a misleading Z; preserve wall time.
    text = str(raw.get("start_date_local") or "")[:19]
    datetime.fromisoformat(text)
    return text


def summarize_streams(points: list[dict], elapsed: float) -> dict:
    """Time-weighted observed halves. Never bridge gaps >5s or impute 0W."""
    observed = 0.0
    sums = {f"{half}_{key}": [0.0, 0.0] for half in ("first", "second") for key in ("watts", "heartrate", "speed_ms")}
    for prev, cur in zip(points, points[1:]):
        start, end = finite(prev.get("time_offset_s")), finite(cur.get("time_offset_s"))
        if start is None or end is None or not 0 < end - start <= 5:
            continue
        observed += end - start
        for half, low, high in (("first", 0, elapsed / 2), ("second", elapsed / 2, elapsed)):
            dt = max(0, min(end, high) - max(start, low))
            for key in ("watts", "heartrate", "speed_ms"):
                v = finite(prev.get(key))
                # Speeds/HR during stops are not an effort comparison. 0W is real.
                if dt and v is not None and (key == "watts" or (prev.get("speed_ms") or 0) > 0.5):
                    pair = sums[f"{half}_{key}"]
                    pair[0] += v * dt
                    pair[1] += dt
    result = {"recorded_seconds": observed, "elapsed_seconds": elapsed,
              "time_coverage": min(1, observed / elapsed) if elapsed > 0 else None}
    for key, (total, duration) in sums.items():
        result[key] = total / duration if duration else None
        result[f"{key}_seconds"] = duration
    return result


def build_bundle(row: dict, raw: dict, history: list[dict], points: list[dict], *, run_id: str, exported_at: str) -> dict:
    """row/history are canonical non-duplicate Gold+Silver rows, not raw candidates."""
    sport = row["sport"]
    if sport not in SPORTS or int(raw["id"]) != int(row["activity_id"]):
        raise ValueError("Activity does not match canonical row")
    when = local_time(raw)
    activity = {
        "id": int(row["activity_id"]), "name": str(row.get("name") or sport), "sport": sport,
        "type": row["type"], "sport_type": row["type"],
        "distance": float(row["distance_km"]) * 1000,
        "moving_time": float(row["moving_time"]), "elapsed_time": float(row["elapsed_time"]),
        "average_speed": finite(row.get("average_speed")),
        "max_speed": finite(row.get("max_speed")),
        "average_heartrate": finite(row.get("average_heartrate")), "max_heartrate": finite(row.get("max_heartrate")),
        "start_date_local": when, "is_indoor": bool(row["is_indoor"]),
        "device_name": row.get("device_name"), "total_elevation_gain": finite(row.get("elev_m")),
    }
    earliest = (datetime.fromisoformat(when) - timedelta(days=28)).isoformat()
    recent = []
    for r in history:
        date = local_time(r["raw"])
        if r["sport"] not in SPORTS or not earliest <= date < when or r["activity_id"] == activity["id"]:
            continue
        recent.append({
            "id": int(r["activity_id"]), "sport": r["sport"], "date": date,
            "distance": float(r["distance_km"]) * 1000, "moving_time": float(r["moving_time"]),
            "average_speed": finite(r.get("average_speed")), "average_heartrate": finite(r.get("average_heartrate")),
            "is_indoor": bool(r["is_indoor"]), "swim_rest_included": bool(r.get("swim_rest_included")),
            "type": r["type"],
            "np_computed": finite(r.get("np_computed"))
            if r["sport"] == "cycling" and r.get("device_watts") is True and not power_unreliable(r) else None,
            "pool_length_m": pool_length_m(r.get("pool_length_m")) if r["sport"] == "swimming" else None,
        })
    recent.sort(key=lambda r: (r["date"], r["id"]), reverse=True)
    if len(recent) > 200:
        raise ValueError("Historical context exceeds contract limit")
    points = sorted(points, key=lambda p: p["point_idx"])
    streams = {}
    if sport == "swimming" and points:
        for output, key in (("time", "time_offset_s"), ("distance", "distance_m"),
                            ("velocity_smooth", "speed_ms"), ("heartrate", "heartrate")):
            values = [finite(p.get(key)) for p in points]
            if any(v is not None for v in values):
                streams[output] = values
    laps = [{k: lap[k] for k in LAP_FIELDS if k in lap} for lap in raw.get("laps", [])] if sport == "swimming" else []
    limitations = ["Training intent, symptoms, weather and recovery are not supplied.",
                   "History covers the preceding 28 days, strictly before this activity."]
    if sport == "swimming":
        limitations.append("Lesson/free-swim is unconfirmed; device type may be mislabeled. Pool setting is unknown if pool_length_m is null.")
        if len(laps) <= 1:
            limitations.append("Recorded rest laps unavailable; reconstruction requires complete monotonic streams.")
    if not points:
        limitations.append("No recorded stream points available.")
    bad_power = power_unreliable(row)
    if sport == "cycling" and bad_power:
        limitations.append("Power dropout flagged or power is estimated; power-derived metrics and half-power comparisons withheld.")
    summary = summarize_streams(points, activity["elapsed_time"])
    if bad_power:
        for key in list(summary):
            if "watts" in key:
                summary[key] = None
    return {
        "schema_version": 1, "activity": activity,
        "metrics": {**{k: None if bad_power and k != "ftp_watts" else finite(row.get(k))
                       for k in ("np_computed", "intensity_factor", "tss", "ftp_watts")},
                    "ftp_date": str(row["ftp_date"]) if row.get("ftp_date") else None,
                    "swim_rest_included": bool(row.get("swim_rest_included")),
                    "pool_length_m": pool_length_m(row.get("pool_length_m")) if sport == "swimming" else None},
        "laps": laps, "streams": streams, "recent": recent,
        "stream_summary": summary,
        "provenance": {"pipeline_run": run_id, "exported_at": exported_at,
                       "quality_checked": True, "source": "trisplit-gold", "limitations": limitations},
    }

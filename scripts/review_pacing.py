#!/usr/bin/env python3
"""Local cycling evidence for reports / an AI counseling tool. No model calls.

Default: read the existing Spark Silver tables (already deduplicated).
Offline: --source backup --root /path/to/training-companion --ftp 250
Use --format json for machine-readable evidence, markdown/html for review.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import gzip
from itertools import groupby
import json
from pathlib import Path

import pacing_review as pacing


def utc(value):
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def deduplicate(activities):
    """Same-sport overlapping sessions: keep longest elapsed, deterministic tie.

    Matches Silver's transitive session grouping; not a filename/source preference.
    Input is already cycling only. Keep evidence of the excluded IDs separately.
    """
    groups, current, end = [], [], None
    for meta in sorted(activities, key=lambda m: (m["start_utc"], m["activity_id"])):
        start = utc(meta["start_utc"])
        if current and start >= end:
            groups.append(current)
            current, end = [], None
        current.append(meta)
        finish = start + timedelta(seconds=max(0, meta.get("elapsed_time") or 0))
        end = max(end, finish) if end else finish
    if current:
        groups.append(current)
    kept, excluded = [], []
    for group in groups:
        ranked = sorted(group, key=lambda m: (-(m.get("elapsed_time") or 0), int(m["activity_id"])))
        kept.append(ranked[0])
        excluded.extend(m["activity_id"] for m in ranked[1:])
    return kept, excluded


def backup_metadata(root):
    by_id = {}
    for folder in (root / "activities/strava/raw-gpx", root / "activities/intervals/normalized"):
        for path in sorted(folder.glob("*.json")):
            a = json.loads(path.read_text())
            kind = a.get("sport_type") or a.get("type") or ""
            if kind not in ("Ride", "VirtualRide", "EBikeRide", "GravelRide", "MountainBikeRide"):
                continue
            if not a.get("start_date") or a.get("id") is None:
                continue
            start = utc(a["start_date"])
            meta = {"activity_id": str(a["id"]), "name": a.get("name", ""),
                    "date": str(a.get("start_date_local") or a["start_date"])[:10],
                    "start_utc": start.isoformat(), "moving_time": a.get("moving_time") or 0,
                    "elapsed_time": a.get("elapsed_time") or 0, "gear_id": a.get("gear_id"),
                    "is_indoor": bool(a.get("trainer") or kind == "VirtualRide"),
                    "device_watts": a.get("device_watts")}
            by_id[meta["activity_id"]] = meta
    return deduplicate(by_id.values())


def select_metadata(metas, target_id=None, lookback_days=180):
    outdoor = [m for m in metas if not m.get("is_indoor")]
    targets = [m for m in outdoor if (str(m["activity_id"]) == str(target_id) if target_id is not None
                                     else m.get("moving_time", 0) >= 10800)]
    target = max(targets, key=lambda m: (m["start_utc"], m["activity_id"]), default=None)
    if target is None:
        return None, []
    start = utc(target["start_utc"])
    selected = [m for m in outdoor if start-timedelta(days=lookback_days) <= utc(m["start_utc"]) <= start]
    return target["activity_id"], selected


def stream_points(stream):
    channels = stream.get("streams", {})
    def values(name):
        value = channels.get(name, {})
        return value.get("data", []) if isinstance(value, dict) else (value or [])
    times = values("time")
    mapping = {"watts": "watts", "heartrate": "heartrate", "cadence": "cadence",
               "velocity_smooth": "speed_ms", "distance": "distance_m", "altitude": "ele_m",
               "moving": "moving", "temp": "temp_c"}
    arrays = {key: values(channel) for channel, key in mapping.items()}
    gps = values("latlng")
    for i, t in enumerate(times):
        point = {"t": t, **{key: array[i] if i < len(array) else None for key, array in arrays.items()}}
        if i < len(gps) and gps[i] and len(gps[i]) == 2:
            point["lat"], point["lon"] = gps[i]
        yield point


def from_backup(root, ftp=None, target_id=None):
    root = Path(root)
    metas, excluded = backup_metadata(root)
    target_id, selected = select_metadata(metas, target_id)
    stream_files = {}
    for path in (root / "lakehouse/raw/streams").glob("*.json.gz"):
        stream_files[path.name.split("_", 1)[-1].removesuffix(".json.gz")] = path
    prepared = []
    for meta in selected:
        path = stream_files.get(meta["activity_id"])
        if path is None:
            points = []
        else:
            with gzip.open(path, "rt") as handle:
                stream = json.load(handle)
            points = stream_points(stream)
        prepared.append(pacing.prepare(meta, points, ftp))
    result = pacing.review(prepared, target_id, ftp)
    result["source"] = {"kind": "local_backup_snapshot", "activities_considered": len(selected),
                        "activities_with_windows": sum(bool(r["windows"]) for r in prepared),
                        "excluded_duplicate_count": len(excluded),
                        "note": "동기화·신규 수집 없이 로컬 백업의 기록 시점으로 분석."}
    result["activity_diagnostics"] = [{"activity_id": r["meta"]["activity_id"],
                                       "date": r["meta"]["date"], "eligible_windows": len(r["windows"]),
                                       "quality": r.get("quality", {}), "rejections": r["rejections"]}
                                      for r in prepared]
    return result


def from_spark(spark, ftp=None, target_id=None):
    """Read-only adapter; no collect of a whole trackpoint table, no GPS output.

    Silver retains device_watts, so known estimated power is excluded just as
    in the backup adapter. Unknown provenance remains explicit in the evidence.
    """
    frame = spark.sql("""SELECT activity_id, name, start_date_key,
                                unix_timestamp(start_ts_utc) as start_epoch_s,
                                moving_time, elapsed_time, gear_id, is_indoor, device_watts
                         FROM tc.silver.activities
                         WHERE sport='cycling' AND NOT is_duplicate""")
    metas = []
    for row in frame.collect():
        a = row.asDict()
        metas.append({"activity_id": str(a["activity_id"]), "name": a["name"],
                      "date": str(a["start_date_key"]),
                      "start_utc": datetime.fromtimestamp(a["start_epoch_s"], timezone.utc).isoformat(),
                      "moving_time": a["moving_time"] or 0, "elapsed_time": a["elapsed_time"] or 0,
                      "gear_id": a["gear_id"], "is_indoor": a["is_indoor"],
                      "device_watts": a["device_watts"]})
    target_id, selected = select_metadata(metas, target_id)
    prepared = []
    by_id = {str(int(m["activity_id"])): m for m in selected}
    if by_id:
        # One bounded query, streaming grouped activities rather than N Spark jobs.
        identifiers = ",".join(str(int(identifier)) for identifier in by_id)
        rows = spark.sql(f"""SELECT activity_id, unix_timestamp(point_ts) as epoch_s, watts, moving,
                                    cadence, speed_ms, lat, lon, distance_m, ele_m,
                                    heartrate, temp_c
                             FROM tc.silver.trackpoints
                             WHERE activity_id IN ({identifiers}) AND NOT is_duplicate_activity
                             ORDER BY activity_id, point_ts, point_idx""").toLocalIterator()
        seen = set()
        for identifier, group in groupby(rows, key=lambda r: str(r["activity_id"])):
            meta = by_id[identifier]
            seen.add(identifier)
            start = utc(meta["start_utc"]).timestamp()
            points = ({"t": r["epoch_s"]-start, **{k: r[k] for k in r.asDict() if k not in ("epoch_s", "activity_id")}}
                      for r in group if r["epoch_s"] is not None)
            prepared.append(pacing.prepare(meta, points, ftp))
        prepared.extend(pacing.prepare(m, [], ftp) for identifier, m in by_id.items() if identifier not in seen)
    result = pacing.review(prepared, target_id, ftp)
    result["source"] = {"kind": "spark_silver", "activities_considered": len(selected),
                        "note": "Silver 중복 제외 적용. device_watts=False인 추정 파워 활동 제외."}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("spark", "backup"), default="spark")
    parser.add_argument("--root", type=Path, help="backup의 training-companion 루트")
    parser.add_argument("--target-id", help="미지정 시 가장 최근 3시간+ 야외 라이딩")
    parser.add_argument("--ftp", type=float, help="기준 출력. 미지정 시 Spark의 현재 ftp_log, backup은 미설정")
    parser.add_argument("--format", choices=("json", "markdown", "html"), default="markdown")
    parser.add_argument("--out", type=Path, help="미지정 시 stdout. 개인 결과는 공개 저장소 밖에 저장")
    args = parser.parse_args()
    if args.ftp is not None and (pacing.number(args.ftp) is None or args.ftp <= 0):
        parser.error("--ftp는 유한한 양수여야 합니다")
    if args.source == "backup":
        if not args.root or not (args.root / "lakehouse/raw/streams").is_dir():
            parser.error("--root에 lakehouse/raw/streams가 있는 백업 루트를 지정하세요")
        result = from_backup(args.root, args.ftp, args.target_id)
    else:
        from spark_session import get_spark
        spark = get_spark("review-pacing")
        try:
            ftp = args.ftp
            if ftp is None:
                rows = spark.sql("SELECT ftp_watts FROM tc.gold.ftp_log WHERE is_current").collect()
                ftp = float(rows[0]["ftp_watts"]) if rows else None
            result = from_spark(spark, ftp, args.target_id)
        finally:
            spark.stop()
    if args.format == "json":
        output = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)
    else:
        if args.format == "markdown":
            output = pacing.markdown(result)
        else:
            output = ('<!doctype html><html lang="ko"><meta charset="utf-8">'
                      '<meta name="viewport" content="width=device-width,initial-scale=1">'
                      '<title>장거리 힘 배분 복기</title><style>'
                      'body{font:15px/1.7 -apple-system,sans-serif;max-width:1040px;margin:30px auto;padding:0 20px}'
                      'table{border-collapse:collapse;min-width:720px;width:100%}'
                      'td,th{padding:10px;border-bottom:1px solid #ddd;text-align:left}'
                      'li{margin:5px 0}</style><body>' + pacing.html(result) + '</body></html>')
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

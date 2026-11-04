#!/usr/bin/env python3
"""Export canonical coaching inputs AFTER dbt_test, outside the web report root."""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from coach_contract import build_bundle


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
    temp.chmod(0o600)
    temp.replace(path)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--since", default=os.environ.get("COACH_SINCE"))
    ap.add_argument("--out", default=os.environ.get("COACH_OUTBOX"))
    ap.add_argument("--quality-checked", action="store_true", help="Only after dbt test succeeds")
    args = ap.parse_args()
    if not args.quality_checked or not args.since or not args.out:
        ap.error("--quality-checked, --since and --out are required")
    since = datetime.strptime(args.since, "%Y-%m-%d").date()
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    oldest = max(since, today - timedelta(days=14))
    history_since = oldest - timedelta(days=28)
    out = Path(args.out).resolve()
    # Explicit private location; never put the payloads under the served report directory.
    root = Path(os.environ.get("TC_ROOT", Path(__file__).resolve().parents[2])).resolve()
    if out.is_relative_to(root / "reports/web") or out.is_relative_to(Path(__file__).resolve().parents[1]):
        raise ValueError("Coach output must be outside the code repository and web root")
    from spark_session import get_spark
    spark = get_spark("export-coach")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        rows = [r.asDict(recursive=True) for r in spark.sql(f"""
          SELECT f.*, s.moving_time,s.elapsed_time,s.average_speed,s.max_speed,s.max_heartrate,s.device_name,
                 b._raw AS raw_json,p.ftp_watts,CAST(ftp.effective_date AS STRING) AS ftp_date,
                 d.power_dropout,s.device_watts
          FROM tc.gold.mart_activity_feed f
          JOIN tc.silver.activities s ON f.activity_id=s.activity_id
          JOIN tc.bronze.activities b ON f.activity_id=b.activity_id
          LEFT JOIN tc.gold.mart_ride_power p ON f.activity_id=p.activity_id
          LEFT JOIN (SELECT ftp_watts,max(effective_date) AS effective_date FROM tc.gold.ftp_log
                     WHERE is_current GROUP BY ftp_watts) ftp ON p.ftp_watts=ftp.ftp_watts
          LEFT JOIN tc.gold.mart_ride_durability d ON f.activity_id=d.activity_id
          WHERE f.sport IN ('cycling','running','swimming') AND NOT s.is_duplicate
            AND f.start_date_key >= DATE'{history_since.isoformat()}'
          ORDER BY f.start_ts_utc
        """).collect()]
        for r in rows:
            r["raw"] = json.loads(r.pop("raw_json"))
        targets = [r for r in rows if oldest <= r["start_date_key"] <= today]
        by_id = defaultdict(list)
        if targets:
            ids = ",".join(str(int(r["activity_id"])) for r in targets)
            # No GPS selected, even in the in-memory handoff representation.
            for point in spark.sql(f"""SELECT activity_id,point_idx,time_offset_s,distance_m,speed_ms,heartrate,watts
              FROM tc.bronze.streams WHERE activity_id IN ({ids}) ORDER BY activity_id,point_idx""").collect():
                by_id[point["activity_id"]].append(point.asDict())
        exported = datetime.now(timezone.utc).isoformat()
        files = []
        for row in targets:
            payload = build_bundle(row, row["raw"], rows, by_id[row["activity_id"]], run_id=args.run_id, exported_at=exported)
            name = f"{row['activity_id']}.json"
            atomic_json(out / name, payload)
            files.append(name)
        atomic_json(out / "manifest.json", {"schema_version": 1, "pipeline_run": args.run_id, "files": files})
        print(f"[coach] exported {len(files)} canonical activities (no model call or notification)")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

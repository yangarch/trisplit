#!/usr/bin/env python3
"""
Silver — 시계열 통합 (두 소스를 하나의 스키마로).

소스: tc.bronze.streams          (392 활동, per-point watts 있음, 시각은 시작 기준 offset)
      tc.bronze.trackpoints_gpx  (607 활동, watts 없음, 시각은 절대시각)
대상: tc.silver.trackpoints

같은 라이딩이 두 소스에 들어 있는데 스키마도 의미도 다르다. 통합 규칙:

  1. **스트림 우선.** watts / distance / speed / grade / moving 이 있는 쪽이다.
     GPX 는 스트림이 없는 활동에만 쓴다 — Strava 삭제(404) 건이나 파워 없는 활동.
     활동 하나가 양쪽에 다 있으면 스트림만 쓴다 (섞으면 중복 포인트가 된다).

  2. **시각 정규화.** 스트림의 time 은 활동 시작 기준 초(offset)라 절대시각이 아니다.
     silver.activities 의 start_ts_utc 와 더해 맞춘다. GPX 는 이미 절대시각이라 그대로.

  3. **수동 드롭 GPX 4 개는 제외.** 2026-09-12 무박부산의 파생 내보내기 파일이다 —
     전부 43,177 포인트로 같고 12 시간으로 잘려 있으며, 타임스탬프가 각각
     2026-09-19 / 09-19 / 09-05 / 09-12 로 어긋나 있다. 원본 기록이 아니라 가공물이라
     시계열 분석에 넣으면 안 된다. (`activities/strava/other/` 의 `*_12h.gpx` 들과 같은 관행)
     Bronze 에는 그대로 남아 있다.

  4. **중복 활동은 표시만.** 중복 판정은 silver.activities 가 이미 했다.
     여기서는 is_duplicate_activity 로 이어받기만 하고 행은 남긴다.

사용법:
  source lakehouse/env.sh
  lakehouse/.venv/bin/python lakehouse/transform/silver_trackpoints.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

LAKEHOUSE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAKEHOUSE_DIR / "scripts"))
from spark_session import CATALOG, get_spark  # noqa: E402

NS = f"{CATALOG}.silver"
TABLE = f"{NS}.trackpoints"
RUN_TS = datetime.now(tz=timezone.utc)

# 통합 스키마 — 두 소스가 맞춰야 할 컬럼 순서
COLS = [
    "activity_id", "sport", "gear_label", "is_indoor", "is_duplicate_activity",
    "point_ts", "point_idx",
    "lat", "lon", "ele_m", "distance_m", "speed_ms",
    "heartrate", "cadence", "watts", "temp_c", "grade_pct", "moving",
    "source",
]


def main() -> int:
    spark = get_spark("silver-trackpoints")
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")

    acts = spark.table(f"{NS}.activities").select(
        "activity_id", "sport", "gear_label", "is_indoor", "start_ts_utc",
        F.col("is_duplicate").alias("is_duplicate_activity"),
    )

    # ---- 스트림 (우선 소스) ----
    st = spark.table(f"{CATALOG}.bronze.streams")
    streams = (
        st.join(acts, "activity_id", "inner")
        .withColumn(
            "point_ts",
            F.expr("start_ts_utc + make_interval(0,0,0,0,0,0, time_offset_s)"),
        )
        .withColumn("source", F.lit("streams"))
        .select(*COLS)
    )

    stream_ids = st.select("activity_id").distinct()

    # ---- GPX (보조 소스) ----
    gpx = spark.table(f"{CATALOG}.bronze.trackpoints_gpx")
    gpx_only = (
        gpx.filter(F.col("source_kind") == "strava_sync")
        .join(stream_ids, "activity_id", "left_anti")   # 스트림에 있는 활동은 제외
        .join(acts, "activity_id", "inner")
        .withColumn("distance_m", F.lit(None).cast("double"))
        .withColumn("speed_ms", F.lit(None).cast("double"))
        .withColumn("watts", F.lit(None).cast("bigint"))
        .withColumn("grade_pct", F.lit(None).cast("double"))
        .withColumn("moving", F.lit(None).cast("boolean"))
        .withColumn("heartrate", F.col("heartrate").cast("bigint"))
        .withColumn("cadence", F.col("cadence").cast("bigint"))
        .withColumn("temp_c", F.col("temp_c").cast("bigint"))
        .withColumn("source", F.lit("gpx"))
        .select(*COLS)
    )

    unified = streams.unionByName(gpx_only).withColumn("_transformed_at", F.lit(RUN_TS))

    (
        unified.writeTo(TABLE)
        .using("iceberg")
        .partitionedBy(F.years("point_ts"), "sport")
        .createOrReplace()
    )

    n = spark.table(TABLE).count()
    print(f"[✓] {TABLE} — {n:,} 행")

    print("\n[*] 소스별 기여")
    spark.sql(f"""
        SELECT source, count(DISTINCT activity_id) activities, count(*) points,
               count(watts) with_watts, count(lat) with_gps
        FROM {TABLE} GROUP BY source ORDER BY points DESC
    """).show(10, False)

    print("[*] 활동 커버리지 대조")
    spark.sql(f"""
        SELECT
          (SELECT count(*) FROM {NS}.activities WHERE NOT is_duplicate) AS clean_activities,
          (SELECT count(DISTINCT activity_id) FROM {TABLE}) AS with_timeseries,
          (SELECT count(DISTINCT activity_id) FROM {TABLE} WHERE watts IS NOT NULL) AS with_power
    """).show(1, False)

    print("[*] 연도별 (중복 제외)")
    spark.sql(f"""
        SELECT year(point_ts) yr, sport, count(DISTINCT activity_id) acts, count(*) points,
               round(avg(CASE WHEN watts > 0 THEN watts END),1) avg_w
        FROM {TABLE} WHERE NOT is_duplicate_activity AND sport='cycling'
        GROUP BY 1,2 ORDER BY 1
    """).show(30, False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

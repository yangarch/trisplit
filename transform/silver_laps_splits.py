#!/usr/bin/env python3
"""
Silver — 랩 / 스플릿 정규화.

소스: tc.bronze.laps, tc.bronze.splits
대상: tc.silver.laps, tc.silver.splits

활동 단위 컨텍스트(종목·장비·인도어 여부·중복 여부)를 붙이고 단위를 분석용으로 맞춘다.
중복 활동의 랩/스플릿도 지우지 않고 is_duplicate_activity 로 표시만 한다 —
Gold 에서 필터한다.

수영 스플릿 주의:
  Strava 의 수영 스플릿은 100m 가 아니라 1,000m 단위다(splits_metric).
  100m 페이스로 바꾸려면 속도에서 역산해야 한다. 여기서 pace_s_per_100m 를 미리 계산해 둔다.
  다만 강습/자유수영 구분은 하지 않는다 — 그건 활동 메타데이터만으로는 불가능하고,
  `activities/swimming/notes-on-pace.md` 의 가이드가 필요한 Gold 단계의 일이다.

사용법:
  source lakehouse/env.sh
  lakehouse/.venv/bin/python lakehouse/transform/silver_laps_splits.py
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
RUN_TS = datetime.now(tz=timezone.utc)


def main() -> int:
    spark = get_spark("silver-laps-splits")
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")

    ctx = spark.table(f"{NS}.activities").select(
        "activity_id", "sport", "gear_label", "is_indoor", "start_date_key", "year", "month",
        F.col("is_duplicate").alias("is_duplicate_activity"),
    )

    # ---- laps ----
    laps = (
        spark.table(f"{CATALOG}.bronze.laps")
        .join(ctx, "activity_id", "inner")
        .withColumn("distance_km", F.col("distance") / 1000)
        .withColumn("moving_h", F.col("moving_time") / 3600)
        .withColumn("avg_speed_kmh", F.col("average_speed") * 3.6)
        .withColumn("max_speed_kmh", F.col("max_speed") * 3.6)
        # 0 으로 나누는 것을 막는다 — 정지 상태 랩이 실제로 존재한다
        .withColumn(
            "pace_s_per_100m",
            F.when(F.col("average_speed") > 0, 100.0 / F.col("average_speed")),
        )
        .select(
            "activity_id", "lap_id", "lap_index", "name", "sport", "gear_label",
            "is_indoor", "is_duplicate_activity", "start_date_key", "year", "month",
            "start_ts_utc", "elapsed_time", "moving_time", "moving_h",
            "distance", "distance_km", "total_elevation_gain",
            "average_speed", "avg_speed_kmh", "max_speed_kmh", "pace_s_per_100m",
            "average_cadence", "average_heartrate", "max_heartrate",
            "average_watts", "device_watts", "start_index", "end_index",
        )
        .withColumn("_transformed_at", F.lit(RUN_TS))
    )
    laps.writeTo(f"{NS}.laps").using("iceberg").partitionedBy("sport").createOrReplace()
    print(f"[✓] {NS}.laps — {spark.table(f'{NS}.laps').count():,} 행")

    # ---- splits ----
    splits = (
        spark.table(f"{CATALOG}.bronze.splits")
        .join(ctx, "activity_id", "inner")
        .withColumn("distance_km", F.col("distance") / 1000)
        .withColumn("avg_speed_kmh", F.col("average_speed") * 3.6)
        .withColumn(
            "pace_s_per_100m",
            F.when(F.col("average_speed") > 0, 100.0 / F.col("average_speed")),
        )
        .withColumn(
            "pace_s_per_km",
            F.when(F.col("average_speed") > 0, 1000.0 / F.col("average_speed")),
        )
        .select(
            "activity_id", "split", "sport", "gear_label", "is_indoor",
            "is_duplicate_activity", "start_date_key", "year", "month",
            "distance", "distance_km", "elapsed_time", "moving_time",
            "elevation_difference", "average_speed", "avg_speed_kmh",
            "pace_s_per_100m", "pace_s_per_km",
            "average_grade_adjusted_speed", "average_heartrate", "pace_zone",
        )
        .withColumn("_transformed_at", F.lit(RUN_TS))
    )
    splits.writeTo(f"{NS}.splits").using("iceberg").partitionedBy("sport").createOrReplace()
    print(f"[✓] {NS}.splits — {spark.table(f'{NS}.splits').count():,} 행")

    print("\n[*] 종목별 랩/스플릿 (중복 제외)")
    spark.sql(f"""
        WITH l AS (
            SELECT sport, count(*) laps FROM {NS}.laps
            WHERE NOT is_duplicate_activity GROUP BY sport
        ), s AS (
            SELECT sport, count(*) splits FROM {NS}.splits
            WHERE NOT is_duplicate_activity GROUP BY sport
        )
        SELECT coalesce(l.sport, s.sport) AS sport,
               coalesce(l.laps, 0) AS laps, coalesce(s.splits, 0) AS splits
        FROM l FULL OUTER JOIN s ON l.sport = s.sport
        ORDER BY laps DESC
    """).show(10, False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

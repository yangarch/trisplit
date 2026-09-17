#!/usr/bin/env python3
"""
Silver — 활동 정규화 + 중복 판정.

소스: tc.bronze.activities
대상: tc.silver.activities

하는 일:
  1. 종목 분류 — Strava 의 type 문자열을 사이클/수영/러닝/걷기로 묶는다.
     분류 기준은 기존 `reports/_scripts/overview.py` 와 동일하게 맞춘다 (6단계 패리티 검증용).
  2. 장비 라벨 — gear_id 를 사람이 읽는 이름으로.
  3. **중복 판정** — 아래 참조.
  4. 파생 컬럼 — 속도 km/h, 거리 km, 시간 h 등 분석에서 매번 나누던 것.

중복이 왜 생기나:
  헤드유닛 두 대가 같은 라이딩을 동시에 기록한다 (예: Edge 830 + Edge 1050 을 같은 자전거에).
  둘 다 Strava 로 올라가면 거리가 이중 계상된다. 실제로 14 건 / 1,005.7 km 가 여기 해당한다.

  전수 조사한 사례:
    2024-08-29  Apple Watch 6.4km    + iGPSPORT 6.4km
    2024-12-26  Zwift 27.4km         + iGPSPORT 20.7km     (인도어)
    2024-12-27  Zwift 27.1km         + iGPSPORT 20.7km     (인도어)
    2025-11-21  Edge 830 38.4km      + Edge 1040 17.6km
    2026-09-07  Edge 830 82.6km      + Edge 1050 3개(21.0+39.0+20.1)
    2026-09-12  Edge 1050 410.9km    + Edge 830 267.3km    (무박부산)

중복을 어떻게 묶나 — 세션화(sessionization):
  단순 쌍 비교로는 2026-09-07 처럼 A-B, B-C 는 겹치지만 A-C 는 안 겹치는 경우를 놓친다.
  종목별로 시작시각 순 정렬해서 "앞선 활동들의 최대 종료시각"보다 늦게 시작하면 새 그룹으로
  끊는다. 윈도우 함수 두 번이면 되고 추이적(transitive) 겹침이 자연히 한 그룹으로 묶인다.

어느 쪽을 남기나:
  그룹 안에서 **elapsed_time 이 가장 긴 것**을 primary 로 본다. 가장 오래 켜져 있던 기기가
  라이딩 전체를 담았다고 보는 것이다. 위 6 개 사례 전부에서 의도한 답이 나온다
  (2026-09-07 은 830 의 연속 82.6km 를 남기고 1050 의 3 조각을 버린다).

  **행을 지우지는 않는다.** is_duplicate / duplicate_of_activity_id 로 표시만 하고,
  Gold 에서 필터한다. 판정이 틀렸을 때 근거를 되짚을 수 있어야 한다.

사용법:
  source lakehouse/env.sh
  lakehouse/.venv/bin/python lakehouse/transform/silver_activities.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

LAKEHOUSE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAKEHOUSE_DIR / "scripts"))
from spark_session import CATALOG, get_spark  # noqa: E402

NS = f"{CATALOG}.silver"
TABLE = f"{NS}.activities"
RUN_TS = datetime.now(tz=timezone.utc)

# overview.py 와 동일하게 유지할 것 — 어긋나면 6단계 패리티 검증이 깨진다.
CYCLING_TYPES = ["Ride", "VirtualRide", "EBikeRide", "GravelRide", "MountainBikeRide"]
RUN_TYPES = ["Run", "TrailRun", "VirtualRun"]
WALK_TYPES = ["Walk", "Hike"]

# 장비 이름은 참조 데이터다. 변환 코드에 박아두면 자전거를 바꿀 때마다 코드를 고쳐야 하고,
# 레포를 공개할 때 개인 식별자와 로직이 섞인다. 설정 파일로 분리한다.
GEAR_CONFIG = LAKEHOUSE_DIR / "config/gear_names.json"
GEAR_NAMES: dict[str, str] = (
    json.loads(GEAR_CONFIG.read_text())["gear"] if GEAR_CONFIG.exists() else {}
)


def main() -> int:
    spark = get_spark("silver-activities")
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")

    b = spark.table(f"{CATALOG}.bronze.activities")

    sport = (
        F.when(F.col("type").isin(CYCLING_TYPES), "cycling")
        .when(F.col("type").isin(RUN_TYPES), "running")
        .when(F.col("type") == "Swim", "swimming")
        .when(F.col("type").isin(WALK_TYPES), "walking")
        .otherwise("other")
    )

    # 설정이 비어 있으면 create_map() 이 인자 0개로 깨지므로 NULL 컬럼으로 대체한다.
    mapped = (
        F.create_map(*[F.lit(x) for kv in GEAR_NAMES.items() for x in kv])[F.col("gear_id")]
        if GEAR_NAMES
        else F.lit(None).cast("string")
    )
    gear_label = F.coalesce(
        mapped,
        F.when(F.col("gear_id").isNull(), F.lit("(미할당)")).otherwise(
            F.concat(F.lit("기타("), F.substring("gear_id", 1, 6), F.lit("…)"))
        ),
    )

    base = (
        b.select(
            "activity_id", "name", "type", "sport_type", "gear_id", "device_name",
            "start_ts_utc", "start_date_key", "start_date_local",
            "distance", "moving_time", "elapsed_time", "total_elevation_gain",
            "elev_high", "elev_low", "average_speed", "max_speed",
            "average_cadence", "average_temp", "average_heartrate", "max_heartrate",
            "average_watts", "weighted_average_watts", "max_watts", "kilojoules",
            "device_watts", "calories", "suffer_score", "workout_type",
            "trainer", "commute", "manual", "private",
            "description", "lap_count", "split_count",
            "start_lat", "start_lng", "end_lat", "end_lng",
        )
        .withColumn("sport", sport)
        .withColumn("gear_label", gear_label)
        .withColumn("year", F.year("start_date_key"))
        .withColumn("month", F.date_format("start_date_key", "yyyy-MM"))
        .withColumn("distance_km", F.col("distance") / 1000)
        .withColumn("moving_h", F.col("moving_time") / 3600)
        .withColumn("elapsed_h", F.col("elapsed_time") / 3600)
        .withColumn("avg_speed_kmh", F.col("average_speed") * 3.6)
        .withColumn("max_speed_kmh", F.col("max_speed") * 3.6)
        .withColumn("elev_m", F.coalesce(F.col("total_elevation_gain"), F.lit(0.0)))
        # VirtualRide 는 trainer 플래그가 없어도 인도어다 (overview.py 와 동일 규칙)
        .withColumn(
            "is_indoor",
            F.coalesce(F.col("trainer"), F.lit(False)) | (F.col("type") == "VirtualRide"),
        )
        .withColumn("has_gps", F.col("start_lat").isNotNull())
        .withColumn("end_ts_utc", F.expr("start_ts_utc + make_interval(0,0,0,0,0,0, elapsed_time)"))
    )

    # ---- 중복 판정: 종목별 세션화 ----
    # 거리 0 인 활동(요가 등)은 겹침 판정 대상이 아니다.
    w_order = "PARTITION BY sport ORDER BY start_ts_utc"
    flagged = (
        base.withColumn("_dedup_scope", F.col("distance") > 0)
        .withColumn(
            "_prev_max_end",
            F.expr(
                f"CASE WHEN _dedup_scope THEN max(CASE WHEN _dedup_scope THEN end_ts_utc END) "
                f"OVER ({w_order} ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) END"
            ),
        )
        .withColumn(
            "_is_new_group",
            F.when(~F.col("_dedup_scope"), F.lit(1))
            .when(F.col("_prev_max_end").isNull(), F.lit(1))
            .when(F.col("start_ts_utc") >= F.col("_prev_max_end"), F.lit(1))
            .otherwise(F.lit(0)),
        )
        .withColumn(
            "overlap_group",
            F.expr(f"sum(_is_new_group) OVER ({w_order} ROWS UNBOUNDED PRECEDING)"),
        )
    )

    # 그룹 안에서 elapsed_time 이 가장 긴 것이 primary. 동률이면 activity_id 가 작은 쪽.
    grp = "PARTITION BY sport, overlap_group"
    resolved = (
        flagged.withColumn("_group_size", F.expr(f"count(*) OVER ({grp})"))
        .withColumn(
            "_rank",
            F.expr(f"row_number() OVER ({grp} ORDER BY elapsed_time DESC, activity_id ASC)"),
        )
        .withColumn(
            "_primary_id",
            F.expr(f"first_value(activity_id) OVER ({grp} ORDER BY elapsed_time DESC, activity_id ASC)"),
        )
        .withColumn("is_duplicate", (F.col("_group_size") > 1) & (F.col("_rank") > 1))
        .withColumn(
            "duplicate_of_activity_id",
            F.when(F.col("is_duplicate"), F.col("_primary_id")),
        )
        .drop("_dedup_scope", "_prev_max_end", "_is_new_group", "_group_size", "_rank", "_primary_id")
        .withColumn("_transformed_at", F.lit(RUN_TS))
    )

    resolved.writeTo(TABLE).using("iceberg").partitionedBy("sport").createOrReplace()

    n = spark.table(TABLE).count()
    n_dup = spark.table(TABLE).filter("is_duplicate").count()
    print(f"[✓] {TABLE} — {n:,} 행 (중복 표시 {n_dup} 건)")

    print("\n[*] 중복으로 판정된 활동 (제외 대상)")
    spark.sql(f"""
        SELECT d.start_date_key AS day, d.activity_id AS dropped, d.device_name AS dropped_dev,
               round(d.distance_km,1) AS dropped_km,
               d.duplicate_of_activity_id AS kept, p.device_name AS kept_dev,
               round(p.distance_km,1) AS kept_km
        FROM {TABLE} d JOIN {TABLE} p ON d.duplicate_of_activity_id = p.activity_id
        WHERE d.is_duplicate ORDER BY d.start_date_key
    """).show(30, False)

    print("[*] 중복 제외 전후 종목별 집계")
    spark.sql(f"""
        SELECT sport,
               count(*) AS all_n, round(sum(distance_km),1) AS all_km,
               count_if(NOT is_duplicate) AS clean_n,
               round(sum(CASE WHEN NOT is_duplicate THEN distance_km END),1) AS clean_km,
               round(sum(CASE WHEN is_duplicate THEN distance_km END),1) AS removed_km
        FROM {TABLE} GROUP BY sport ORDER BY all_km DESC
    """).show(10, False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

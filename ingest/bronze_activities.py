#!/usr/bin/env python3
"""
Bronze 적재 — 활동 요약 / 랩 / 스플릿.

소스: activities/strava/raw-gpx/*.json  (기존 fetch.py 가 떨어뜨리는 원본, 읽기만 한다)
대상: tc.bronze.activities / tc.bronze.laps / tc.bronze.splits

설계 판단:
  * **명시적 스키마.** 838건 JSON 의 필드 커버리지가 고르지 않다 —
    `weighted_average_watts` 46%, `workout_type` 44%, `suffer_score` 21%.
    스키마 추론에 맡기면 파일 집합이 바뀔 때마다 타입이 흔들리므로 직접 고정한다.
  * **`_raw` 보존.** Bronze 는 원본 충실성이 목적이라, 스키마에 없는 필드도
    나중에 꺼낼 수 있도록 원본 JSON 문자열을 통째로 들고 간다.
  * **파티셔닝 없음.** activities 는 838 행이다. 파티셔닝하면 파일만 잘게 쪼개져 손해다.
    (트랙포인트 182만 행은 얘기가 다르다 — bronze_trackpoints.py 참조)
  * **MERGE INTO 로 증분.** 매일 신규 활동만 파일로 추가되므로 전체 재적재는 낭비다.
    activity_id 기준 upsert — Iceberg 의 row-level 연산을 그대로 쓴다.

사용법:
  source lakehouse/env.sh
  lakehouse/.venv/bin/python lakehouse/ingest/bronze_activities.py
  lakehouse/.venv/bin/python lakehouse/ingest/bronze_activities.py --full-refresh
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

LAKEHOUSE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAKEHOUSE_DIR / "scripts"))
from ingest_audit import record as audit_record  # noqa: E402
from spark_session import CATALOG, get_spark  # noqa: E402

RAW_JSON_GLOB = str(LAKEHOUSE_DIR.parent / "activities/strava/raw-gpx/*.json")
NS = f"{CATALOG}.bronze"

# 적재 1회 = 1 배치. 배치 전체가 같은 값을 갖는 게 의미상으로도 맞고,
# current_timestamp() 는 비결정적이라 MERGE 소스에 쓸 수 없다 (아래 upsert 주석 참조).
RUN_TS = datetime.now(tz=timezone.utc)

LAP_STRUCT = StructType([
    StructField("id", LongType()),
    StructField("lap_index", LongType()),
    StructField("name", StringType()),
    StructField("split", LongType()),
    StructField("start_date", StringType()),
    StructField("start_date_local", StringType()),
    StructField("elapsed_time", LongType()),
    StructField("moving_time", LongType()),
    StructField("distance", DoubleType()),
    StructField("start_index", LongType()),
    StructField("end_index", LongType()),
    StructField("total_elevation_gain", DoubleType()),
    StructField("average_speed", DoubleType()),
    StructField("max_speed", DoubleType()),
    StructField("average_cadence", DoubleType()),
    StructField("average_heartrate", DoubleType()),
    StructField("max_heartrate", DoubleType()),
    StructField("average_watts", DoubleType()),
    StructField("device_watts", BooleanType()),
    StructField("pace_zone", LongType()),
])

SPLIT_STRUCT = StructType([
    StructField("split", LongType()),
    StructField("distance", DoubleType()),
    StructField("elapsed_time", LongType()),
    StructField("moving_time", LongType()),
    StructField("elevation_difference", DoubleType()),
    StructField("average_speed", DoubleType()),
    StructField("average_grade_adjusted_speed", DoubleType()),
    StructField("average_heartrate", DoubleType()),
    StructField("pace_zone", LongType()),
])

ACTIVITY_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("type", StringType()),
    StructField("sport_type", StringType()),
    StructField("start_date", StringType()),
    StructField("start_date_local", StringType()),
    StructField("timezone", StringType()),
    StructField("utc_offset", DoubleType()),
    StructField("distance", DoubleType()),
    StructField("moving_time", LongType()),
    StructField("elapsed_time", LongType()),
    StructField("total_elevation_gain", DoubleType()),
    StructField("elev_high", DoubleType()),
    StructField("elev_low", DoubleType()),
    StructField("average_speed", DoubleType()),
    StructField("max_speed", DoubleType()),
    StructField("average_cadence", DoubleType()),
    StructField("average_temp", DoubleType()),
    StructField("has_heartrate", BooleanType()),
    StructField("average_heartrate", DoubleType()),
    StructField("max_heartrate", DoubleType()),
    StructField("average_watts", DoubleType()),
    StructField("weighted_average_watts", DoubleType()),
    StructField("max_watts", DoubleType()),
    StructField("kilojoules", DoubleType()),
    StructField("device_watts", BooleanType()),
    StructField("calories", DoubleType()),
    StructField("suffer_score", DoubleType()),
    StructField("perceived_exertion", DoubleType()),
    StructField("workout_type", LongType()),
    StructField("gear_id", StringType()),
    StructField("device_name", StringType()),
    StructField("trainer", BooleanType()),
    StructField("commute", BooleanType()),
    StructField("manual", BooleanType()),
    StructField("private", BooleanType()),
    StructField("flagged", BooleanType()),
    StructField("visibility", StringType()),
    StructField("description", StringType()),
    StructField("achievement_count", LongType()),
    StructField("kudos_count", LongType()),
    StructField("athlete_count", LongType()),
    StructField("pr_count", LongType()),
    StructField("upload_id", LongType()),
    StructField("external_id", StringType()),
    StructField("start_latlng", ArrayType(DoubleType())),
    StructField("end_latlng", ArrayType(DoubleType())),
    StructField("laps", ArrayType(LAP_STRUCT)),
    StructField("splits_metric", ArrayType(SPLIT_STRUCT)),
])

# 스칼라 컬럼만 (배열/중첩은 별도 테이블로 분리)
SCALAR_COLS = [
    f.name for f in ACTIVITY_SCHEMA.fields
    if f.name not in ("laps", "splits_metric", "start_latlng", "end_latlng")
]


def read_source(spark: SparkSession) -> DataFrame:
    """JSON 파일을 통째로 읽어 명시적 스키마로 파싱한다. 원본 문자열도 함께 보존."""
    # wholetext 는 반드시 text() 의 키워드 인자로 넘긴다.
    # .option("wholetext", ...).text(...) 형태는 text() 의 기본 인자(None)가
    # 앞서 설정한 옵션을 덮어써서 조용히 줄 단위로 읽힌다 (838 파일 → 171만 행).
    raw = (
        spark.read.text(RAW_JSON_GLOB, wholetext=True)
        .withColumn("_source_file", F.input_file_name())
        .withColumnRenamed("value", "_raw")
    )
    return raw.withColumn("p", F.from_json("_raw", ACTIVITY_SCHEMA)).filter(
        F.col("p.id").isNotNull()
    )


def build_activities(src: DataFrame) -> DataFrame:
    return (
        src.select(
            *[F.col(f"p.{c}").alias(c) for c in SCALAR_COLS],
            # 실내 활동(231건)은 latlng 이 빈 배열이다. Spark 4 는 ANSI 모드가 기본이라
            # [0] 접근이 ArrayIndexOutOfBounds 로 터진다 → 범위 밖이면 NULL 을 주는 get() 사용.
            F.get(F.col("p.start_latlng"), F.lit(0)).alias("start_lat"),
            F.get(F.col("p.start_latlng"), F.lit(1)).alias("start_lng"),
            F.get(F.col("p.end_latlng"), F.lit(0)).alias("end_lat"),
            F.get(F.col("p.end_latlng"), F.lit(1)).alias("end_lng"),
            F.size(F.coalesce(F.col("p.laps"), F.array())).alias("lap_count"),
            F.size(F.coalesce(F.col("p.splits_metric"), F.array())).alias("split_count"),
            F.col("_raw"),
            F.col("_source_file"),
        )
        .withColumn("start_ts_utc", F.to_timestamp("start_date"))
        .withColumn("start_date_key", F.to_date(F.substring("start_date_local", 1, 10)))
        .withColumn("_ingested_at", F.lit(RUN_TS))
        .withColumnRenamed("id", "activity_id")
    )


def build_laps(src: DataFrame) -> DataFrame:
    e = src.select(
        F.col("p.id").alias("activity_id"),
        F.explode("p.laps").alias("l"),
        F.col("_source_file"),
    )
    return (
        e.select(
            "activity_id",
            F.col("l.id").alias("lap_id"),
            *[
                F.col(f"l.{f.name}").alias(f.name)
                for f in LAP_STRUCT.fields
                if f.name != "id"
            ],
            F.col("_source_file"),
        )
        .withColumn("start_ts_utc", F.to_timestamp("start_date"))
        .withColumn("_ingested_at", F.lit(RUN_TS))
    )


def build_splits(src: DataFrame) -> DataFrame:
    e = src.select(
        F.col("p.id").alias("activity_id"),
        F.explode("p.splits_metric").alias("s"),
        F.col("_source_file"),
    )
    return e.select(
        "activity_id",
        *[F.col(f"s.{f.name}").alias(f.name) for f in SPLIT_STRUCT.fields],
        F.col("_source_file"),
    ).withColumn("_ingested_at", F.lit(RUN_TS))


def upsert(spark: SparkSession, df: DataFrame, table: str, keys: list[str], full: bool) -> None:
    """첫 실행이면 생성, 아니면 key 기준 MERGE INTO."""
    exists = spark.catalog.tableExists(table)
    if full or not exists:
        df.writeTo(table).using("iceberg").createOrReplace()
        print(f"  [{table}] {'전체 재적재' if exists else '신규 생성'} — {df.count():,} 행")
        return

    # MERGE 소스는 결정적(deterministic)이어야 한다.
    # 소스 플랜에 input_file_name() / current_timestamp() 가 남아 있으면
    # Spark 가 INVALID_NON_DETERMINISTIC_EXPRESSIONS 로 거부한다 —
    # full outer join 후 재작성 과정에서 소스를 여러 번 스캔하기 때문이다.
    # 그래서 스테이징을 실테이블로 한 번 실체화한 뒤 거기서 MERGE 한다.
    stg = f"{NS}._stg_{table.split('.')[-1]}"
    # 앞선 실행이 MERGE 전에 죽으면 스테이징 테이블이 남는다.
    # createOrReplace 는 **기존 테이블의 기록된 location 을 그대로 쓰기 때문에**,
    # 웨어하우스 경로가 바뀐 환경(예: 컨테이너)에서 남은 것을 만나면
    # 옛 경로에 쓰려다 Mkdirs 로 죽는다. 만들기 전에 지운다.
    spark.sql(f"DROP TABLE IF EXISTS {stg} PURGE")
    df.writeTo(stg).using("iceberg").create()

    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""
        MERGE INTO {table} t
        USING {stg} s
        ON {cond}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    n_src = spark.table(stg).count()
    spark.sql(f"DROP TABLE IF EXISTS {stg} PURGE")
    print(f"  [{table}] MERGE 완료 — 소스 {n_src:,} 행 / 현재 {spark.table(table).count():,} 행")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--full-refresh", action="store_true", help="MERGE 대신 전체 재적재")
    args = ap.parse_args()

    spark = get_spark("bronze-activities")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")

    src = read_source(spark).cache()
    n_files = src.select("_source_file").distinct().count()
    print(f"[*] 소스 JSON {n_files:,} 파일 파싱 완료")

    upsert(spark, build_activities(src), f"{NS}.activities", ["activity_id"], args.full_refresh)
    upsert(spark, build_laps(src), f"{NS}.laps", ["activity_id", "lap_id"], args.full_refresh)
    upsert(spark, build_splits(src), f"{NS}.splits", ["activity_id", "split"], args.full_refresh)

    print("\n[*] 적재 결과")
    for t in ("activities", "laps", "splits"):
        print(f"  {NS}.{t:12s} {spark.table(f'{NS}.{t}').count():>9,} 행")

    # 수집량 대비 적재량 감사.
    # 활동은 "소스 JSON 파일 1개 = 행 1개" 라 파일 수가 그대로 기대 건수다.
    # 랩/스플릿은 활동 안에 중첩돼 있어 소스 JSON 에서 직접 센 합계를 기대값으로 쓴다.
    nested = src.select(
        F.sum(F.size(F.coalesce(F.col("p.laps"), F.array()))).alias("laps"),
        F.sum(F.size(F.coalesce(F.col("p.splits_metric"), F.array()))).alias("splits"),
    ).first()
    print()
    for tbl, expected in (
        ("activities", n_files),
        ("laps", nested["laps"]),
        ("splits", nested["splits"]),
    ):
        audit_record(
            spark, layer="bronze", table_name=f"{NS}.{tbl}",
            source_kind="strava_json", source_count=expected,
            loaded_count=spark.table(f"{NS}.{tbl}").count(),
            run_ts=RUN_TS,
            note="raw-gpx/*.json",
        )

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

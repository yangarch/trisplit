#!/usr/bin/env python3
"""
Bronze 적재 — Strava 원본 스트림 (per-point 파워 포함).

소스: lakehouse/raw/streams/*.json.gz   (fetch_streams.py 가 받아둔 원본 응답)
대상: tc.bronze.streams

이 테이블이 재구축의 실질적 명분이다.
기존 파이프라인은 스트림을 GPX 로 직렬화하면서 **watts 를 버렸다** — GPX 표준에 파워 확장이
없다는 이유였고(`gpx_writer.py:53`), JSON 쪽에도 스트림은 저장되지 않았다.
그래서 파워 분석이 활동 평균/NP 단위에 갇혀 있었다. 원본 스트림을 그대로 적재하면 풀린다.

설계 판단:
  * **arrays_zip + posexplode.** 스트림은 "키별 배열"로 오는데(컬럼 지향), 분석은 행 지향이
    편하다. posexplode 로 전치하면서 원본 인덱스를 point_idx 로 남긴다.
  * **없는 스트림은 빈 배열로.** 활동마다 있는 스트림이 다르다(실내엔 latlng 없음).
    arrays_zip 은 짧은 배열을 NULL 로 채우므로, 없는 것을 빈 배열로 만들면 자연히 NULL 이 된다.
  * **시각은 offset 그대로.** Strava time 스트림은 활동 시작 기준 초(offset)다.
    절대 시각으로 바꾸려면 activities 와 조인해야 하는데, 그건 Bronze 의 일이 아니라 Silver 의 일이다.

사용법:
  source lakehouse/env.sh
  lakehouse/.venv/bin/python lakehouse/ingest/bronze_streams.py
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

LAKEHOUSE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAKEHOUSE_DIR / "scripts"))
from incremental import delete_rows, plan_incremental, to_uri  # noqa: E402
from ingest_audit import record as audit_record  # noqa: E402
from spark_session import CATALOG, get_spark  # noqa: E402

STREAMS_DIR = LAKEHOUSE_DIR / "raw/streams"
NS = f"{CATALOG}.bronze"
TABLE = f"{NS}.streams"
RUN_TS = datetime.now(tz=timezone.utc)


def _s(elem):
    """Strava 스트림 한 종류의 스키마: {"data": [...], "series_type": ..., ...}"""
    return StructType([StructField("data", ArrayType(elem))])


SCHEMA = StructType([
    StructField("activity_id", LongType()),
    StructField("date", StringType()),
    StructField("type", StringType()),
    StructField("fetched_at", StringType()),
    StructField("streams", StructType([
        StructField("time", _s(LongType())),
        StructField("latlng", _s(ArrayType(DoubleType()))),
        StructField("distance", _s(DoubleType())),
        StructField("altitude", _s(DoubleType())),
        StructField("velocity_smooth", _s(DoubleType())),
        StructField("heartrate", _s(LongType())),
        StructField("cadence", _s(LongType())),
        StructField("watts", _s(LongType())),
        StructField("temp", _s(LongType())),
        StructField("grade_smooth", _s(DoubleType())),
        StructField("moving", _s(BooleanType())),
    ])),
])

# (스트림 이름, Spark 타입) — 없는 스트림은 빈 배열로 대체해 arrays_zip 이 NULL 로 채우게 한다.
STREAM_COLS = [
    ("time", "bigint"),
    ("latlng", "array<double>"),
    ("distance", "double"),
    ("altitude", "double"),
    ("velocity_smooth", "double"),
    ("heartrate", "bigint"),
    ("cadence", "bigint"),
    ("watts", "bigint"),
    ("temp", "bigint"),
    ("grade_smooth", "double"),
    ("moving", "boolean"),
]


def count_source_points(files: list[Path]) -> int:
    """소스에서 기대 포인트 수를 독립적으로 센다.

    trackpoints 는 `<trkpt` 바이트 스캔으로 되지만 스트림은 JSON 이라 파싱이 필요하다.
    다만 Spark 를 거치지 않고 드라이버에서 직접 세므로 적재 로직과 독립이다 —
    392파일 20.7MB(gz) 에 맥북 1초 / 서버 추정 4~8초.

    arrays_zip 이 가장 긴 배열에 맞춰 패딩하므로 max 를 쓴다 (적재 로직과 동일 규칙).
    """
    total = 0
    for f in files:
        try:
            with gzip.open(f, "rt", encoding="utf-8") as fh:
                d = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        streams = d.get("streams") or {}
        total += max(
            (len((v or {}).get("data") or []) for v in streams.values()),
            default=0,
        )
    return total


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--full-refresh",
        action="store_true",
        help="증분 대신 전체 재적재 (스키마 변경 후 1회 필요)",
    )
    args = ap.parse_args()

    spark = get_spark("bronze-streams")
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")

    all_files = sorted(STREAMS_DIR.glob("*.json.gz"))
    plan = plan_incremental(spark, TABLE, all_files, full_refresh=args.full_refresh)
    print(f"[*] 소스 스트림 {len(all_files):,} 개 — {plan.describe()}")

    if plan.nothing_to_do:
        print("[*] 변경 없음 — 파싱 건너뜀")
        n = spark.table(TABLE).count()
        audit_record(
            spark, layer="bronze", table_name=TABLE,
            source_kind="strava_streams", source_count=count_source_points(all_files),
            loaded_count=n, run_ts=RUN_TS, note=f"{len(all_files)} files (no-op)",
        )
        print(f"[✓] {TABLE} — {n:,} 행 (변동 없음)")
        spark.stop()
        return 0

    if plan.to_delete:
        delete_rows(spark, TABLE, plan.to_delete)
        print(f"[*] 기존 행 삭제: 소스 {len(plan.to_delete)} 개분")

    # .json.gz 는 Spark 가 알아서 풀어 읽는다. 파일당 1줄 JSON 이라 multiLine 불필요.
    # 이번에 적재할 파일 경로만 명시적으로 넘긴다.
    paths = [str(x) for x in plan.to_parse]
    df = (
        spark.read.schema(SCHEMA).json(paths)
        .withColumn("_source_file", F.input_file_name())
        .filter(F.col("activity_id").isNotNull())
    )

    mtime_df = spark.createDataFrame(
        [(to_uri(x), float(x.stat().st_mtime)) for x in plan.to_parse],
        "_source_file string, _source_mtime double",
    )

    n_files = len(plan.to_parse)
    print(f"[*] 파싱 대상 {n_files:,} 개")

    zipped = F.arrays_zip(
        *[
            F.coalesce(
                F.col(f"streams.{name}.data"),
                F.expr(f"cast(array() as array<{typ}>)"),
            ).alias(name)
            for name, typ in STREAM_COLS
        ]
    )

    points = (
        df.select(
            "activity_id",
            F.to_date("date").alias("activity_date"),
            F.col("type").alias("activity_type"),
            F.col("_source_file"),
            F.posexplode(zipped).alias("point_idx", "p"),
        )
        .select(
            "activity_id",
            "activity_date",
            "activity_type",
            "_source_file",
            "point_idx",
            F.col("p.time").alias("time_offset_s"),
            # latlng 은 [lat, lng] 2원소 배열. 실내 활동엔 아예 없어 NULL 이 된다.
            F.get(F.col("p.latlng"), F.lit(0)).alias("lat"),
            F.get(F.col("p.latlng"), F.lit(1)).alias("lon"),
            F.col("p.distance").alias("distance_m"),
            F.col("p.altitude").alias("ele_m"),
            F.col("p.velocity_smooth").alias("speed_ms"),
            F.col("p.heartrate").alias("heartrate"),
            F.col("p.cadence").alias("cadence"),
            F.col("p.watts").alias("watts"),
            F.col("p.temp").alias("temp_c"),
            F.col("p.grade_smooth").alias("grade_pct"),
            F.col("p.moving").alias("moving"),
        )
        .withColumn("_ingested_at", F.lit(RUN_TS))
        .join(mtime_df, "_source_file", "left")
    )

    if plan.table_exists and not args.full_refresh:
        points.writeTo(TABLE).append()
    else:
        (
            points.writeTo(TABLE)
            .using("iceberg")
            .partitionedBy(F.years("activity_date"))
            .createOrReplace()
        )

    n = spark.table(TABLE).count()
    print(f"[✓] {TABLE} — {n:,} 행 적재")

    # 수집량은 **소스 전체**를 드라이버에서 독립적으로 센다 (이번에 파싱한 것만이 아니라).
    print()
    audit_record(
        spark, layer="bronze", table_name=TABLE,
        source_kind="strava_streams", source_count=count_source_points(all_files),
        loaded_count=n, run_ts=RUN_TS,
        note=f"{len(all_files)} files, parsed {n_files}",
    )

    print("\n[*] 스트림 커버리지 (이게 핵심 — watts 가 처음으로 per-point 로 남는다)")
    spark.sql(f"""
        SELECT count(*) points,
               count(watts)     AS with_watts,
               count(heartrate) AS with_hr,
               count(cadence)   AS with_cadence,
               count(lat)       AS with_gps,
               count(temp_c)    AS with_temp
        FROM {TABLE}
    """).show(1, False)

    print("[*] 연도별")
    spark.sql(f"""
        SELECT year(activity_date) yr, count(DISTINCT activity_id) activities,
               count(*) points, count(watts) with_watts,
               round(avg(CASE WHEN watts > 0 THEN watts END), 1) avg_watts_moving
        FROM {TABLE} GROUP BY 1 ORDER BY 1
    """).show(30, False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

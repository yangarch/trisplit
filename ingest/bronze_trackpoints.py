#!/usr/bin/env python3
"""
Bronze 적재 — GPX 트랙포인트 (시계열).

소스: activities/strava/raw-gpx/*.gpx  (611 파일 / 약 182만 포인트)
대상: tc.bronze.trackpoints_gpx

여기가 이 프로젝트에서 유일하게 "규모가 있는" 레이어다.
활동 요약은 838 행이지만 트랙포인트는 182만 행이고, 그게 611 개의 작은 XML 에 흩어져 있다.
전형적인 small-file 문제 — 컬럼 하나 집계하려 해도 XML 611 개를 전부 파싱해야 했다.

설계 판단:
  * **years(point_ts) 로 파티셔닝.** 2018~2026, 연 단위면 9 파티션에 파티션당 약 20 만 행.
    월 단위(95 파티션)는 파티션당 2 만 행이라 이 규모에서는 잘게 쪼개는 쪽 손해가 크다.
  * **activity_id 는 파일명에서 뽑는다.** GPX 본문에는 활동 ID 가 없다
    (`gpx_writer.py` 가 name/type/time 만 메타데이터로 넣는다).
  * **watts 없음.** GPX 표준에 파워 확장이 없어 `gpx_writer.py:53` 이 버렸다.
    파워는 bronze_streams.py 가 적재하는 원본 스트림 쪽에 있다.

사용법:
  source lakehouse/env.sh
  lakehouse/.venv/bin/python lakehouse/ingest/bronze_trackpoints.py
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType,
)

LAKEHOUSE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAKEHOUSE_DIR / "scripts"))
from ingest_audit import record as audit_record  # noqa: E402
from spark_session import CATALOG, get_spark  # noqa: E402

GPX_GLOB = str(LAKEHOUSE_DIR.parent / "activities/strava/raw-gpx/*.gpx")
NS = f"{CATALOG}.bronze"
TABLE = f"{NS}.trackpoints_gpx"
RUN_TS = datetime.now(tz=timezone.utc)

GPX_NS = "{http://www.topografix.com/GPX/1/1}"
TPX_NS = "{http://www.garmin.com/xmlschemas/TrackPointExtension/v1}"

POINT_STRUCT = StructType([
    StructField("idx", IntegerType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ele_m", DoubleType()),
    StructField("time_iso", StringType()),
    StructField("heartrate", IntegerType()),
    StructField("cadence", IntegerType()),
    StructField("temp_c", IntegerType()),
])


def _parse_gpx(xml: str) -> list[tuple]:
    """GPX 문자열 → 트랙포인트 리스트. 깨진 파일은 빈 리스트 (적재는 계속)."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return []

    out = []
    for i, pt in enumerate(root.iter(f"{GPX_NS}trkpt")):
        lat = pt.get("lat")
        lon = pt.get("lon")
        if lat is None or lon is None:
            continue

        ele = pt.findtext(f"{GPX_NS}ele")
        tm = pt.findtext(f"{GPX_NS}time")

        hr = cad = tmp = None
        tpx = pt.find(f"{GPX_NS}extensions/{TPX_NS}TrackPointExtension")
        if tpx is not None:
            hr = tpx.findtext(f"{TPX_NS}hr")
            cad = tpx.findtext(f"{TPX_NS}cad")
            tmp = tpx.findtext(f"{TPX_NS}atemp")

        out.append((
            i,
            float(lat),
            float(lon),
            float(ele) if ele else None,
            tm,
            int(hr) if hr else None,
            int(cad) if cad else None,
            int(tmp) if tmp else None,
        ))
    return out


parse_gpx_udf = F.udf(_parse_gpx, ArrayType(POINT_STRUCT))

# fetch.py 가 떨어뜨리는 파일명은 <YYYY-MM-DD>_<activity_id>.gpx 다.
# 다만 raw-gpx 는 "자동 수집 또는 수동 드롭" 디렉토리라(CLAUDE.md), 손으로 넣은 파일도 있다 —
# 2026-09-12 무박부산 411km 의 변형본 4개가 그렇고, 파일명에 activity_id 가 없다.
# Bronze 는 원본 충실성이 목적이므로 버리지 않고 source_kind 로 구분해 적재한다.
DATE_RE = r"(\d{4}-\d{2}-\d{2})_"
ID_RE = r"_(\d+)\.gpx$"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit-files", type=int, help="앞에서 N개 파일만 (개발용)")
    args = ap.parse_args()

    spark = get_spark("bronze-trackpoints")
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")

    files = (
        spark.read.text(GPX_GLOB, wholetext=True)
        .withColumn("_source_file", F.input_file_name())
        .withColumnRenamed("value", "xml")
    )
    if args.limit_files:
        files = files.limit(args.limit_files)

    n_files = files.count()
    print(f"[*] GPX 파일 {n_files:,} 개 읽음")

    # wholetext 로 읽으면 파티션이 몇 개 안 되고, 파티션 하나가 1MB 짜리 XML 수십 개 +
    # 파싱된 포인트 배열(활동당 최대 3만 건)을 동시에 들고 있게 된다 → OOM.
    # 파일 수에 맞춰 잘게 쪼개 태스크당 상주 메모리를 낮춘다.
    files = files.repartition(min(n_files, 96))

    # 매칭 실패 시 regexp_extract 는 빈 문자열을 준다. Spark 4 는 ANSI 가 기본이라
    # ''.cast("long") 이 CAST_INVALID_INPUT 으로 잡을 터뜨린다 → try_cast 로 NULL 처리.
    parsed = (
        files.withColumn("_id_str", F.regexp_extract("_source_file", ID_RE, 1))
        .withColumn("activity_id", F.expr("try_cast(_id_str AS BIGINT)"))
        # 정규식은 반드시 Column API 로 넘긴다.
        # F.expr("...regexp_extract(x, '(\\d{4})', 1)...") 형태는 Spark SQL 문자열 리터럴이
        # 백슬래시를 이스케이프로 먹어 \d 가 d 로 바뀐다 → 매칭 0건, try_to_date 가 조용히 NULL.
        .withColumn("activity_date", F.try_to_date(F.regexp_extract("_source_file", DATE_RE, 1)))
        .withColumn(
            "source_kind",
            F.when(F.col("activity_id").isNull(), F.lit("manual_drop")).otherwise(F.lit("strava_sync")),
        )
        .withColumn("pts", parse_gpx_udf("xml"))
        .drop("xml", "_id_str")
    )

    points = (
        parsed.select(
            "activity_id",
            "activity_date",
            "source_kind",
            F.explode("pts").alias("p"),
            "_source_file",
        )
        .select(
            "activity_id",
            "activity_date",
            "source_kind",
            F.col("p.idx").alias("point_idx"),
            F.col("p.lat").alias("lat"),
            F.col("p.lon").alias("lon"),
            F.col("p.ele_m").alias("ele_m"),
            F.to_timestamp("p.time_iso").alias("point_ts"),
            F.col("p.heartrate").alias("heartrate"),
            F.col("p.cadence").alias("cadence"),
            F.col("p.temp_c").alias("temp_c"),
            F.col("_source_file"),
        )
        .withColumn("_ingested_at", F.lit(RUN_TS))
    )

    # 트랙포인트는 append-only 시계열이라 MERGE 가 필요 없다 — 전체 재작성이 더 싸고 단순하다.
    (
        points.writeTo(TABLE)
        .using("iceberg")
        .partitionedBy(F.years("point_ts"))
        .createOrReplace()
    )

    n = spark.table(TABLE).count()
    print(f"[✓] {TABLE} — {n:,} 행 적재")

    # 파싱 실패가 조용히 전량 NULL 로 넘어가는 것을 막는다.
    # (실제로 정규식 이스케이프 문제로 activity_date 가 182만 행 전부 NULL 이 된 적이 있다)
    bad = spark.sql(f"""
        SELECT sum(CASE WHEN activity_date IS NULL THEN 1 ELSE 0 END) null_date,
               sum(CASE WHEN point_ts IS NULL THEN 1 ELSE 0 END) null_ts,
               sum(CASE WHEN lat IS NULL OR lon IS NULL THEN 1 ELSE 0 END) null_latlon
        FROM {TABLE}
    """).first()
    print(f"[*] 결측 점검 — activity_date {bad['null_date']:,} / point_ts {bad['null_ts']:,} / latlon {bad['null_latlon']:,}")
    if bad["null_date"] == n:
        raise SystemExit("[!] activity_date 가 전량 NULL — 파일명 파싱 실패. 적재 중단.")

    print("\n[*] 소스 구분")
    spark.sql(f"""
        SELECT source_kind, count(*) points, count(DISTINCT _source_file) files
        FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC
    """).show(10, False)

    print("[*] 연도별 분포 / 파티션")
    spark.sql(f"""
        SELECT year(point_ts) yr, count(*) points,
               count(DISTINCT activity_id) activities,
               count(heartrate) with_hr, count(cadence) with_cad
        FROM {TABLE} GROUP BY 1 ORDER BY 1
    """).show(30, False)

    files_meta = spark.sql(f"SELECT count(*) n, round(sum(file_size_in_bytes)/1024/1024,1) mb FROM {TABLE}.files").first()
    print(f"[*] 데이터 파일 {files_meta['n']}개 / {files_meta['mb']} MB (원본 XML {n_files}개 → Parquet)")

    # 수집량 = 소스 GPX 안의 <trkpt> 총 개수. 파싱 후 행 수와 일치해야 한다.
    # (파일 수가 아니라 포인트 수로 세는 게 핵심 — 파일 하나가 통째로 파싱 실패해도 잡힌다)
    expected_pts = parsed.select(F.sum(F.size("pts"))).first()[0] or 0
    print()
    audit_record(
        spark, layer="bronze", table_name=TABLE,
        source_kind="gpx_trkpt", source_count=expected_pts,
        loaded_count=n, run_ts=RUN_TS, note=f"{n_files} files",
    )

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

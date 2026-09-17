"""
적재 감사 기록 — "수집량 대비 적재량" 비교의 근거 데이터.

왜 필요한가:
  실무에서 하던 품질 검증은 "소스에서 몇 건 읽었는지" 와 "테이블에 몇 건 들어갔는지" 를
  비교해 어긋나면 알람을 쏘는 방식이었다. 그런데 **수집량은 파이프라인이 끝나면 사라진다** —
  dbt 는 테이블만 볼 수 있어서, 소스 파일이 몇 개였는지 알 방법이 없다.

  그래서 적재 시점에 (소스 건수, 적재 건수) 를 테이블로 남긴다.
  dbt test 는 이 테이블을 읽어 둘이 맞는지 검사한다.

  이게 두 방식의 실질적 차이다:
    · 실무(Airflow+HDFS): 적재 태스크 안에서 두 숫자를 비교하고 임계 초과 시 알람.
      검증이 적재 코드에 묻혀 있어서, 검증만 따로 돌리거나 이력을 보기 어려웠다.
    · dbt test: 검증이 적재와 분리된 선언으로 존재하고, 감사 테이블에 이력이 쌓인다.
      대신 **감사 기록을 남기는 일은 여전히 적재 쪽 책임**이다 — dbt 가 대신해 주지 않는다.

테이블: tc.audit.ingest_runs
"""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType, TimestampType,
)

AUDIT_NS = "tc.audit"
AUDIT_TABLE = f"{AUDIT_NS}.ingest_runs"

SCHEMA = StructType([
    StructField("run_ts", TimestampType()),
    StructField("layer", StringType()),
    StructField("table_name", StringType()),
    StructField("source_kind", StringType()),
    StructField("source_count", LongType()),   # 수집량 — 소스에서 읽은 단위 수
    StructField("loaded_count", LongType()),   # 적재량 — 테이블에 들어간 행 수
    StructField("note", StringType()),
])


def record(
    spark: SparkSession,
    *,
    layer: str,
    table_name: str,
    source_kind: str,
    source_count: int,
    loaded_count: int,
    run_ts: datetime | None = None,
    note: str = "",
) -> None:
    """적재 1건을 감사 테이블에 append."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {AUDIT_NS}")

    row = [(
        run_ts or datetime.now(tz=timezone.utc),
        layer, table_name, source_kind,
        int(source_count), int(loaded_count), note,
    )]
    df = spark.createDataFrame(row, SCHEMA)

    if spark.catalog.tableExists(AUDIT_TABLE):
        df.writeTo(AUDIT_TABLE).append()
    else:
        df.writeTo(AUDIT_TABLE).using("iceberg").create()

    delta = loaded_count - source_count
    flag = "OK" if delta == 0 else f"DELTA {delta:+,}"
    print(f"  [audit] {table_name}: 수집 {source_count:,} / 적재 {loaded_count:,} [{flag}]")

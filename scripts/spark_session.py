"""
공용 SparkSession 팩토리 — Iceberg 카탈로그가 붙은 세션을 만든다.

모든 ingest/transform 스크립트와 dbt-spark(session) 가 이걸 통해 동일한 설정을 쓴다.

버전 고정 근거:
  Iceberg 1.11.0 이 제공하는 Spark 런타임은 3.4 / 3.5 / 4.0 / 4.1 까지다.
  PySpark 최신은 4.2 지만 매칭되는 iceberg-spark-runtime 아티팩트가 없어 4.1 로 고정했다.

카탈로그:
  `tc` — hadoop 타입. 별도 메타스토어 서비스 없이 warehouse 디렉토리의 메타데이터 파일로
  동작한다. 개인 프로젝트에 Hive Metastore/REST 카탈로그를 띄우는 것은 과하다고 판단.
"""
from __future__ import annotations

import os
from pathlib import Path

# 드라이버 힙은 JVM 이 뜨기 전에 정해져야 한다.
# local 모드에서는 SparkSession.builder.config("spark.driver.memory", ...) 가 효과가 없다 —
# 그 설정이 적용되는 시점엔 이미 JVM 이 기본 힙(1GB)으로 떠 있다.
# 그래서 py4j 게이트웨이가 읽는 PYSPARK_SUBMIT_ARGS 로 넘긴다.
_DRIVER_MEM = os.environ.get("TC_DRIVER_MEMORY", "8g")
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS", f"--driver-memory {_DRIVER_MEM} pyspark-shell"
)

from pyspark.sql import SparkSession  # noqa: E402

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
CATALOG = "tc"

LAKEHOUSE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_WAREHOUSE = LAKEHOUSE_DIR / "warehouse"


def get_spark(app_name: str = "training-companion", warehouse: Path | None = None) -> SparkSession:
    wh = Path(os.environ.get("TC_WAREHOUSE") or warehouse or DEFAULT_WAREHOUSE)
    wh.mkdir(parents=True, exist_ok=True)

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ICEBERG_RUNTIME)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(wh))
        .config("spark.sql.defaultCatalog", CATALOG)
        # 로컬 단일 머신 — 셔플 파티션 기본 200 은 과하다.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .master("local[*]")
        .getOrCreate()
    )

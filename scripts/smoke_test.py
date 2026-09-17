"""
0단계 스모크 테스트 — Spark + Iceberg 가 실제로 붙는지 확인한다.

Iceberg 테이블을 만들고 → 쓰고 → 읽고 → 스키마 진화까지 한 번 돌린 뒤 정리한다.
여기서 막히면 그 위 레이어는 의미가 없으므로 가장 먼저 통과시킨다.

  source lakehouse/env.sh && lakehouse/.venv/bin/python lakehouse/scripts/smoke_test.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spark_session import CATALOG, get_spark  # noqa: E402

TABLE = f"{CATALOG}.smoke.probe"


def main() -> int:
    spark = get_spark("smoke-test")
    print(f"[1] SparkSession OK — Spark {spark.version}")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.smoke")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

    spark.sql(f"""
        CREATE TABLE {TABLE} (
            id     BIGINT,
            sport  STRING,
            dist_km DOUBLE
        ) USING iceberg
        PARTITIONED BY (sport)
    """)
    print("[2] Iceberg 테이블 생성 OK (PARTITIONED BY sport)")

    spark.sql(f"""
        INSERT INTO {TABLE} VALUES
            (1, 'cycling', 411.0),
            (2, 'swimming', 1.9),
            (3, 'running', 10.0)
    """)
    n = spark.table(TABLE).count()
    print(f"[3] 쓰기/읽기 OK — {n} 행")

    # 스키마 진화: 컬럼 추가 후 기존 행이 NULL 로 읽히는지 (재작성 없이)
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN avg_watts DOUBLE")
    spark.sql(f"INSERT INTO {TABLE} VALUES (4, 'cycling', 109.0, 185.0)")
    print("[4] 스키마 진화 OK — ADD COLUMN 후 재적재")
    spark.sql(f"SELECT * FROM {TABLE} ORDER BY id").show()

    # 스냅샷 = Iceberg 의 타임트래블 근거. 메타데이터 테이블이 붙는지 확인.
    snaps = spark.sql(f"SELECT snapshot_id, operation FROM {TABLE}.snapshots").collect()
    print(f"[5] 스냅샷 메타데이터 OK — {len(snaps)}개 (타임트래블 가능)")

    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {CATALOG}.smoke")
    print("[6] 정리 완료 — 0단계 통과 ✅")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

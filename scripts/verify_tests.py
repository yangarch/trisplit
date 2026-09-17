#!/usr/bin/env python3
"""
테스트가 실제로 잡는지 검증한다 — "테스트를 테스트하기".

44 개가 전부 통과하는 것만으로는 아무것도 증명되지 않는다.
한 번도 실패한 적 없는 테스트는 잘못 짜여 있어도 알 수 없기 때문이다.

그래서 **실제로 겪었던 사고를 일부러 주입하고**, 해당 테스트가 FAIL 하는지 확인한 뒤
원상복구한다. 복구는 Iceberg 스냅샷 롤백을 쓴다 — 데이터를 손으로 되돌리지 않는다.

  python3 lakehouse/scripts/verify_tests.py
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

LAKEHOUSE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAKEHOUSE / "scripts"))
from spark_session import get_spark  # noqa: E402

DBT = LAKEHOUSE / ".venv/bin/dbt"
AUDIT = "tc.audit.ingest_runs"
SILVER_ACTS = "tc.silver.activities"


def run_test(name: str) -> bool:
    """dbt test 하나를 돌려 통과 여부 반환."""
    r = subprocess.run(
        [str(DBT), "test", "--select", name],
        cwd=LAKEHOUSE / "dbt", capture_output=True, text=True,
    )
    return "PASS=1" in r.stdout


def snapshot_id(spark, table: str) -> int:
    """현재 스냅샷 ID.

    `.snapshots` 를 committed_at 으로 정렬하면 안 된다 —
    롤백으로 버려진 스냅샷도 목록에 그대로 남아서, 롤백 직후에는
    가장 최근 커밋이 **방금 버린 불량 스냅샷**이 된다.
    현재 상태는 `.refs` 의 main 브랜치가 가리키는 것이다.
    """
    return spark.sql(
        f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'"
    ).first()["snapshot_id"]


def rollback(spark, table: str, snap: int) -> None:
    short = table.split(".", 1)[1]          # tc.silver.activities → silver.activities
    spark.sql(f"CALL tc.system.rollback_to_snapshot('{short}', {snap}L)")


def main() -> int:
    spark = get_spark("verify-tests")
    spark.sparkContext.setLogLevel("ERROR")

    results: list[tuple[str, str, bool]] = []

    def check(label: str, test_name: str, expect_fail: bool = True) -> None:
        passed = run_test(test_name)
        ok = (not passed) if expect_fail else passed
        results.append((label, test_name, ok))
        verdict = "잡음 ✅" if ok else "못 잡음 ❌"
        print(f"    → {test_name}: {'PASS' if passed else 'FAIL'} ({verdict})")

    # ── 1. 2단계에서 실제로 겪은 사고: 날짜 컬럼 전량 NULL ──
    print("\n[1] 날짜 컬럼 전량 NULL (2단계 실제 사고 재현)")
    snap = snapshot_id(spark, SILVER_ACTS)
    spark.sql(f"UPDATE {SILVER_ACTS} SET start_date_key = NULL")
    check("날짜 전량 NULL", "source_not_null_silver_activities_start_date_key")
    rollback(spark, SILVER_ACTS, snap)
    n_null = spark.sql(
        f"SELECT count(*) c FROM {SILVER_ACTS} WHERE start_date_key IS NULL"
    ).first()["c"]
    print(f"    복구: 스냅샷 롤백 완료 (NULL {n_null}건)")

    # ── 2. 수집량 ≠ 적재량 ──
    print("\n[2] 수집량 대비 적재량 불일치 (파싱 실패로 일부 유실)")
    snap_a = snapshot_id(spark, AUDIT)
    spark.sql(f"""
        INSERT INTO {AUDIT}
        SELECT current_timestamp(), 'bronze', 'tc.bronze.activities',
               'strava_json', 838L, 830L, 'FAULT INJECTION'
    """)
    check("수집 838 / 적재 830", "assert_ingest_reconciled")
    rollback(spark, AUDIT, snap_a)

    # ── 3. 적재량 급감 (임계값 알람) ──
    print("\n[3] 적재량 직전 대비 급감 (임계값 초과)")
    snap_a = snapshot_id(spark, AUDIT)
    spark.sql(f"""
        INSERT INTO {AUDIT}
        SELECT current_timestamp(), 'bronze', 'tc.bronze.trackpoints_gpx',
               'gpx_trkpt', 1000000L, 1000000L, 'FAULT INJECTION'
    """)
    check("182만 → 100만 (-45%)", "assert_no_volume_regression")
    rollback(spark, AUDIT, snap_a)

    # ── 4. 중복 필터 유실 (리팩터링 사고 시뮬레이션) ──
    print("\n[4] Gold 가 중복 활동을 다시 포함 (412.9km 이중 계상 재발)")
    stg = LAKEHOUSE / "dbt/models/staging/stg_activities.sql"
    original = stg.read_text()
    try:
        stg.write_text(original.replace("where not is_duplicate", "where 1 = 1"))
        subprocess.run([str(DBT), "run", "--select", "mart_sport_totals"],
                       cwd=LAKEHOUSE / "dbt", capture_output=True, text=True)
        check("중복 8건 재포함", "assert_gold_excludes_duplicates")
    finally:
        stg.write_text(original)
        subprocess.run([str(DBT), "run", "--select", "mart_sport_totals"],
                       cwd=LAKEHOUSE / "dbt", capture_output=True, text=True)
        print("    복구: stg_activities 원복 + 마트 재빌드")

    # ── 결과 ──
    print("\n" + "=" * 62)
    print(f"{'주입한 사고':<28}{'테스트':<26}{'결과':>6}")
    print("-" * 62)
    for label, test, ok in results:
        print(f"{label:<28}{test[:24]:<26}{'✅' if ok else '❌':>6}")
    caught = sum(1 for _, _, ok in results if ok)
    print("-" * 62)
    print(f"{caught}/{len(results)} 건 탐지")

    spark.stop()
    return 0 if caught == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())

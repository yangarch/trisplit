#!/usr/bin/env bash
# 레이크하우스 파이프라인 전체 실행 (Bronze → Silver).
#
# 5단계에서 Airflow DAG 가 이 순서를 그대로 태스크로 옮긴다.
# 스트림 재수집(fetch_streams.py)은 Strava API 한도에 걸려 성격이 달라서 여기 넣지 않는다 —
# 별도로 돌린다.
#
#   bash lakehouse/scripts/run_pipeline.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAKEHOUSE="$(dirname "$HERE")"
# shellcheck source=/dev/null
source "$LAKEHOUSE/env.sh"
PY="$LAKEHOUSE/.venv/bin/python"

run() {
  echo ""
  echo "════════ $1 ════════"
  "$PY" "$LAKEHOUSE/$2"
}

run "Bronze: 활동/랩/스플릿" "ingest/bronze_activities.py"
run "Bronze: GPX 트랙포인트" "ingest/bronze_trackpoints.py"
run "Bronze: 원본 스트림"    "ingest/bronze_streams.py"
run "Silver: 활동 정규화+중복판정" "transform/silver_activities.py"
run "Silver: 시계열 통합"    "transform/silver_trackpoints.py"
run "Silver: 랩/스플릿"      "transform/silver_laps_splits.py"

echo ""
echo "════════ Gold: dbt ════════"
# ftp-log.md 는 사람이 손으로 고치는 파일이라 매 실행마다 시드를 다시 뽑는다.
"$PY" "$LAKEHOUSE/scripts/export_ftp_seed.py"
cd "$LAKEHOUSE/dbt"
"$LAKEHOUSE/.venv/bin/dbt" seed
"$LAKEHOUSE/.venv/bin/dbt" run

echo ""
echo "════════ 품질 검증: dbt test ════════"
# 적재가 끝난 뒤 검증한다. 실패하면 파이프라인도 실패시킨다 (set -e) —
# 조용히 통과시키면 2단계의 "전량 NULL 인데 성공 보고" 가 반복된다.
"$LAKEHOUSE/.venv/bin/dbt" test

# 리포트 생성은 파이프라인의 마지막 단계다.
# 밖에서 따로 호출하면 env.sh 를 안 거쳐 JAVA_HOME 이 없어 죽는다 (실제로 겪었다).
if [ -n "${TC_REPORT_OUT:-}" ]; then
  echo ""
  echo "════════ 리포트 생성 ════════"
  "$PY" "$LAKEHOUSE/scripts/build_report.py" --out "$TC_REPORT_OUT"
fi

echo ""
echo "[✓] 파이프라인 완료 (적재 + 변환 + 검증)"

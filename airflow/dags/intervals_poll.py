"""
intervals.icu 폴링 — 15분마다 새 활동을 확인하고, 있을 때만 레이크하우스를 돌린다.

웹훅을 쓰지 않는 이유: intervals.icu 웹훅은 OAuth 앱 전용이라 개인 API 키로는 받을 수 없다.
15분 간격이면 업로드 후 반영까지 대략 15~25분 — Strava 웹훅(약 10분)과 체감 차이가 크지 않고,
호출은 하루 ~100회로 한도(5,000)의 2% 다.

    fetch ──(NEW>0)──▶ trigger_pipeline ─▶ training_companion_lakehouse
          └(NEW=0)──▶ 건너뜀 (skipped — 실패가 아니다)

수집 태스크는 메인 DAG 의 fetch_intervals 와 같은 pool(intervals_api, 슬롯 1)을 쓴다 —
둘이 동시에 state.json 을 고치지 않게.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from tc_alerts import notify_failure


def _has_new(ti) -> bool:
    out = (ti.xcom_pull(task_ids="fetch") or "").strip()
    return out.startswith("NEW=") and int(out[4:] or 0) > 0


with DAG(
    dag_id="intervals_poll",
    description="intervals.icu 새 활동 확인 → 있으면 레이크하우스 실행",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    # 폴링 한 번 실패는 다음 15분이 메운다 — 재시도 1회 후에도 실패면 알린다 (API 키·차단 등)
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=3),
                  "on_failure_callback": notify_failure},
    tags=["intervals", "poll"],
) as dag:
    fetch = BashOperator(
        task_id="fetch",
        bash_command='python "$TC_ROOT/lakehouse/ingest/intervals/fetch.py" sync',
        pool="intervals_api",
    )
    has_new = ShortCircuitOperator(task_id="has_new", python_callable=_has_new)
    trigger = TriggerDagRunOperator(
        task_id="trigger_pipeline",
        trigger_dag_id="training_companion_lakehouse",
        wait_for_completion=False,
    )
    fetch >> has_new >> trigger

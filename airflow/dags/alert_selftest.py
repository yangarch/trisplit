"""
실패 알림 자가 점검 — 일부러 실패하는 태스크 하나.

알림 경로(NTFY_TOPIC → 콜백 → ntfy.sh → 폰)는 진짜 실패가 나기 전까지 검증되지 않는다.
알림이 조용히 안 울리는 것도 "실패했는데 멀쩡해 보이는" 상태라, 수동으로 고장을 주입해 확인한다.
토픽을 바꾸거나 compose 를 고친 뒤 한 번 트리거하면 된다. 스케줄 없음.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from tc_alerts import notify_failure

with DAG(
    dag_id="alert_selftest",
    description="실패 알림 점검용 — 일부러 실패한다",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Seoul"),
    catchup=False,
    # 재시도 없이 바로 최종 실패 → 콜백
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["ops"],
) as dag:
    BashOperator(
        task_id="deliberate_failure",
        bash_command='echo "알림 점검용 고장 주입"; exit 1',
    )

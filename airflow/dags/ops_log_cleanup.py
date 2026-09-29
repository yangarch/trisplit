"""
Airflow 태스크 로그 정리 — 30일 넘은 로그를 지운다.

Airflow 3 는 로그를 스스로 지우지 않는다. 파이프라인 한 번에 로그가 1~2MB 쌓여
1년이면 수백 MB 가 되고, 볼륨(airflow-logs)이 조용히 커진다.
30일이면 "지난달 그 실패가 뭐였지" 를 보기에 충분하다 — 그보다 오래된 건 리포트와
git 이력이 대신 말해 준다.

파이프라인(22:00)과 겹치지 않게 새벽 04:00 에 돈다.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from tc_alerts import notify_failure

RETENTION_DAYS = 30

with DAG(
    dag_id="ops_log_cleanup",
    description=f"{RETENTION_DAYS}일 넘은 태스크 로그 삭제",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["ops"],
) as dag:
    BashOperator(
        task_id="delete_old_logs",
        # 파일을 먼저 지우고, 비게 된 run/task 디렉토리를 정리한다.
        # -mindepth 1: 로그 루트 자체는 지우지 않는다 (볼륨 마운트 지점).
        bash_command=(
            f"find /opt/airflow/logs -type f -mtime +{RETENTION_DAYS} -print -delete | wc -l"
            " | xargs -I{} echo '삭제한 로그 파일: {}개' && "
            "find /opt/airflow/logs -mindepth 1 -type d -empty -delete && "
            "du -sh /opt/airflow/logs"
        ),
    )

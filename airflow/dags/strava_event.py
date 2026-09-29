"""
Strava 업로드 이벤트 → 라이덕 분석 대기 → 레이크하우스 실행.

웹훅 수신기(airflow/webhook/)가 활동 생성·수정 이벤트를 모아(debounce) 이 DAG 를
conf={"activity_ids": [...]} 로 트리거한다.

    wait_for_riduck ──(준비됨 / 20분 초과)──→ fetch_events ──→ trigger_pipeline
                                                                   └→ training_companion_lakehouse

왜 기다리나:
  라이덕은 업로드 **뒤에** description 에 분석(훈련부하·R파워·훈련량)을 써 넣는다.
  업로드 즉시 받으면 분석 없는 JSON 이 저장되고, sync 는 받은 활동을 다시 받지 않는다.
  고정 지연(예: 20분)이 아니라 **조건 대기**다 — 라이덕 처리 시간은 매번 다르고(보통 수 분),
  수영·러닝처럼 라이덕 대상이 아닌 활동은 기다릴 이유가 없어 바로 통과한다.

20분 안에 분석이 안 붙으면 기다림을 포기하고 진행한다(soft_fail → skipped).
그 활동의 라이덕 지표는 22:00 보정 실행(최근 2일 재수집)이 채운다.

reschedule 모드: 확인할 때만 깨어나고 사이에는 작업 슬롯을 비운다 —
2014년 i5 에서 20분간 프로세스 하나를 붙잡아 둘 이유가 없다.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.bash import BashSensor

from tc_alerts import notify_failure

FETCH = 'python "$TC_ROOT/lakehouse/ingest/strava/fetch.py"'
# conf 는 수신기가 정수만 넣지만, UI 에서 손으로 트리거할 수도 있다.
# int 필터로 강제해 셸에 다른 문자열이 섞이지 않게 한다 (변환 불가 값은 0 → 조회 실패로 끝남).
IDS = '{{ dag_run.conf.get("activity_ids", []) | map("int") | join(" ") }}'

with DAG(
    dag_id="strava_event",
    description="Strava 업로드 웹훅 → 라이덕 대기 → 레이크하우스 실행",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Seoul"),
    catchup=False,
    # 수신기가 이미 묶어서 보내므로 여러 실행이 겹칠 일은 드물다.
    # 겹쳐도 기다리는 동안은 슬롯을 안 쓰고, 실제 적재는 메인 DAG(max_active_runs=1)가 줄 세운다.
    max_active_runs=2,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2),
                  "on_failure_callback": notify_failure},
    tags=["strava", "event"],
) as dag:

    wait_for_riduck = BashSensor(
        task_id="wait_for_riduck",
        bash_command=f"{FETCH} check-riduck {IDS}",
        mode="reschedule",
        poke_interval=120,
        timeout=20 * 60,
        # 시간 초과는 실패가 아니다 — 분석 없이 먼저 반영하고 밤에 채운다.
        soft_fail=True,
        # 센서 자체 재시도는 의미 없다 (타임아웃은 soft_fail 로 처리)
        retries=0,
    )

    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=f"{FETCH} one {IDS}",
        # 대기가 시간 초과로 skipped 여도 수집은 한다
        trigger_rule="none_failed",
    )

    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_pipeline",
        trigger_dag_id="training_companion_lakehouse",
        # 메인 DAG 는 max_active_runs=1 — 22:00 실행 중이면 그 뒤에 줄 선다
        wait_for_completion=False,
    )

    wait_for_riduck >> fetch_events >> trigger_pipeline

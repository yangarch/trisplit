"""
training-companion 레이크하우스 파이프라인 DAG.

`scripts/run_pipeline.sh` 는 순서대로 한 줄씩 돌리지만, 실제 의존 관계는 선형이 아니다.
Bronze 세 개는 서로 독립이고, Silver 두 개도 activities 만 끝나면 병렬로 갈 수 있다.
Airflow 로 옮기는 실익이 여기 있다 — 순서가 아니라 **의존 그래프**를 선언한다.

    bronze_activities ──┬─→ silver_activities ─┬─→ silver_trackpoints ──┐
    bronze_trackpoints ─┤                      └─→ silver_laps_splits ──┤
    bronze_streams ─────┘                                               │
                                                                        ▼
                                              dbt_seed → dbt_run → dbt_test

  silver_trackpoints 는 bronze_streams + bronze_trackpoints + silver_activities 를 모두 쓴다
  (스트림 우선 통합 + start_ts_utc 로 offset 정규화).

스트림 재수집(fetch_streams.py)은 여기 없다.
Strava 일일 한도에 묶여 있어 주기와 실패 성격이 다르다 — 별도 DAG 가 맞다.

메모리 — 의존 그래프와 동시 실행은 별개다:
  Docker 전체 가용이 7.7GB 인데 Airflow 컴포넌트가 이미 1.5~2GB 를 쓴다.
  Spark 드라이버 3g 를 둘만 띄워도 한계라 **max_active_tasks=1** 로 둔다.

  Bronze 세 개가 서로 독립이라는 **선언은 그대로 유지한다** — 병렬로 못 도는 것은
  이 노트북의 자원 제약이지 파이프라인의 성질이 아니다.
  워커가 여러 대인 환경에 올리면 이 값만 올리면 된다.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# 경로는 **런타임 셸에서** $TC_ROOT 로 푼다. DAG 파싱 시점에 os.environ 을 읽어
# 값을 복제하면 안 된다 — 직렬화된 DAG 에 옛 값이 굳어서, compose 설정을 고쳐도
# 태스크는 계속 옛 경로를 쓴다 (실제로 /opt/project 가 남아 Mkdirs 실패가 났다).
# 환경변수의 진실 공급원은 docker-compose.yml 하나다.
LAKEHOUSE = "$TC_ROOT/lakehouse"

# DAG 수준 정책만 여기서 정한다. 경로·JAVA_HOME 은 compose 가 이미 내보낸다.
TASK_ENV = {
    # 호스트는 8g 지만 Docker 전체 가용이 7.7GB 라 컨테이너에서는 낮춘다.
    "TC_DRIVER_MEMORY": "3g",
}


def spark_task(dag: DAG, task_id: str, script: str, args: str = "") -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f'python "{LAKEHOUSE}/{script}" {args}'.strip(),
        env=TASK_ENV,
        append_env=True,
        dag=dag,
    )


with DAG(
    dag_id="training_companion_lakehouse",
    description="Strava 원본 → Iceberg 메달리온 → dbt 마트 + 품질 검증",
    # 기존 launchd 동기화가 매일 22:00 에 돌아 raw-gpx 를 채운다.
    # 그 뒤에 도는 게 맞으므로 23:00 (KST) 으로 둔다.
    schedule="0 23 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Seoul"),
    catchup=False,
    # 3g 짜리 Spark 드라이버가 셋씩 뜨면 컨테이너가 죽는다.
    max_active_tasks=1,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["lakehouse", "iceberg", "dbt"],
) as dag:

    # ── Bronze — 서로 독립. 소스가 다르다. ──
    bronze_activities = spark_task(dag, "bronze_activities", "ingest/bronze_activities.py")
    bronze_trackpoints = spark_task(dag, "bronze_trackpoints", "ingest/bronze_trackpoints.py")
    bronze_streams = spark_task(dag, "bronze_streams", "ingest/bronze_streams.py")

    # ── Silver ──
    silver_activities = spark_task(dag, "silver_activities", "transform/silver_activities.py")
    silver_trackpoints = spark_task(dag, "silver_trackpoints", "transform/silver_trackpoints.py")
    silver_laps_splits = spark_task(dag, "silver_laps_splits", "transform/silver_laps_splits.py")

    # ── Gold (dbt) ──
    # ftp-log.md 는 사람이 손으로 고치는 파일이라 매 실행마다 시드를 다시 뽑는다.
    export_ftp = BashOperator(
        task_id="export_ftp_seed",
        bash_command=f'python "{LAKEHOUSE}/scripts/export_ftp_seed.py"',
        env=TASK_ENV,
        append_env=True,
    )
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f'cd "{LAKEHOUSE}/dbt" && dbt seed',
        env=TASK_ENV,
        append_env=True,
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f'cd "{LAKEHOUSE}/dbt" && dbt run',
        env=TASK_ENV,
        append_env=True,
    )
    # 검증 실패를 파이프라인 실패로 만든다.
    # 통과시키면 2단계의 "전량 NULL 인데 성공 보고" 가 그대로 반복된다.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f'cd "{LAKEHOUSE}/dbt" && dbt test',
        env=TASK_ENV,
        append_env=True,
    )

    # ── 의존 그래프 ──
    bronze_activities >> silver_activities

    # 시계열 통합은 두 Bronze 소스와 silver_activities(start_ts_utc) 를 모두 쓴다
    [bronze_trackpoints, bronze_streams, silver_activities] >> silver_trackpoints

    silver_activities >> silver_laps_splits

    [silver_trackpoints, silver_laps_splits] >> export_ftp >> dbt_seed >> dbt_run >> dbt_test

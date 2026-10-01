"""
training-companion 레이크하우스 파이프라인 DAG.

`scripts/run_pipeline.sh` 는 순서대로 한 줄씩 돌리지만, 실제 의존 관계는 선형이 아니다.
Bronze 세 개는 서로 독립이고, Silver 두 개도 activities 만 끝나면 병렬로 갈 수 있다.
Airflow 로 옮기는 실익이 여기 있다 — 순서가 아니라 **의존 그래프**를 선언한다.

                     fetch_intervals  (Garmin → intervals.icu → 원본 계약으로 변환)
                              │  활동 JSON + 스트림을 한 번에 만든다 (Bronze 세 개 모두의 상류)
                              ▼
    bronze_activities ──┬─→ silver_activities ─┬─→ silver_trackpoints ──┐
    bronze_trackpoints ─┤                      └─→ silver_laps_splits ──┤
    bronze_streams ─────┘                                               │
                                                                        ▼
                  export_ftp_seed → export_body_seeds → dbt_seed → dbt_run → dbt_test ─┬─→ build_report
                                                                                       └─→ build_html

  silver_trackpoints 는 bronze_streams + bronze_trackpoints + silver_activities 를 모두 쓴다
  (스트림 우선 통합 + start_ts_utc 로 offset 정규화).

수집 소스 — 2026-09-30 에 바뀌었다:
  ~09-29  Strava API (ingest/strava/). Strava 가 Standard API 를 유료 구독 전용으로 바꾸며
          앱이 Inactive 가 됐다 (HTTP 403 Application Status Inactive). 받아 둔 원본은 그대로 쓴다.
  09-30~  intervals.icu (ingest/intervals/). Garmin 공식 연동, 개인 API 키. 원본을 기존 계약
          (Strava activity JSON · 스트림 json.gz) 으로 변환해 이 아래는 소스를 모른다.
  라이덕 분석(Strava description)은 이 경로에 없다 — 받아 둔 9/29 까지만 남는다.

실행 경로:
  22:00 예약       — 매일. 새 활동이 없어도 돌아 리포트의 "오늘" 을 갱신한다.
  intervals_poll   — 15분마다 새 활동을 확인하고, 있을 때만 이 DAG 를 트리거한다.
                     (intervals.icu 웹훅은 OAuth 앱 전용이라 개인 키로는 폴링한다)
  두 경로의 수집 태스크는 pool "intervals_api"(슬롯 1)로 묶어 동시에 돌지 않게 한다 —
  같은 state.json 을 쓴다.

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

from tc_alerts import notify_failure

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
    description="Garmin(intervals.icu) 원본 → Iceberg 메달리온 → dbt 마트 + 품질 검증",
    schedule="0 22 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Seoul"),
    catchup=False,
    # 3g 짜리 Spark 드라이버가 셋씩 뜨면 컨테이너가 죽는다.
    max_active_tasks=1,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        # 최종 실패 시 폰으로 (tc_alerts.py). 폴백이 없으니 알리지 않으면 조용히 낡는다.
        "on_failure_callback": notify_failure,
    },
    tags=["lakehouse", "iceberg", "dbt"],
) as dag:

    # ── 수집 ── 표준 라이브러리만 쓴다 (Spark 불필요).
    # 컷오버(2026-09-30) 이후 새로/바뀐 활동만 받는다 (내용 지문 비교 — 이름·설명 수정도 다시 받는다).
    fetch_intervals = BashOperator(
        task_id="fetch_intervals",
        bash_command=f'python "{LAKEHOUSE}/ingest/intervals/fetch.py" sync',
        pool="intervals_api",
    )

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
    # 증상 로그·피팅 변경 이력도 사람이 대화하며 고치는 마크다운이다 — 같은 이유로 매번 시드를 뽑는다.
    # 형식이 깨진 행은 여기서 실패한다 (조용히 버리면 "기록했는데 분석에 안 나오는" 상태).
    export_body = BashOperator(
        task_id="export_body_seeds",
        bash_command=f'python "{LAKEHOUSE}/scripts/export_body_seeds.py"',
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

    # ── 리포트 ── 검증을 통과한 마트로만 만든다 (dbt_test 뒤).
    #   마크다운 — AI 상담 입력용
    #   HTML     — 사람이 브라우저·폰으로 보는 용도. compose 의 report 서비스(nginx)가 서빙한다.
    #              reports/ 가 아니라 reports/web/ 에 쓴다 — nginx 에는 이 폴더만 마운트해서
    #              날짜별 마크다운·스크립트가 LAN 에 노출되지 않게 한다.
    build_report = BashOperator(
        task_id="build_report",
        bash_command=f'python "{LAKEHOUSE}/scripts/build_report.py" --out "$TC_ROOT/reports/latest_overview.md"',
        env=TASK_ENV,
        append_env=True,
    )
    build_html = BashOperator(
        task_id="build_html",
        bash_command=f'python "{LAKEHOUSE}/scripts/build_html.py" --out "$TC_ROOT/reports/web/index.html"',
        env=TASK_ENV,
        append_env=True,
    )

    # ── 의존 그래프 ──
    fetch_intervals >> [bronze_activities, bronze_trackpoints, bronze_streams]
    bronze_activities >> silver_activities

    # 시계열 통합은 두 Bronze 소스와 silver_activities(start_ts_utc) 를 모두 쓴다
    [bronze_trackpoints, bronze_streams, silver_activities] >> silver_trackpoints

    silver_activities >> silver_laps_splits

    [silver_trackpoints, silver_laps_splits] >> export_ftp >> export_body >> dbt_seed >> dbt_run >> dbt_test
    dbt_test >> [build_report, build_html]

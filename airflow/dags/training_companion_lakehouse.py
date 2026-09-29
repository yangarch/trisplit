"""
training-companion 레이크하우스 파이프라인 DAG.

`scripts/run_pipeline.sh` 는 순서대로 한 줄씩 돌리지만, 실제 의존 관계는 선형이 아니다.
Bronze 세 개는 서로 독립이고, Silver 두 개도 activities 만 끝나면 병렬로 갈 수 있다.
Airflow 로 옮기는 실익이 여기 있다 — 순서가 아니라 **의존 그래프**를 선언한다.

                        fetch_strava  (Strava 신규 활동 → raw-gpx)
                              │  (Bronze 세 개 모두의 상류)
                              ├─→ fetch_streams (per-point 파워 → raw/streams) ─→ bronze_streams
                              ▼
    bronze_activities ──┬─→ silver_activities ─┬─→ silver_trackpoints ──┐
    bronze_trackpoints ─┤                      └─→ silver_laps_splits ──┤
    bronze_streams ─────┘                                               │
                                                                        ▼
                  export_ftp_seed → export_body_seeds → dbt_seed → dbt_run → dbt_test ─┬─→ build_report
                                                                                       └─→ build_html

  silver_trackpoints 는 bronze_streams + bronze_trackpoints + silver_activities 를 모두 쓴다
  (스트림 우선 통합 + start_ts_utc 로 offset 정규화).

Strava 수집(fetch.py sync)은 원래 맥의 launchd 가 22:00 에 돌렸다. 홈서버로 옮기면서
이 DAG 의 첫 태스크가 됐다 — 수집과 적재가 한 실행 안에서 의존 관계로 묶인다.

실행 경로는 둘이다:
  22:00 예약     — 보정 실행. 최근 이틀을 다시 받아 늦게 붙은 분석·수정·놓친 이벤트를 메운다.
  strava_event  — 업로드 웹훅이 트리거. 라이덕 분석을 기다렸다가 이 DAG 를 실행한다.
⚠️ Strava 는 토큰 갱신 때 refresh_token 을 회전시킨다. 수집하는 곳이 둘이면 한쪽 토큰이
   무효가 되므로 **이 DAG 만 수집한다** (맥 launchd 는 disable). max_active_runs=1 도
   같은 이유로 필요하다 — 두 실행이 동시에 토큰을 갱신하면 안 된다.

스트림 수집(fetch_streams.py)도 이 DAG 안에 있다. 처음엔 과거 392건 백필이 일일 한도에
걸리는 작업이라 따로 뒀는데, 그 뒤로 **아무도 돌리지 않아** 새 라이딩의 파워가 조용히 빠졌다
(9/13·9/19 6건). 백필이 끝난 지금은 신규 라이딩 몇 건이라 한도와 무관하다.
한도에 걸려도 상태를 저장하고 0 으로 끝나며, 다음 실행이 이어 받는다.

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
    description="Strava 원본 → Iceberg 메달리온 → dbt 마트 + 품질 검증",
    # 수집이 이 DAG 안으로 들어왔으므로 launchd 가 돌던 22:00 (KST) 에 그대로 시작한다.
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
    # 22:00 예약 실행은 **최근 이틀을 이미 받은 것까지 다시 받는다** (보정 실행).
    #   웹훅(strava_event DAG)은 업로드 직후라 라이덕 분석·제목 수정이 빠질 수 있고,
    #   서버가 꺼져 있던 동안의 이벤트는 아예 놓친다. 야간 실행이 그 빈틈을 메운다.
    # 이벤트·수동 트리거 실행은 새 활동만 받는다 (빠르게).
    fetch_strava = BashOperator(
        task_id="fetch_strava",
        bash_command=(
            f'python "{LAKEHOUSE}/ingest/strava/fetch.py" sync'
            '{{ " --since 2d --refresh" if dag_run.run_type == "scheduled" else "" }}'
        ),
    )

    # fetch_strava 가 떨어뜨린 활동 JSON 을 보고 파워 있는 활동만 고른다 → 반드시 그 뒤.
    fetch_streams = BashOperator(
        task_id="fetch_streams",
        bash_command=f'python "{LAKEHOUSE}/ingest/fetch_streams.py" fetch',
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
    fetch_strava >> [bronze_activities, bronze_trackpoints]
    fetch_strava >> fetch_streams >> bronze_streams
    bronze_activities >> silver_activities

    # 시계열 통합은 두 Bronze 소스와 silver_activities(start_ts_utc) 를 모두 쓴다
    [bronze_trackpoints, bronze_streams, silver_activities] >> silver_trackpoints

    silver_activities >> silver_laps_splits

    [silver_trackpoints, silver_laps_splits] >> export_ftp >> export_body >> dbt_seed >> dbt_run >> dbt_test
    dbt_test >> [build_report, build_html]

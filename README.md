# trisplit

사이클·수영·러닝 훈련 데이터 파이프라인을 **메달리온 아키텍처 레이크하우스**로 재구축한 기록.
PySpark · Apache Iceberg · dbt · Airflow.

> **이름** — 철인삼종에서 `split` 은 수영/바이크/런 구간 기록을 가리키는 표준 용어다.
> 동시에 이 파이프라인의 실제 테이블이기도 하다 (`bronze.splits`, 12,167 행).

> 이 저장소는 **코드만** 담는다. 원본 활동 데이터(GPS 트랙 포함)는 비공개 저장소에 있다.
> 아래 「데이터 계약」에 스크립트가 기대하는 입력 형식을 적어 두었다.

## 무엇을 바꿨나

이미 돌아가던 파이프라인이 있었다 — Strava API 증분 동기화 → GPX/JSON 파일 저장 →
표준 라이브러리 Python 스크립트가 집계해 마크다운 리포트 생성. 매일 22:00 에 launchd 가 돌린다.

그걸 레이크하우스로 옮기면서 **기존 파이프라인은 끝까지 건드리지 않았다.**
Gold 마트가 같은 숫자를 낸다는 것이 확인된 뒤에 전환했고, 지금도 실패 시 폴백으로 남아 있다.

| 계층 | 내용 | 도구 |
|---|---|---|
| Bronze | 원본 활동·랩·스플릿·트랙포인트·스트림 적재 | PySpark → Iceberg |
| Silver | 정규화, 중복 판정, 두 시계열 소스 통합 | PySpark |
| Gold | 종목별 파생 지표 마트 | dbt (`method: session`) |
| 품질 | 수집량 대비 적재량, 임계값, 스키마 | dbt test 44개 |
| 오케스트레이션 | 의존 그래프 · 스케줄링 | Airflow 3 (Docker Compose) |

## 규모

| 테이블 | 행 |
|---|---:|
| `bronze.activities` | 840 |
| `bronze.laps` / `splits` | 7,695 / 12,167 |
| `bronze.trackpoints_gpx` | 1,824,629 |
| `bronze.streams` | 1,361,818 |
| `silver.trackpoints` (통합) | 1,697,925 |

활동 요약은 840 행이지만 시계열은 **180만 행**이고, 원래 611 개의 작은 XML 파일에 흩어져
있었다. Parquet 으로 옮기며 **418.9MB / 611 파일 → 28.1MB / 5 파일** 이 됐다.

## 파이프라인이 실제로 찾아낸 것

### 1. 거리 이중 계상 412.9 km

사이클 누적이 11,077.9km 로 잡혀 있었는데 실제는 **10,665.0 km** 였다.
**헤드유닛 두 대가 같은 라이딩을 각각 기록**해 둘 다 Strava 에 올라간 날이 6 일 있었고,
활동 14 건이 여기 묶였다. 그룹마다 가장 오래 기록된 것 하나만 남겨 8 건을 제외했고,
거리로는 412.9km 다. 기존 스크립트는 활동 JSON 을 전부 세기 때문에 그대로 합산됐다.

단순 쌍 비교로는 부족했다 — 어떤 날은 한 기기가 4 시간 연속으로 기록하는 동안
다른 기기가 3 조각으로 끊어 기록했고, 조각끼리는 서로 겹치지 않는다.
종목별로 시작시각 순 정렬해 "앞선 활동들의 최대 종료시각" 보다 늦게 시작하면 새 그룹으로
끊는 **세션화**(윈도우 함수 2 회)로 추이적 겹침을 한 그룹으로 묶고,
그룹 안에서 `elapsed_time` 이 가장 긴 것을 남겼다.

**행은 지우지 않는다.** `is_duplicate` / `duplicate_of_activity_id` 로 표시만 하고
Gold 에서 필터한다 — 판정이 틀렸을 때 근거를 되짚을 수 있어야 한다.

### 2. 버려지고 있던 per-point 파워

기존 파이프라인은 Strava 스트림을 GPX 로 직렬화하면서 **watts 를 버렸다.**
GPX 표준에 파워 확장이 없다는 이유였고, JSON 쪽에도 스트림은 저장되지 않았다.
그래서 파워 분석이 활동 평균/NP 단위에 갇혀 있었다.

원본 스트림을 그대로 Bronze 에 적재해 복구했다. per-point watts 135 만 행.
주행 중 평균 파워가 2024 년 184.3W → 2026 년 202.7W 로 잡힌다.

### 3. NP 를 직접 계산하고 벤더 값과 대조

이제 NP(Normalized Power)를 원본에서 계산할 수 있다 — 30 초 이동평균 → 4 제곱 평균 → 4 제곱근.
368 라이딩에서 Strava 제공값과 대조: **상관 0.983, 그런데 편향이 +8.04W 로 MAE 와 같다.**
노이즈가 아니라 체계적 차이라는 뜻이다.

| 가설 | 검증 | 결과 |
|---|---|---|
| 윈도우 정의 차이 | ROWS/RANGE × 첫 30초 포함/제외 4가지 | 전부 +8.0~8.7W. 아님 |
| 1Hz 가정 오류 | 샘플 간격 실측 | 1s 131만 / 2s 이상 930건. 아님 |
| 평활 강도 차이 | 창 길이별 편향 | 20s +11.64 → 60s +2.16, **단조 감소** |

Strava 의 `weighted_average_watts` 는 표준 NP 보다 강하게 평활하는 자체 지표다.
**창을 Strava 에 맞추지 않았다** — 30 초는 표준 정의고, 비공개 지표를 역산해 맞추면
값이 비표준이 되어 다른 도구와 비교할 수 없다. 두 값을 다 남기고 `np_diff` 로 차이를 드러낸다.

## 품질 검증 — 옮겨보니 경계가 바뀐다

실무(Airflow + HDFS)에서 구현했던 "수집량 대비 적재량 비교 + 임계값 알람" 을 dbt test 로 옮겼다.

**수집량은 파이프라인이 끝나면 사라진다.** dbt 는 테이블만 볼 수 있어서 "소스 파일이 몇 개였는지"
를 알 방법이 없다. 그래서 적재 시점에 (수집량, 적재량) 을 감사 테이블에 남기는 코드를
따로 만들어야 했다 (`scripts/ingest_audit.py`).

| | Airflow + HDFS | dbt test |
|---|---|---|
| 검증 위치 | 적재 태스크 **안** | 적재와 분리된 선언 |
| 따로 돌리기 | 안 됨 | `dbt test` 단독 |
| 실패 기록 | "그 태스크가 죽었다" | 어느 검증이 왜 깨졌는지 |
| 임계값 | 코드에 하드코딩 | `dbt_project.yml` vars |
| 수집량 확보 | 태스크가 이미 들고 있음 | **감사 테이블을 새로 만들어야** |

검증은 깔끔해지는 대신 **수집량을 데이터로 남길 책임이 적재 쪽에 새로 생긴다.**

### 테스트를 테스트했다

44 개가 전부 통과하는 것만으로는 아무것도 증명되지 않는다.
실제로 겪은 사고를 주입하고 해당 테스트가 FAIL 하는지 확인한다 (`scripts/verify_tests.py`).
복구는 **Iceberg 스냅샷 롤백**으로 한다.

| 주입한 사고 | 테스트 | |
|---|---|:-:|
| 날짜 컬럼 전량 NULL | `not_null` | ✅ |
| 수집 838 / 적재 830 | `assert_ingest_reconciled` | ✅ |
| 적재량 −45% 급감 | `assert_no_volume_regression` | ✅ |
| 중복 재포함 | `assert_gold_excludes_duplicates` | ✅ |

그런데 **그걸로도 부족했다.** 나중에 `assert_ingest_reconciled` 가 다섯 테이블 중 하나만
검사하고 있었다는 것을 발견했다 — 적재 스크립트가 셋이라 `run_ts` 가 제각각인데
전역 `max(run_ts)` 로 "최신 실행" 을 잡은 탓이다. 주입한 결함이 마침 그 테이블에 있어서
결함 주입 검증은 통과했었다.

## 부딪힌 것들

| 문제 | 원인 |
|---|---|
| `OutOfMemoryError` | local 모드 기본 힙 1GB. `builder.config()` 로는 못 바꾼다 — JVM 이 이미 떠 있어서 `PYSPARK_SUBMIT_ARGS` 로 넘겨야 한다 |
| `INVALID_NON_DETERMINISTIC_EXPRESSIONS` | MERGE 는 소스를 여러 번 스캔해 `input_file_name()` 을 거부한다 → 스테이징을 실체화 |
| 컬럼 하나가 조용히 전량 NULL | 정규식을 `F.expr()` 안 SQL 문자열로 넘겨 백슬래시가 이스케이프로 먹힘. `try_*` 가 실패를 NULL 로 삼켜 파이프라인은 "성공" 보고 |
| `Replacing a view is not supported` | Iceberg **hadoop 카탈로그는 뷰를 지원하지 않는다** → staging 을 `ephemeral` 로 |
| `NotFoundException: /Users/...` | **Iceberg hadoop 카탈로그가 절대경로를 메타데이터에 박는다.** 컨테이너에서 다른 경로로 마운트하면 못 읽는다 |
| 404 로 파이프라인 정지 | 재시도 불가한 404 를 치명적 에러로 처리해 뒤가 전부 막힘 |

마지막 두 개가 가장 값진 교훈이었다. 0 단계에서 "개인 프로젝트에 메타스토어는 과하다" 고
판단해 hadoop 카탈로그를 골랐는데, **그 선택의 대가가 컨테이너로 옮길 때 정확히 이 형태로
나타났다.** 실무가 오브젝트 스토리지 + 카탈로그 서비스(REST/Glue/Nessie)를 쓰는 이유다.

## 왜 PySpark 4.1 인가 (4.2 가 아니라)

PySpark 최신은 4.2.0 이지만 Iceberg 1.11.0 이 배포하는 Spark 런타임은 3.4 / 3.5 / 4.0 / 4.1 까지다.
테이블 포맷 쪽에 버전을 맞췄다.

## 실행

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp config/gear_names.example.json config/gear_names.json   # 장비 매핑 (선택)
source env.sh                                  # JAVA_HOME, 경로, 드라이버 힙
.venv/bin/python scripts/smoke_test.py         # Spark + Iceberg 연결 확인

bash scripts/run_pipeline.sh                   # Bronze → Silver → dbt seed/run/test
.venv/bin/python scripts/parity_check.py       # 기존 파이프라인과 전 지표 대조
.venv/bin/python scripts/verify_tests.py       # 결함 주입으로 테스트 검증
```

Airflow:

```bash
cd airflow
cp .env.example .env && $EDITOR .env           # 경로 + 자격증명 (기본값 없음)
docker compose up -d                           # http://localhost:8080
```

로그인은 `.env` 의 `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` 를 쓴다.
Airflow 3 의 기본 인증은 `SimpleAuthManager` 라 `airflow users create`(FAB 전용)가
동작하지 않는다 — 사용자는 설정으로 선언하고 비밀번호는 init 이 파일로 써 넣는다.

## 데이터 계약

스크립트는 저장소 상위 디렉토리에 원본이 있다고 가정한다.

```
<project-root>/
├── activities/
│   ├── strava/raw-gpx/
│   │   ├── <YYYY-MM-DD>_<activity_id>.json    # Strava /activities/{id} 응답 그대로
│   │   └── <YYYY-MM-DD>_<activity_id>.gpx     # latlng/time/altitude/hr/cad 스트림 → GPX
│   └── cycling/ftp-log.md                     # FTP 이력 (마크다운 표)
└── lakehouse/                                 # 이 저장소
```

`ingest/fetch_streams.py` 가 `lakehouse/raw/streams/` 에 원본 스트림을 받아 둔다
(Strava 자격증명 필요).

## 구조

```
ingest/      Bronze — 원본 → Iceberg, 적재 감사 기록
transform/   Silver — 정규화·중복 판정·시계열 통합
dbt/         Gold   — 마트 6개 + 테스트 44개
airflow/     DAG 10태스크 (Docker Compose)
scripts/     공용 세션, 파이프라인 러너, 패리티·테스트 검증, 리포트 생성
config/      참조 데이터 (장비 매핑)
```

# trisplit

사이클·수영·러닝 훈련 데이터 파이프라인을 **메달리온 아키텍처 레이크하우스**로 재구축하고,
홈서버에서 매일 운영하는 기록. PySpark · Apache Iceberg · dbt · Airflow.

> **이름** — 철인삼종에서 `split` 은 수영/바이크/런 구간 기록을 가리키는 표준 용어다.
> 동시에 이 파이프라인의 실제 테이블이기도 하다 (`bronze.splits`, 12,314 행).

> 이 저장소는 **코드만** 담는다. 원본 활동 데이터(GPS 트랙 포함)와 토큰은 공개하지 않는다 —
> 홈서버와 로컬에만 있다. 아래 「데이터 계약」에 스크립트가 기대하는 입력 형식을 적어 두었다.

## 무엇을 바꿨나

이미 돌아가던 파이프라인이 있었다 — Strava API 증분 동기화 → GPX/JSON 파일 저장 →
표준 라이브러리 Python 스크립트가 집계해 마크다운 리포트 생성. 매일 22:00 에 노트북의 launchd 가 돌렸다.

그걸 레이크하우스로 옮기면서 **기존 파이프라인은 끝까지 건드리지 않았다.**
Gold 마트가 94 개 지표에서 같은 숫자를 낸다는 것을 셀 단위로 확인(94/94)한 뒤에 전환했다.

| 계층 | 내용 | 도구 |
|---|---|---|
| 수집 | 활동 · per-point 스트림 (Garmin 기록) | intervals.icu API — 2026-09-29 까지는 Strava API |
| Bronze | 원본 활동·랩·스플릿·트랙포인트·스트림 적재 (증분) | PySpark → Iceberg |
| Silver | 정규화, 중복 판정, 두 시계열 소스 통합 | PySpark |
| Gold | 종목별 지표 · 파워 분석 · 증상×부하 · 현황 모델 17 개 | dbt (`method: session`) |
| 품질 | 수집량 대비 적재량, 임계값, 스키마, 값 회귀 | dbt test 86 개 |
| 리포트 | 마크다운(상담 입력) + HTML 두 페이지 — 「지금」(전 종목 현황·최근 운동) · 「기록」(월·연·전체) | Python, 인라인 SVG, nginx |
| 운영 | 의존 그래프 · 스케줄 · 이벤트 트리거 · 실패 알림 | Airflow 3 (Docker Compose, 홈서버) |

## 규모

| 테이블 | 행 |
|---|---:|
| `bronze.activities` | 857 |
| `bronze.laps` / `splits` | 7,790 / 12,314 |
| `bronze.trackpoints_gpx` | 1,850,066 |
| `bronze.streams` | 1,380,772 |
| `silver.trackpoints` (통합) | 1,723,362 |

활동 요약은 857 행이지만 시계열은 **180만 행**이고, 원래 600 개가 넘는 작은 XML 파일에 흩어져
있었다. Parquet 으로 옮기며 **418.9MB / 611 파일 → 28.1MB / 5 파일** 이 됐다 (전환 시점 기준).

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

이 발견이 나중에 설계 하나를 뒤집었다. 전환 뒤에도 옛 스크립트를 **실패 시 폴백**으로 남겨 뒀는데,
폴백이 도는 날엔 리포트가 **경고 없이 이중 계상된 숫자로 되돌아간다.** 기능이 조금 줄어드는
안전망이 아니라 정확성이 퇴화하는 안전망이었다. 홈서버로 옮기며 폴백을 없애고 실패 알림으로 바꿨다.

### 2. 버려지고 있던 per-point 파워

기존 파이프라인은 Strava 스트림을 GPX 로 직렬화하면서 **watts 를 버렸다.**
GPX 표준에 파워 확장이 없다는 이유였고, JSON 쪽에도 스트림은 저장되지 않았다.
그래서 파워 분석이 활동 평균/NP 단위에 갇혀 있었다.

원본 스트림을 그대로 Bronze 에 적재해 복구했다. per-point watts 135 만 행 이상.
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

### 4. 기록해 둔 FTP 가 과대추정이었다

per-point 파워로 구간별 최고 파워(Mean Maximal Power, 5초~4시간 14 구간)를 계산했다.
**불완전 윈도우를 거르는 것**이 정확성의 핵심이다 — 활동보다 긴 구간은 계산하지 않아서,
4시간 구간의 유효 라이딩은 368 건 중 7 건뿐이다.

| 근거 | 기록된 FTP 대비 |
|---|---|
| 20분 최고 × 0.95 | −14W |
| CP 모델 회귀 (2~20분 구간) | −16W |
| 분석 서비스(라이덕)의 최근 추정 | −32~43W |

서로 다른 두 수식이 **3W 안에서 일치**했다. 20분 최고값은 분석 서비스의 피크파워와 **정확히 같은
값**이 나와 계산 자체도 교차 검증됐다. 이런 추정은 보통 "최대 노력 기록이 없으면 하한일 뿐" 인데,
20분 최고가 나온 세션의 IF 가 0.99 라 전제도 충족됐다. 기록된 FTP 는 사람이 관리하는 앵커라
파이프라인이 고치지 않는다 — 차이를 마트로 드러낼 뿐이다.

MMP 는 라이딩 × 14 구간 윈도우 연산이라 전체를 다시 돌면 181초가 걸렸다.
dbt 증분 모델(`int_power_curve_ride`)로 새 라이딩만 계산해 **3.4초**가 됐다.

### 5. 몸의 기록을 라이딩 데이터에 붙이기

훈련 상담은 숫자만으로 끝나지 않는다 — 통증·불편 같은 몸의 신호와 피팅(클릿·안장·바 높이) 변경이
대화로 쌓인다. 그 기록을 **사람이 쓰는 마크다운 표 두 개**(증상 로그: 한 부위 한 줄, 피팅 변경:
적용일 기준)로 남기고, 파이프라인은 읽기만 한다.

- 라이딩마다 그날 증상 · 부하(저케이던스 고토크 시간, 클라임 케이던스) · **그 시점의 세팅 구간**을 붙인다.
- **기록하지 않은 라이딩도 포함한다.** 기록한 날만 모으면 비교 기준이 없고, 미기록을 0 으로 치면
  좋아 보이는 착시가 생긴다 — 0(봤는데 없음)과 미기록을 구분한다.
- 같은 날 두 가지를 바꾸면 그 구간의 효과는 분리할 수 없다고 표시한다.

가설 하나가 숫자로 반박됐다. 노트에는 "고토크 클라임에서 증상이 온다" 가 반복됐는데,
증상이 강했던 라이딩과 약했던 라이딩의 고토크 시간·클라임 케이던스는 거의 같았다.
같은 코스를 다른 세팅으로 탄 두 날의 차이도 부하가 아니라 **세팅 구간**에서 갈렸다.
기록 수가 적어 결론이 아니라 다음 대화의 근거로 쓴다.

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

### 증분 적재에서 감사를 지키기

트랙포인트·스트림을 매번 전부 다시 파싱하던 것을 `(경로, mtime)` 기준 변경분만 처리하도록 바꿨다
(변경·삭제된 파일의 행을 지운 뒤 append — MERGE 불필요). 전체 106초 → 85초인데,
중요한 건 시간보다 **성질**이다. 파일 수에 비례해 늘던 것이 신규분에만 비례하게 됐다.

위험은 감사였다. 이번에 파싱한 것만 세면 건너뛴 파일이 검증에서 빠진다. 그래서 **수집량은 매 실행
소스 전체를 적재 경로와 독립적으로 센다** — GPX 는 XML 파싱 없이 `<trkpt` 바이트 스캔,
스트림은 드라이버에서 gz+JSON 직접 파싱. 건너뛴 파일까지 포함해 총 행수가 맞는지 검증된다.

### 테스트를 테스트했다

테스트가 전부 통과하는 것만으로는 아무것도 증명되지 않는다.
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

알림도 같은 방식으로 검증한다. 알림 경로는 진짜 실패가 나기 전까지 검증되지 않으므로,
일부러 실패하는 DAG(`alert_selftest`)로 고장을 주입해 폰에 도착하는 것까지 확인했다.

## 운영 — 홈서버에서 매일

노트북 launchd 에서 **홈서버(2014년형 맥미니 + Ubuntu, 4 스레드, 8GB)** 의 Airflow 로 옮겼다.
Spark 는 태스크 안에서 local 모드로 뜨고 내려간다 — 상주 클러스터를 둘 규모가 아니다.

```
운동 업로드 (Garmin → intervals.icu 자동 동기화)
  └─ intervals_poll (15분) ─▶ 새 활동 있으면 ─▶ 메인 DAG 트리거
22:00 예약 ─▶ 메인 DAG (새 활동이 없어도 리포트의 "오늘" 갱신)

메인 DAG
  fetch_intervals ─┬─▶ bronze_activities ──▶ silver_activities ─┬─▶ silver_laps_splits ──┐
                   ├─▶ bronze_trackpoints ────────────────────┐ │                        │
                   └─▶ bronze_streams ────────────────────────┴─┴─▶ silver_trackpoints ─┤
                                                                                        ▼
            export_ftp_seed ─▶ export_body_seeds ─▶ dbt seed ─▶ dbt run ─▶ dbt test ─┬─▶ build_report
                                                                                     └─▶ build_html
```

### 수집 소스가 바뀌었다 — Strava 에서 intervals.icu 로

2026-09-30, Strava 가 Standard 등급 API 를 **유료 구독 전용**으로 바꾸면서(기존 개발자 무료 3개월 종료)
API 앱이 비활성이 됐다 — 모든 호출이 `403 Application Status Inactive`. 실패 알림으로 그날 바로 알았다.

기록의 원천은 Garmin 이고 Strava 는 중간 경유지였다. **Garmin → intervals.icu(공식 연동, 개인 API 키) → 여기**
로 경로를 바꿨다. 수집기(`ingest/intervals/`)가 응답을 **기존 원본 계약**(Strava 활동 JSON · 스트림 json.gz)
으로 변환해 저장하므로 Bronze 이후는 한 줄도 바꾸지 않았다 — 활동 JSON 을 읽는 경로 하나만 늘렸다.

- **컷오버는 날짜로.** Strava 쪽 외부 id(`garmin_ping_…`)와 Garmin 활동 id 가 다른 체계라 같은 활동을 id 로
  짝지을 수 없다. Strava 로 받은 마지막 날(9/29) 다음 날부터 받는다.
- **id 충돌 회피.** `10¹² + intervals 번호` — Strava id 대역과 겹치지 않게 자리를 띄웠다.
- **없는 스트림은 만든다.** 경사(고도/거리 차분, 약 30m 구간)와 정지 판정(속도 > 0.5 m/s).
  대신 **토크 · 좌우 밸런스**가 초 단위로 새로 생겼다 (양발 파워미터).
- **웹훅 → 폴링.** intervals.icu 웹훅은 OAuth 앱 전용이라 개인 키로는 15분 폴링이다. 새 활동이 없으면
  뒤를 건너뛴다(skipped — 실패가 아니다). 하루 ~100회로 한도의 2%.
- **잃은 것:** 라이덕 분석(Strava description 에 쓰이던 훈련부하) — 9/29 까지만 남는다.
- 이전 Strava 경로(업로드 웹훅 + 라이덕 분석 조건 대기 + 공유 앱 구독을 nginx `mirror` 로 복제)의
  코드는 `ingest/strava/` · `airflow/webhook/` 에 남겨 두었다.

### 실패하면 알린다

- 태스크가 재시도까지 실패하면 `on_failure_callback` 이 ntfy 로 폰에 푸시한다. 공개 서버라
  메시지에는 태스크 이름만 넣는다.
- 스케줄러가 죽어 **아예 안 돈** 경우는 서버 안에서 알 수 없다. 로컬 동기화 스크립트가 리포트
  날짜를 보고 낡았으면 알린다.
- 태스크 로그는 30일 보관 (`ops_log_cleanup`).

### 배포

코드는 이 저장소 하나가 원본이다. 서버의 작업 폴더는 이 저장소의 클론이고 `git pull --ff-only` 로
배포한다. 데이터(`raw/`, `warehouse/`)와 시크릿(`airflow/.env`, 토큰)은 `.gitignore` 로 같은 폴더에
공존한다. 개인 데이터가 섞인 로컬 폴더는 공개 저장소의 작업 폴더로 두지 않는다 —
`.gitignore` 에 하나를 빠뜨리는 순간 공개되기 때문이다.

## 부딪힌 것들

| 문제 | 원인 |
|---|---|
| `OutOfMemoryError` | local 모드 기본 힙 1GB. `builder.config()` 로는 못 바꾼다 — JVM 이 이미 떠 있어서 `PYSPARK_SUBMIT_ARGS` 로 넘겨야 한다 |
| `INVALID_NON_DETERMINISTIC_EXPRESSIONS` | MERGE 는 소스를 여러 번 스캔해 `input_file_name()` 을 거부한다 → 스테이징을 실체화 |
| 컬럼 하나가 조용히 전량 NULL | 정규식을 `F.expr()` 안 SQL 문자열로 넘겨 백슬래시가 이스케이프로 먹힘. `try_*` 가 실패를 NULL 로 삼켜 파이프라인은 "성공" 보고 |
| `Replacing a view is not supported` | Iceberg **hadoop 카탈로그는 뷰를 지원하지 않는다** → staging 을 `ephemeral` 로 |
| `NotFoundException: /Users/...` | **Iceberg hadoop 카탈로그가 절대경로를 메타데이터에 박는다.** 컨테이너에서 다른 경로로 마운트하면 못 읽는다. 서버로 옮길 때도 웨어하우스를 복사할 수 없어 원본에서 재구축했다 |
| 404 로 파이프라인 정지 | 재시도 불가한 404 를 치명적 에러로 처리해 뒤가 전부 막힘 |
| `.gitignore` 규칙이 조용히 무효 | **git 은 `#` 을 줄 맨 앞에서만 주석으로 본다.** 인라인 주석을 붙이면 설명이 패턴의 일부가 되어 아무것도 매칭하지 않는다. 에러도 경고도 없다 |
| init 컨테이너가 항상 성공으로 끝남 | `set -e` 없이 마지막 명령이 `echo` 였다. 앞의 실패가 덮여 `service_completed_successfully` 를 통과하고, 원인 표시 없이 로그인 401 만 남는다 |
| Linux 에서만 `Permission denied` | Docker Desktop 은 bind mount 의 소유권을 무시하지만 Linux 는 검사한다. 컨테이너 uid 50000 으로는 호스트 사용자 소유 폴더에 못 쓴다 → `AIRFLOW_UID` |
| `ModuleNotFoundError: airflow` | 이미지에 없는 uid 로 돌면 `HOME=/` 가 되어 `~/.local` 의 패키지를 못 찾는다. 공식 entrypoint 가 보정해 주는데 init 이 `entrypoint: bash` 로 그걸 건너뛰고 있었다 |
| 새 라이딩의 파워가 조용히 빠짐 | 스트림 수집기를 백필용으로 한 번 돌리고 **스케줄에 넣지 않았다.** 활동은 매일 들어오니 멀쩡해 보였고, 파워 분석만 2주 가까이 멈춰 있었다 |
| 시드 빈 칸이 문자열 `'None'` 으로 | dbt-spark(session) seed 는 빈 문자열 칸을 NULL 이 아니라 `'None'` 으로 싣는다. 테스트가 전부 통과한 채 이름표에 "(None)" 이 찍혔다 → staging 에서 정규화 + **값을 직접 보는** 회귀 테스트 |
| intervals.icu 만 403 (Cloudflare 1010) | 키는 맞았다. Python 기본 User-Agent(`Python-urllib`)를 Cloudflare 가 막고 있었다 — UA 를 명시하니 200 |
| rsync 제외 패턴이 새어 `.env` 가 복사됨 | `/`로 시작하는 제외 패턴은 **소스 루트 기준**이다. 하위 폴더만 보내는 순간 매칭이 안 된다. git 배포로 바꿔 경로 자체를 없앴다 |

이 목록에서 반복되는 모양이 하나 있다 — **실패했는데 성공처럼 보이는 것**.
`try_*` 는 실패를 NULL 로 바꾸고, `.gitignore` 는 규칙이 무효여도 조용하고, `echo` 는 앞의 실패를 덮고,
수집기 하나가 빠져도 나머지가 돌면 파이프라인은 초록색이다. 품질 테스트와 실패 알림,
그리고 알림 자체의 고장 주입 검증을 붙인 이유가 여기 있다.

hadoop 카탈로그 두 줄이 가장 값진 교훈이었다. 0 단계에서 "개인 프로젝트에 메타스토어는 과하다" 고
판단해 hadoop 카탈로그를 골랐는데, **그 선택의 대가가 컨테이너로, 다시 서버로 옮길 때마다 정확히
이 형태로 나타났다.** 실무가 오브젝트 스토리지 + 카탈로그 서비스(REST/Glue/Nessie)를 쓰는 이유다.

## 왜 PySpark 4.1 인가 (4.2 가 아니라)

PySpark 최신은 4.2.0 이지만 Iceberg 1.11.0 이 배포하는 Spark 런타임은 3.4 / 3.5 / 4.0 / 4.1 까지다.
테이블 포맷 쪽에 버전을 맞췄다.

## 실행

로컬:

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

Airflow (서버):

```bash
cd airflow
cp .env.example .env && $EDITOR .env           # 경로 · 자격증명 · AIRFLOW_UID(Linux) · 알림 토픽 · Strava id
docker compose up -d                           # Airflow :8080 · 리포트 :8090 · 웹훅 수신기 :8091
```

로그인은 `.env` 의 `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` 를 쓴다.
Airflow 3 의 기본 인증은 `SimpleAuthManager` 라 `airflow users create`(FAB 전용)가
동작하지 않는다 — 사용자는 설정으로 선언하고 비밀번호는 init 이 파일로 써 넣는다.
알림 경로 점검은 `alert_selftest` DAG 를 한 번 트리거한다.

## 데이터 계약

스크립트는 저장소 상위 디렉토리에 원본이 있다고 가정한다.

```
<project-root>/
├── activities/
│   ├── strava/
│   │   ├── api/config.json                     # OAuth 토큰 (회전된다, 커밋 금지)
│   │   ├── api/state.json                      # 동기화 상태
│   │   └── raw-gpx/
│   │       ├── <YYYY-MM-DD>_<activity_id>.json # Strava /activities/{id} 응답 그대로
│   │       └── <YYYY-MM-DD>_<activity_id>.gpx  # latlng/time/altitude/hr/cad 스트림 → GPX
│   └── cycling/ftp-log.md                      # FTP 이력 (마크다운 표, 사람이 관리)
├── reports/                                    # 리포트 출력 (web/ 만 nginx 로 서빙)
└── lakehouse/                                  # 이 저장소
```

수집기(`ingest/strava/fetch.py`)가 `raw-gpx/` 를, `ingest/fetch_streams.py` 가
`lakehouse/raw/streams/` 를 채운다. 토큰은 `ingest/strava/oauth.py` 로 한 번 발급한다.

## 구조

```
ingest/intervals/ 수집 — intervals.icu → 기존 원본 계약 변환 (2026-09-30~)
ingest/strava/   수집 — Strava 수집기 · OAuth (~2026-09-29, 보존)
ingest/          Bronze — 원본 → Iceberg (증분), 스트림 수집, 적재 감사 기록
transform/       Silver — 정규화·중복 판정·시계열 통합
dbt/             Gold   — 모델 17개 + 테스트 86개
airflow/dags/    메인 파이프라인 · intervals 폴링 · 로그 정리 · 알림 점검
airflow/webhook/ Strava 웹훅 수신기 + nginx mirror 예시
scripts/         공용 세션, 파이프라인 러너, 증분 판정, 패리티·테스트 검증, 리포트·차트 생성
config/          참조 데이터 (장비 매핑)
```

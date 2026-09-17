{#
  임계값 알람 — 적재량이 직전 실행 대비 급감하면 실패.

  실무에서 쓰던 "수집량이 평소보다 N% 이상 줄면 알람" 을 옮긴 것이다.
  소스가 일부만 도착했거나, 필터가 잘못 걸리거나, 상태 파일이 깨져 재수집이 덜 된 경우를 잡는다.

  허용폭은 `volume_regression_pct` 변수 (기본 5%).
  임계값을 코드에 묻지 않고 dbt_project.yml 로 끌어낸 것이 실무 방식과의 차이다 —
  조정 이력이 git 에 남고, 테이블마다 다르게 주기도 쉽다.

  ⚠️ 이 데이터는 append-only 라 행 수가 줄어들 일이 정상적으로는 없다.
     줄었다면 거의 항상 사고다.
#}

{% set pct = var('volume_regression_pct') %}

with runs as (
    select
        table_name,
        run_ts,
        loaded_count,
        lag(loaded_count) over (partition by table_name order by run_ts) as prev_count
    from {{ source('audit', 'ingest_runs') }}
),

latest as (
    select
        table_name,
        run_ts,
        loaded_count,
        prev_count,
        row_number() over (partition by table_name order by run_ts desc) as rn
    from runs
    where prev_count is not null
)

select
    table_name,
    prev_count,
    loaded_count,
    round(100.0 * (loaded_count - prev_count) / prev_count, 2) as change_pct,
    {{ pct }} as allowed_drop_pct
from latest
where rn = 1
  and loaded_count < prev_count * (1 - {{ pct }} / 100.0)

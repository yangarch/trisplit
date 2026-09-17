{#
  파워 커버리지 하한.

  Strava 가 "평균 파워 있음" 이라고 표시한 라이딩은 per-point watts 도 있어야 한다.
  스트림 재수집이 덜 됐거나, 통합 규칙이 잘못돼 GPX(watts 없음) 쪽으로 넘어가면 떨어진다.

  하한은 `min_power_coverage_pct` (기본 95%).
  100% 가 아닌 이유: Strava 에서 삭제된 활동 1 건은 스트림을 영영 못 받는다.
#}

with acts as (
    select count(*) as n_power_rides
    from {{ ref('stg_activities') }}
    where sport = 'cycling' and average_watts is not null
),

covered as (
    select count(distinct activity_id) as n_covered
    from {{ ref('stg_trackpoints') }}
    where sport = 'cycling' and watts is not null
)

select
    a.n_power_rides,
    c.n_covered,
    round(100.0 * c.n_covered / a.n_power_rides, 1) as coverage_pct,
    {{ var('min_power_coverage_pct') }} as min_pct
from acts a
cross join covered c
where 100.0 * c.n_covered / a.n_power_rides < {{ var('min_power_coverage_pct') }}

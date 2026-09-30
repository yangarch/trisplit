-- 월 · 연 × 종목 누적 — 기록 페이지용 (records.html).
-- 라이프타임 한 줄은 매번 보기엔 지루하다 — 대신 기간을 골라 볼 수 있게 월·연 단위로 남긴다.
--
--   period_type = 'month' → period = 'YYYY-MM'
--   period_type = 'year'  → period = 'YYYY'
--   period_type = 'all'   → period = 'all'

with a as (
    select sport, month, cast(year as string) as year, distance_km, moving_h, elev_m
    from {{ ref('stg_activities') }}
)

select 'month' as period_type, month as period, sport,
       count(*) as activities, round(sum(distance_km), 1) as distance_km,
       round(sum(moving_h), 1) as moving_h, round(sum(elev_m), 0) as elev_m
from a group by month, sport

union all

select 'year', year, sport,
       count(*), round(sum(distance_km), 1), round(sum(moving_h), 1), round(sum(elev_m), 0)
from a group by year, sport

union all

select 'all', 'all', sport,
       count(*), round(sum(distance_km), 1), round(sum(moving_h), 1), round(sum(elev_m), 0)
from a group by sport

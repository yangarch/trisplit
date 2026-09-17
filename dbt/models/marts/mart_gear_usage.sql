-- overview.py 「3. 사이클 디테일 > 자전거별 사용」 이식.
-- 장비 결정에 쓰는 누적 거리라 (CLAUDE.md), 중복 제거가 특히 중요하다 —
-- 2026-09-12 와 09-07 중복이 전부 같은 자전거(chichi)에 붙어 있었다.

select
    gear_id,
    gear_label,
    count(*)                                  as activities,
    sum(distance_km)                          as distance_km,
    sum(moving_h)                             as moving_h,
    sum(elev_m)                               as elev_m,
    avg(distance_km)                          as avg_km_per_ride,
    count_if(is_indoor)                       as indoor_activities,
    100.0 * count_if(is_indoor) / count(*)    as indoor_pct,
    min(start_date_key)                             as first_used,
    max(start_date_key)                             as last_used
from {{ ref('stg_activities') }}
where sport = 'cycling'
group by gear_id, gear_label

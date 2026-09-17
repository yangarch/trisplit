-- overview.py 「2. 연도별 활동 수 / 연도별 거리」 이식.

select
    year,
    sport,
    count(*)                    as activities,
    sum(distance_km)            as distance_km,
    sum(moving_h)               as moving_h,
    sum(elev_m)                 as elev_m,
    count_if(is_indoor)         as indoor_activities
from {{ ref('stg_activities') }}
group by year, sport

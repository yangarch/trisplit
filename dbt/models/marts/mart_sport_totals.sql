-- overview.py 「1. 종목별 누적 (전 기간)」 이식.
-- 차이: overview.py 는 중복 활동을 그대로 합산한다. 여기서는 제외된다.
--
-- 반올림하지 않는다. 마트는 값을 저장하고 반올림은 표현 쪽에서 한다 —
-- 저장 단계에서 자르면 정밀도가 영영 사라지고, 언어마다 반올림 규칙이 달라
-- 대조할 때 가짜 불일치가 난다 (Python 은 banker's rounding, SQL 은 올림.
-- 걷기 고도 합계 8720.5 가 8720 / 8721 로 갈렸다).

select
    sport,
    count(*)                       as activities,
    sum(distance_km)               as distance_km,
    sum(moving_h)                  as moving_h,
    sum(elev_m)                    as elev_m,
    avg(distance_km)               as avg_km_per_activity,
    min(start_date_key)            as first_date,
    max(start_date_key)            as last_date
from {{ ref('stg_activities') }}
group by sport

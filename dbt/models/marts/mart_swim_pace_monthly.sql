-- overview.py 「4. 수영 디테일 > 월별 평균 페이스」 이식.
--
-- 거리 가중평균이다. 단순 평균이 아닌 이유: 200m 한 번과 2,000m 한 번의 페이스를
-- 같은 무게로 평균 내면 왜곡된다.
--
-- ⚠️ overview.py 의 경고를 그대로 옮긴다 — Strava swim 의 평균 속도는 휴식 시간을 포함한다.
-- 강습과 자유수영을 섞어 비교하면 오해가 생긴다 (`activities/swimming/notes-on-pace.md`).
-- 이 마트는 추세 파악용이고, 강습/자유수영 분리는 하지 않는다.

with swims as (
    select
        month,
        year,
        distance_km * 1000 as distance_m,
        average_speed
    from {{ ref('stg_activities') }}
    where sport = 'swimming'
      and average_speed > 0
)

select
    month,
    year,
    count(*)                                                    as swims,
    sum(distance_m)                                             as distance_m,
    -- 거리로 가중한 평균 속도 → 100m 당 초
    100.0 / (sum(average_speed * distance_m) / sum(distance_m)) as pace_s_per_100m,
    concat(
        cast(floor(100.0 / (sum(average_speed * distance_m) / sum(distance_m)) / 60) as int),
        ':',
        lpad(cast(cast(100.0 / (sum(average_speed * distance_m) / sum(distance_m)) % 60 as int) as string), 2, '0')
    )                                                           as pace_mmss
from swims
group by month, year

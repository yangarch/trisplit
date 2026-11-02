-- overview.py 「4. 수영 디테일 > 월별 평균 페이스」 이식 — 2026-11-02 에 페이스를 둘로 나눴다.
--
-- 기기마다 "시간" 이 다르다:
--   가민        moving_time 이 헤엄친 시간 (벽에서 쉬는 시간 제외, 이동/전체 중앙 0.27)
--   애플워치    moving_time = elapsed_time (휴식 구분 없음, 85건 모두 0.98~1.0)
--               Strava 시절에도, intervals.icu(Intervals Companion 앱 경유, type OpenWaterSwim)에서도 같다.
-- 둘을 average_speed 로 섞으면 월 추세가 실력이 아니라 **그달 어느 시계를 찼나** 를 따라간다.
-- 2026-11-02 애플워치 한 건(47분 725m, 휴식 포함 6:36/100m)이 11월 평균이 되어 60~300초 테스트를 깼다.
--
-- 그래서 페이스는 **헤엄친 시간 기준, 휴식을 구분한 기록만** 낸다 (swim_rest_included = false).
-- 애플워치만 있는 달은 NULL — 지어내지 않는다. 횟수·거리는 모든 기록.
--
-- "휴식 포함 페이스(전체 시간/거리)" 를 월 지표로 냈다가 뺐다: 강습 날은 킥판·드릴이 레인 카운트에
-- 안 잡혀 45분에 175m 같은 기록이 흔하다(2026-01~03 가민) → 15~780분/100m 가 나와 의미가 없다.
--
-- 거리 가중이다 (200m 한 번과 2,000m 한 번을 같은 무게로 평균 내지 않는다).
-- 강습/자유수영 구분은 여전히 없다 (`activities/swimming/notes-on-pace.md`).

with swims as (
    select
        month,
        year,
        distance_km * 1000 as distance_m,
        moving_time,
        elapsed_time,
        swim_rest_included
    from {{ ref('stg_activities') }}
    where sport = 'swimming'
      and distance_km > 0
),

m as (
    select
        month,
        year,
        count(*)                                                        as swims,
        count_if(not swim_rest_included)                                as swims_rest_separated,
        sum(distance_m)                                                 as distance_m,
        -- 헤엄친 시간 / 거리 (휴식을 구분한 기록만)
        100.0 * sum(case when not swim_rest_included then moving_time end)
              / nullif(sum(case when not swim_rest_included then distance_m end), 0)
                                                                        as pace_s_per_100m
    from swims
    group by month, year
)

select
    *,
    {{ mmss('pace_s_per_100m') }}  as pace_mmss
from m

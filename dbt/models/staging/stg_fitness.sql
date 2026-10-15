-- intervals.icu 일별 훈련부하 (activities/intervals/wellness.json → seed intervals_fitness).
-- 라이덕 체력/피로/균형(9/29 에서 멈춤)을 대신한다.
--
-- 응답에는 미래 날짜의 예측값(앞으로 운동이 없을 때의 감쇠)도 실려 온다 — 오늘(KST)까지만.
-- 시작점(2025-10)은 0 에서 쌓이므로 처음 6주 남짓은 과소평가다.

select
    fitness_date,
    ctl,
    atl,
    ramp_rate,
    load,
    ride_eftp
from {{ ref('intervals_fitness') }}
where fitness_date <= {{ today_kst() }}

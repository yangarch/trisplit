-- 활동 1건 = 1행, 종목 무관. "최근 운동" 목록과 "방금 한 운동이 들어왔나" 확인용.
--
-- 종목마다 볼 지표가 다르다 — 한 표에 공통 칸 + 종목별 칸:
--   사이클  NP(직접 계산) · IF · TSS · 라이덕 훈련량
--   수영    100m 페이스 (Strava 평균 속도 기준 — 휴식 포함이라 강습/자유수영 비교엔 주의)
--   러닝    km 페이스
-- 증상 로그에 적은 게 있으면 symptom_rows > 0.

with symptoms as (
    select activity_id, count(*) as symptom_rows
    from {{ ref('stg_symptoms') }}
    where activity_id is not null
    group by activity_id
)

select
    a.activity_id,
    a.start_date_key,
    a.start_ts_utc,
    a.sport,
    a.type,
    a.name,
    a.is_indoor,
    a.gear_label,
    a.distance_km,
    round(a.moving_time / 60.0, 0)                                      as moving_min,
    a.elev_m,
    a.average_heartrate,
    -- 사이클
    p.np_computed,
    p.intensity_factor,
    p.tss,
    r.load                                                              as riduck_load,
    -- 수영 / 러닝 페이스 (초). 속도 0 이면 NULL — ANSI 모드는 0 나누기가 에러다
    case when a.sport = 'swimming' then 100.0  / nullif(a.average_speed, 0) end as pace_s_per_100m,
    case when a.sport = 'running'  then 1000.0 / nullif(a.average_speed, 0) end as pace_s_per_km,
    coalesce(s.symptom_rows, 0)                                         as symptom_rows
from {{ ref('stg_activities') }} a
left join {{ ref('mart_ride_power') }}      p on a.activity_id = p.activity_id
left join {{ ref('mart_riduck_metrics') }}  r on a.activity_id = r.activity_id
left join symptoms                          s on a.activity_id = s.activity_id

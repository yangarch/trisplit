-- 라이딩 1건 = 1행: 그날의 증상 · 부하 · 그 시점의 피팅 세팅.
--
-- 증상 기록을 시작한 날부터의 사이클 라이딩 전부가 대상이다 — **증상을 안 적은 라이딩도 포함**한다.
-- 적은 날만 모으면 "증상이 있던 날" 만 보게 되어 비교가 안 된다. 적지 않은 날은
-- symptom_rows = 0 으로 남기고, 지켜보는 부위를 0 으로 적은 날과 구분한다(0 ≠ 미기록).
--
-- 세팅 구간(setup_since): 그 라이딩에 적용되던 가장 최근 피팅 변경의 적용일.
--   자전거 변경은 그 자전거에만, 신발(클릿) 변경은 모든 자전거에 적용된다.
--   KICKR 인도어(gear 없음)는 chichi 가 올라가 있으므로 chichi 로 본다.

with sym as (
    select * from {{ ref('stg_symptoms') }}
    where activity_id is not null
),

since as (
    select min(symptom_date) as first_day from {{ ref('stg_symptoms') }}
),

rides as (
    select
        a.activity_id,
        a.start_date_key                    as ride_date,
        a.name,
        a.is_indoor,
        case when a.gear_label like 'kiki%' then 'kiki' else 'chichi' end as bike,
        a.distance_km,
        a.moving_time,
        a.elev_m
    from {{ ref('stg_activities') }} a
    cross join since s
    where a.sport = 'cycling'
      and a.distance_km > 0
      and a.start_date_key >= s.first_day
),

-- 부위별로 한 칸. 여러 줄이면(좌·우 따로 등) 가장 심한 값.
-- 무릎내측은 좌측 추적이 목적이라 좌·양만 본다.
pivot as (
    select
        activity_id,
        count(*)                                                                  as symptom_rows,
        max(case when area = '무릎내측' and side in ('좌', '양') then severity end) as knee_medial_l,
        max(case when area = '무릎내측' and side in ('좌', '양') then next_day end) as knee_medial_l_next,
        max(case when area = '안장'     then severity end)                         as saddle,
        max(case when area = '손저림'   then severity end)                         as hand_numb,
        max(case when area = '삼두어깨' then severity end)                         as triceps_shoulder,
        max(case when area = '허리'     then severity end)                         as low_back,
        max(case when area like '경련-%' then severity end)                        as cramp,
        -- 같은 오염을 여러 부위 행에 적는 일이 흔하다 → 중복 제거
        concat_ws(' / ', array_distinct(collect_list(confounders)))                as confounders
    from sym
    group by activity_id
),

fit as (
    select effective_date, target from {{ ref('stg_fitting_changes') }}
),

setup as (
    select r.activity_id, max(f.effective_date) as setup_since
    from rides r
    join fit f
      on f.effective_date <= r.ride_date
     and f.target in (r.bike, '신발')
    group by r.activity_id
)

select
    r.activity_id,
    r.ride_date,
    r.name,
    r.bike,
    r.is_indoor,
    r.distance_km,
    round(r.moving_time / 60.0, 1)                      as moving_min,
    r.elev_m,
    p.intensity_factor,
    p.np_computed,
    l.grind_s,
    l.climb_s,
    l.grind_climb_s,
    l.climb_cadence,
    l.pedaling_cadence,
    -- 시간이 다른 라이딩끼리 비교하려고 시간당으로도 낸다
    round(l.grind_s / 60.0, 1)                          as grind_min,
    -- Spark 4 는 ANSI 모드라 0 으로 나누면 NULL 이 아니라 에러 — nullif 로 막는다
    round(l.grind_s / (nullif(r.moving_time, 0) / 3600.0) / 60.0, 1) as grind_min_per_h,
    coalesce(v.symptom_rows, 0)                         as symptom_rows,
    v.knee_medial_l,
    v.knee_medial_l_next,
    v.saddle,
    v.hand_numb,
    v.triceps_shoulder,
    v.low_back,
    v.cramp,
    nullif(v.confounders, '')                           as confounders,
    s.setup_since
from rides r
left join {{ ref('mart_ride_power') }} p on r.activity_id = p.activity_id
left join {{ ref('int_ride_load') }}   l on r.activity_id = l.activity_id
left join pivot v                         on r.activity_id = v.activity_id
left join setup s                         on r.activity_id = s.activity_id

-- 피팅 세팅 구간별 요약 — "바꾼 뒤 나아졌나" 를 보는 표.
--
-- 구간 = 한 피팅 변경의 적용일부터 다음 변경 전날까지 (chichi 기준: chichi + 신발 변경).
-- 첫 변경 전은 baseline. 같은 날 여러 변경이면 한 구간으로 묶이고 n_changes 로 드러난다 —
-- 그 구간의 효과는 어느 변경 때문인지 분리할 수 없다.
--
-- 무릎 수치는 **기록한 라이딩만으로** 평균낸다 (knee_rides). 미기록을 0 으로 치면 좋아 보이는 착시.
-- 부하(grind)는 기록 여부와 무관하게 구간의 모든 파워 라이딩으로 낸다 — 같은 무릎 강도라도
-- 그 구간에 고토크 클라임을 많이 탔는지 함께 봐야 한다.

with changes as (
    select
        effective_date,
        count(*)                                  as n_changes,
        concat_ws(', ', collect_list(change_label)) as changes
    from {{ ref('stg_fitting_changes') }}
    where target in ('chichi', '신발')
    group by effective_date
),

rides as (
    select * from {{ ref('mart_symptom_ride') }}
    where bike = 'chichi'
)

select
    coalesce(cast(r.setup_since as string), 'baseline')        as epoch,
    r.setup_since                                              as epoch_start,
    c.changes,
    c.n_changes,
    min(r.ride_date)                                           as first_ride,
    max(r.ride_date)                                           as last_ride,
    count(*)                                                   as rides,
    round(sum(r.distance_km), 1)                               as distance_km,
    -- 무릎 (좌 내측) — 기록한 라이딩만
    count(r.knee_medial_l)                                     as knee_rides,
    round(avg(r.knee_medial_l), 1)                             as knee_avg,
    max(r.knee_medial_l)                                       as knee_max,
    sum(case when r.knee_medial_l >= 4 then 1 else 0 end)      as knee_ge4_rides,
    -- 부하 — 파워 라이딩 전부
    round(avg(r.grind_min_per_h), 1)                           as grind_min_per_h_avg,
    round(avg(r.climb_cadence), 1)                             as climb_cadence_avg,
    round(avg(r.intensity_factor), 2)                          as if_avg
from rides r
left join changes c on r.setup_since = c.effective_date
group by r.setup_since, c.changes, c.n_changes

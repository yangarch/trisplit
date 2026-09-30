-- 종목별 "지금" — 최근 4주 vs 직전 4주, 마지막 운동 후 경과일.
-- 메인 페이지 첫 문단이 이 표로 쓰인다. 누적(라이프타임)은 기록 페이지로 뺐다 — 매번 볼 숫자가 아니다.
--
-- 4주(28일)인 이유: 주 단위 변동(비·출장 한 주)에 덜 흔들리면서, 한 블록(수영 블록 등)의 변화는 드러난다.
-- "오늘" 은 KST (macros/today_kst.sql).

{% set win = 28 %}

with t as (
    select {{ today_kst() }} as today
),

a as (
    select a.*, datediff(t.today, a.start_date_key) as age_d
    from {{ ref('stg_activities') }} a
    cross join t
    where a.sport in ('cycling', 'swimming', 'running')
)

select
    s.sport,
    max(t.today)                                                               as today,
    max(a.start_date_key)                                                      as last_date,
    datediff(max(t.today), max(a.start_date_key))                              as days_since,
    -- 최근 4주 (오늘 포함 28일)
    count_if(a.age_d between 0 and {{ win - 1 }})                              as n_recent,
    round(sum(case when a.age_d between 0 and {{ win - 1 }} then a.distance_km end), 1) as km_recent,
    round(sum(case when a.age_d between 0 and {{ win - 1 }} then a.moving_h end), 1)    as h_recent,
    -- 직전 4주
    count_if(a.age_d between {{ win }} and {{ 2 * win - 1 }})                  as n_prev,
    round(sum(case when a.age_d between {{ win }} and {{ 2 * win - 1 }} then a.distance_km end), 1) as km_prev,
    round(sum(case when a.age_d between {{ win }} and {{ 2 * win - 1 }} then a.moving_h end), 1)    as h_prev,
    -- 최근 4주 중 가장 긴 것
    round(max(case when a.age_d between 0 and {{ win - 1 }} then a.distance_km end), 1) as longest_recent_km
from (select explode(array('cycling', 'swimming', 'running')) as sport) s
cross join t
left join a on a.sport = s.sport
group by s.sport

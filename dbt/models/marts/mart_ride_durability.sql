-- 지속력(durability) — 장거리 후반에 파워가 얼마나 유지되는가.
--
-- 왜 이 지표인가:
--   브레베·장거리 라이더에게 "1시간 최고 파워" 보다 중요한 건 **10시간째에 얼마를 내는가** 다.
--   기성 도구는 이걸 잘 주지 않는다. 라이딩을 경과시간 4분할해서 구간별 NP 를 내고,
--   마지막 1/4 을 첫 1/4 로 나눈 비율을 본다.
--
--   1.0 근처   페이스를 균일하게 유지
--   0.8 이하   후반 페이드가 큼 (보급·페이싱·체력 중 하나가 원인)
--   1.0 초과   후반에 더 밀어붙였음 (네거티브 스플릿)
--
-- 3시간 이상 라이딩만 본다 — 짧은 라이딩은 4분할이 의미가 없다.
-- NP 는 구간별로 각각 계산한다 (30초 이동평균 → 4제곱 평균 → 4제곱근).
--
-- 파워 끊김 — 파워미터가 중간에 꺼지면 그 뒤가 null 이 아니라 **0W 로** 실려 온다
-- (2026-10-20 205km: 4.9h 이후 파워·케이던스 0, 심박·속도는 정상 → 비율 0.0 으로 테스트 실패).
-- 0W 는 코스팅에서도 정상이라 점 하나로는 못 가른다. 분할 안에서 페달링(watts > 0) 비율을 본다:
-- 3시간+ 라이딩 27건 108분할의 최저가 42.8%(중앙 83%), 끊긴 분할은 0% — 사이 {{ var('dropout_pedaling_share') }} 로 자른다.
-- 끊긴 분할의 NP 와 비율은 NULL (지어내지 않는다), 행은 남기고 power_dropout 으로 표시한다.

with long_rides as (
    select activity_id
    from {{ ref('stg_activities') }}
    where sport = 'cycling' and moving_h >= 3.0
),

pts as (
    select
        p.activity_id,
        p.point_ts,
        p.watts,
        -- 경과시간 기준 4분할 (샘플 수가 아니라 시간으로 — 정지 구간 때문)
        ntile(4) over (partition by p.activity_id order by p.point_ts) as quarter
    from {{ ref('stg_power_points') }} p
    join long_rides l using (activity_id)
),

rolling as (
    select
        activity_id,
        quarter,
        avg(watts) over (
            partition by activity_id, quarter
            order by point_ts
            range between interval 30 seconds preceding and current row
        ) as p30
    from pts
),

pedaling as (
    select
        activity_id,
        quarter,
        avg(case when watts > 0 then 1.0 else 0.0 end) as pedaling_share
    from pts
    group by activity_id, quarter
),

np_by_quarter as (
    select
        r.activity_id,
        r.quarter,
        case when max(d.pedaling_share) >= {{ var('dropout_pedaling_share') }}
             then pow(avg(pow(r.p30, 4)), 0.25) end as np,
        max(d.pedaling_share) < {{ var('dropout_pedaling_share') }} as dropped
    from rolling r
    join pedaling d using (activity_id, quarter)
    group by r.activity_id, r.quarter
),

pivoted as (
    select
        activity_id,
        max(case when quarter = 1 then np end) as np_q1,
        max(case when quarter = 2 then np end) as np_q2,
        max(case when quarter = 3 then np end) as np_q3,
        max(case when quarter = 4 then np end) as np_q4,
        bool_or(dropped)                         as power_dropout
    from np_by_quarter
    group by activity_id
)

select
    p.activity_id,
    a.start_date_key,
    a.year,
    a.name,
    a.gear_label,
    a.distance_km,
    a.moving_h,
    p.np_q1,
    p.np_q2,
    p.np_q3,
    p.np_q4,
    p.power_dropout,
    -- 끊긴 분할이 있으면 NP 가 NULL 이라 비율도 저절로 NULL 이 된다
    p.np_q4 / nullif(p.np_q1, 0)                        as durability_ratio,
    (p.np_q3 + p.np_q4) / nullif(p.np_q1 + p.np_q2, 0)  as second_half_ratio
from pivoted p
join {{ ref('stg_activities') }} a using (activity_id)

-- 파워 커브 — 구간별 최고 파워를 전 기간 / 최근 90일로.
--
-- 구간별 "언제 세운 기록인지" 를 함께 낸다. 4시간 최고가 2년 전 기록이면
-- 지금 실력이 아니라는 뜻이고, 최근 90일 값과 비교하면 그게 드러난다.

with base as (
    select * from {{ ref('int_power_curve_ride') }}
),

ranked as (
    select
        duration_s,
        'all_time' as window_label,
        best_watts,
        activity_id,
        start_date_key,
        name,
        row_number() over (partition by duration_s order by best_watts desc) as rn
    from base

    union all

    select
        duration_s,
        'recent_90d' as window_label,
        best_watts,
        activity_id,
        start_date_key,
        name,
        row_number() over (partition by duration_s order by best_watts desc) as rn
    from base
    where start_date_key >= date_add(current_date(), -90)
)

select
    duration_s,
    case
        when duration_s < 60   then concat(cast(duration_s as string), '초')
        when duration_s < 3600 then concat(cast(duration_s / 60 as int), '분')
        else concat(cast(duration_s / 3600 as int), '시간')
    end                                   as duration_label,
    window_label,
    best_watts,
    activity_id                           as best_activity_id,
    start_date_key                        as best_date,
    name                                  as best_ride_name
from ranked
where rn = 1

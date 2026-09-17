-- 중복으로 판정된 헤드유닛 동시기록을 여기서 걸러낸다.
-- 판정 자체는 Silver 가 했고(세션화 + elapsed_time 최장), 여기서는 필터만 한다.
-- 원본은 silver.activities 에 그대로 남아 있어 근거를 되짚을 수 있다.

select
    activity_id,
    name,
    type,
    sport,
    gear_id,
    gear_label,
    device_name,
    start_ts_utc,
    start_date_key,
    year,
    month,
    distance_km,
    moving_h,
    elapsed_h,
    moving_time,
    elapsed_time,
    elev_m,
    avg_speed_kmh,
    max_speed_kmh,
    average_speed,
    average_heartrate,
    max_heartrate,
    average_cadence,
    average_watts,
    weighted_average_watts,
    max_watts,
    kilojoules,
    calories,
    suffer_score,
    is_indoor,
    has_gps,
    commute,
    description

from {{ source('silver', 'activities') }}
where not is_duplicate

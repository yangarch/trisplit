-- 중복 활동의 포인트를 제외한 시계열.
-- watts 는 streams 소스에서 온 활동에만 있다 (GPX 는 파워를 담지 못한다).

select
    activity_id,
    sport,
    gear_label,
    is_indoor,
    point_ts,
    point_idx,
    lat,
    lon,
    ele_m,
    distance_m,
    speed_ms,
    heartrate,
    cadence,
    watts,
    temp_c,
    grade_pct,
    moving,
    source

from {{ source('silver', 'trackpoints') }}
where not is_duplicate_activity

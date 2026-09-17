-- 라이딩 1건 = 1행. 파워 지표를 **원본 스트림에서 직접 계산**한다.
--
-- 기존 파이프라인이 못 하던 것:
--   `overview.py` 는 Strava 가 계산해 준 `weighted_average_watts` 를 그대로 받아 쓴다.
--   per-point 파워가 저장되지 않아서 직접 계산할 방법이 없었다 (`gpx_writer.py:53`).
--   이제 Bronze 에 원본 스트림이 있으므로 NP 를 직접 계산하고, Strava 값과 대조할 수 있다.
--
-- NP (Normalized Power) 계산:
--   1. 파워의 30 초 이동평균
--   2. 그 값의 4 제곱 평균
--   3. 4 제곱근
--
--   이동평균 윈도우를 ROWS 가 아니라 **RANGE + INTERVAL** 로 잡는다.
--   스트림은 1Hz 지만 일시정지 구간에서 샘플이 빠진다. ROWS 29 preceding 으로 세면
--   정지 전후를 30 초로 착각한다. 시간 기준이면 실제 경과 시간대로 잡힌다.
--
-- IF  = NP / FTP
-- TSS = (moving_time_s × NP × IF) / (FTP × 3600) × 100
--
-- Strava 값과의 차이 — 측정해서 남긴다:
--   368 라이딩 대조. 상관 0.983, 평균절대오차 8.04W, 그런데 **편향이 +8.04W 로 MAE 와 같다** —
--   노이즈가 아니라 직접 계산값이 항상 높은 체계적 차이다.
--
--   원인을 좁힌 과정:
--     · 윈도우 정의(ROWS/RANGE, 첫 30초 포함/제외) 4가지 → 전부 +8.0~8.7W. 원인 아님.
--     · 샘플링 실측 → 1Hz 맞음 (1s 간격 131만 / 2s 이상 930건). 원인 아님.
--     · 평활 창 길이별 편향 → 20s +11.64 / 30s +8.04 / 40s +5.51 / 50s +3.62 / 60s +2.16.
--       **단조 감소** — Strava 가 30초보다 더 강하게 평활한다는 뜻이다.
--
--   결론: `weighted_average_watts` 는 Strava 의 자체 "Weighted Average Power" 이고
--   표준 NP 와 다른 평활을 쓴다. **창을 Strava 에 맞추지 않는다** — 30초는 Coggan 의 표준
--   NP 정의이고, 비공개 지표를 역산해 맞추면 값이 비표준이 되어 다른 도구와 비교할 수 없게 된다.
--   두 값을 다 남기고 np_diff 로 차이를 드러낸다.

with power_points as (
    select
        activity_id,
        point_ts,
        watts
    from {{ ref('stg_trackpoints') }}
    where sport = 'cycling'
      and watts is not null
),

rolling_30s as (
    select
        activity_id,
        avg(watts) over (
            partition by activity_id
            order by point_ts
            range between interval 30 seconds preceding and current row
        ) as p30
    from power_points
),

computed as (
    select
        activity_id,
        count(*)                  as power_samples,
        pow(avg(pow(p30, 4)), 0.25) as np_computed,
        avg(p30)                  as avg_p30
    from rolling_30s
    group by activity_id
),

ftp as (
    select ftp_watts from {{ ref('stg_ftp') }} where is_current
)

select
    a.activity_id,
    a.start_date_key,
    a.year,
    a.month,
    a.name,
    a.gear_label,
    a.is_indoor,
    a.distance_km,
    a.moving_h,
    a.moving_time,
    a.elev_m,
    a.average_watts                                             as avg_watts,
    a.weighted_average_watts                                    as np_strava,
    c.np_computed,
    c.power_samples,
    -- 직접 계산한 값과 Strava 값의 차이. 클수록 스트림 결손이나 정지 처리 차이를 의심한다.
    c.np_computed - a.weighted_average_watts                    as np_diff,
    f.ftp_watts,
    c.np_computed / f.ftp_watts                                 as intensity_factor,
    (a.moving_time * c.np_computed * (c.np_computed / f.ftp_watts))
        / (f.ftp_watts * 3600) * 100                            as tss,
    a.average_heartrate,
    a.max_heartrate,
    a.kilojoules

from {{ ref('stg_activities') }} a
join computed c on a.activity_id = c.activity_id
cross join ftp f
where a.sport = 'cycling'

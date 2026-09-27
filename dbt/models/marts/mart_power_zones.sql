-- 파워 존 분포 — 원본 watts 에서 직접 계산.
--
-- 라이덕도 존 분포를 주지만 그건 description 텍스트를 파싱한 값이고,
-- 라이덕이 처리한 라이딩만 있다. 여기서는 per-point watts 로 전 라이딩을 계산한다.
--
-- 존 경계는 Coggan 7존 (FTP 대비 %):
--   Z1 회복 <56 / Z2 지구력 56-75 / Z3 템포 76-90 / Z4 임계 91-105
--   Z5 VO2max 106-120 / Z6 무산소 121-150 / Z7 신경근 >150
--
-- 앵커 FTP 는 ftp-log.md 현재값을 쓴다 (CLAUDE.md 규칙).
-- ⚠️ 그 값이 바뀌면 이 마트 전체가 다시 계산돼야 한다 — 존은 FTP 상대값이므로.
--
-- 1Hz 샘플이라 샘플 수 ≈ 초. 정지 구간(watts=0)은 Z1 에 들어간다.

with ftp as (
    select ftp_watts from {{ ref('stg_ftp') }} where is_current
),

zoned as (
    select
        p.activity_id,
        case
            when p.watts < f.ftp_watts * 0.56 then 1
            when p.watts < f.ftp_watts * 0.76 then 2
            when p.watts < f.ftp_watts * 0.91 then 3
            when p.watts < f.ftp_watts * 1.06 then 4
            when p.watts < f.ftp_watts * 1.21 then 5
            when p.watts < f.ftp_watts * 1.51 then 6
            else 7
        end as zone
    from {{ ref('stg_power_points') }} p
    cross join ftp f
),

per_ride as (
    select activity_id, zone, count(*) as seconds
    from zoned
    group by activity_id, zone
)

select
    r.activity_id,
    a.start_date_key,
    a.year,
    a.month,
    a.name,
    a.is_indoor,
    r.zone,
    case r.zone
        when 1 then 'Z1 회복'   when 2 then 'Z2 지구력' when 3 then 'Z3 템포'
        when 4 then 'Z4 임계'   when 5 then 'Z5 VO2max' when 6 then 'Z6 무산소'
        else 'Z7 신경근'
    end                                                          as zone_label,
    r.seconds,
    r.seconds / 60.0                                             as minutes,
    100.0 * r.seconds / sum(r.seconds) over (partition by r.activity_id) as pct_of_ride
from per_ride r
join {{ ref('stg_activities') }} a using (activity_id)

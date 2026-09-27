{{ config(
    materialized='incremental',
    unique_key=['activity_id', 'duration_s'],
    incremental_strategy='merge',
) }}

-- 라이딩별 구간 최고 파워 (Mean Maximal Power).
--
-- 각 구간 길이 d 에 대해 "d 초 이동평균의 최댓값" 을 구한다.
-- 파워 커브·FTP 추정·존 분석이 모두 이 테이블을 재사용한다.
--
-- ⚠️ **불완전한 윈도우를 반드시 걸러내야 한다.**
--    이동평균 윈도우는 활동 시작 직후에는 d 초만큼 채워지지 않는다.
--    그대로 최댓값을 취하면 10분 라이딩에서 "20분 최고 파워" 가 나오고,
--    그 값은 사실 전체 평균일 뿐이다 — 커브 오른쪽이 통째로 과대평가된다.
--    윈도우 안 샘플 수가 d 의 90% 이상일 때만 유효로 본다
--    (1Hz 기준. 10% 여유는 일시정지로 빠진 샘플을 허용하기 위한 것).
--
-- 윈도우는 ROWS 가 아니라 RANGE + INTERVAL 로 잡는다 —
-- 정지 구간에서 샘플이 빠지므로 행 수로 세면 실제 경과 시간과 어긋난다.
--
-- **증분 모델이다.** 윈도우 14개 × 136만 행이라 전체 재계산에 191초가 걸린다
-- (맥북 M4. 4스레드 서버에서는 훨씬 길다). 지난 라이딩의 파워 커브는 바뀌지 않으므로
-- 신규 활동만 계산한다. 윈도우 연산 **전에** 소스를 걸러야 절감이 생긴다 —
-- 결과를 나중에 필터하면 계산은 이미 끝난 뒤다.

{% set durations = [5, 15, 30, 60, 120, 300, 480, 720, 1200, 1800, 3600, 7200, 10800, 14400] %}

with src as (
    select activity_id, point_ts, watts
    from {{ ref('stg_power_points') }}
    {% if is_incremental() %}
    -- 이미 계산된 활동은 제외. 재수집으로 스트림이 바뀐 활동은
    -- Bronze 에서 행이 교체되므로 여기서도 다시 계산하는 편이 맞지만,
    -- 그 경우는 activity_id 가 이미 있어 걸러진다 — 필요하면 --full-refresh.
    where activity_id not in (select distinct activity_id from {{ this }})
    {% endif %}
),

rolling as (
    select
        activity_id,
        {% for d in durations %}
        case
            when count(watts) over (
                     partition by activity_id order by point_ts
                     range between interval {{ d }} seconds preceding and current row
                 ) >= {{ (d * 0.9) | round(0, 'ceil') | int }}
            then avg(watts) over (
                     partition by activity_id order by point_ts
                     range between interval {{ d }} seconds preceding and current row
                 )
        end as p_{{ d }}{{ "," if not loop.last }}
        {% endfor %}
    from src
),

best as (
    select
        activity_id,
        {% for d in durations %}
        max(p_{{ d }}) as best_{{ d }}{{ "," if not loop.last }}
        {% endfor %}
    from rolling
    group by activity_id
),

-- 구간을 컬럼에서 행으로 (분석·조인이 쉬워진다)
unpivoted as (
    select
        activity_id,
        stack(
            {{ durations | length }}
            {%- for d in durations -%}
            , {{ d }}, best_{{ d }}
            {%- endfor %}
        ) as (duration_s, best_watts)
    from best
)

select
    u.activity_id,
    u.duration_s,
    u.best_watts,
    a.start_date_key,
    a.year,
    a.name,
    a.gear_label,
    a.is_indoor,
    a.distance_km,
    a.moving_h
from unpivoted u
join {{ ref('stg_activities') }} a using (activity_id)
where u.best_watts is not null

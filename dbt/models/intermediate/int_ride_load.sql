{{ config(
    materialized='incremental',
    unique_key='activity_id',
    incremental_strategy='merge',
) }}

-- 라이딩별 "무릎이 싫어하는 부하" — 증상 로그와 비교하기 위한 지표.
--
-- 무릎 내측 건염 노트에 반복해 나오는 가설을 숫자로 옮긴다:
--   "클라임 고토크에서 자극", "케이던스 90+ / 토크 최소면 괜찮다".
-- 토크 ∝ 파워 / 케이던스 이므로, 같은 파워를 **낮은 케이던스로** 낼수록 무릎 부하가 크다.
--
--   grind_s        케이던스 < grind_cadence_max 이면서 파워 ≥ FTP × grind_ftp_pct%  인 초
--   climb_s        경사 ≥ climb_grade_min% 인 이동 중 초
--   grind_climb_s  둘 다 — 가설이 가장 직접적으로 가리키는 구간
--   climb_cadence  클라임 중 평균 케이던스 (페달링 중인 샘플만)
--
-- 1Hz 스트림이라 샘플 수 = 초. 파워가 있는 라이딩만 (GPX 는 파워를 담지 못한다).
-- FTP 는 ftp-log 현재값 — 마트 전체와 같은 앵커를 쓴다. 앵커가 바뀌면 --full-refresh.
--
-- 증분: 라이딩이 끝난 뒤 스트림은 바뀌지 않는다. 170만 행을 매번 다시 훑을 이유가 없다.

with ftp as (
    select ftp_watts from {{ ref('stg_ftp') }} where is_current
),

pts as (
    select t.activity_id, t.watts, t.cadence, t.grade_pct, t.moving
    from {{ ref('stg_trackpoints') }} t
    where t.sport = 'cycling'
      and t.watts is not null
    {% if is_incremental() %}
      and t.activity_id not in (select activity_id from {{ this }})
    {% endif %}
)

select
    p.activity_id,
    f.ftp_watts                                                            as ftp_anchor,
    count(*)                                                               as power_samples,
    sum(case when p.cadence > 0 and p.cadence < {{ var('grind_cadence_max') }}
              and p.watts >= f.ftp_watts * {{ var('grind_ftp_pct') }} / 100.0
             then 1 else 0 end)                                            as grind_s,
    sum(case when p.grade_pct >= {{ var('climb_grade_min') }}
              and coalesce(p.moving, true)
             then 1 else 0 end)                                            as climb_s,
    sum(case when p.grade_pct >= {{ var('climb_grade_min') }}
              and p.cadence > 0 and p.cadence < {{ var('grind_cadence_max') }}
              and p.watts >= f.ftp_watts * {{ var('grind_ftp_pct') }} / 100.0
             then 1 else 0 end)                                            as grind_climb_s,
    avg(case when p.grade_pct >= {{ var('climb_grade_min') }} and p.cadence > 0
             then p.cadence end)                                           as climb_cadence,
    avg(case when p.cadence > 0 then p.cadence end)                        as pedaling_cadence
from pts p
cross join ftp f
group by p.activity_id, f.ftp_watts

-- 증상 로그 (body/symptoms/log.md → seed symptom_log). 한 부위 한 줄.
--
-- activity 칸은 같은 날 같은 종목이 둘 이상일 때만 사람이 적는다. 비어 있으면
-- 날짜(현지) + 종목으로 활동을 찾아 붙인다. 거리 0 인 빈 기록(워치 오작동 등)은 후보에서 뺀다.
--   link_method: explicit(사람이 적음) / date_sport(자동, 후보 1건) / ambiguous(후보 여럿) / unmatched
-- ambiguous·unmatched 는 assert_symptoms_linked 가 경고한다 — 기록했는데 분석에 안 붙는 상태.
--
-- 행 키는 내용 해시다. row_number 는 CTE 가 여러 번 평가될 때 번호가 달라질 수 있어
-- (Spark 는 CTE 를 참조마다 재계산할 수 있다) 조인 키로 쓰면 안 된다.

with raw as (
    -- 문자열 칸의 빈 값은 seed 가 'None' 으로 싣는다 → 여기서 NULL 로 (macros/blank_to_null.sql)
    select
        symptom_date,
        sport,
        activity_id,
        area,
        {{ blank_to_null('side') }}         as side,
        severity,
        {{ blank_to_null('onset') }}        as onset,
        {{ blank_to_null('course') }}       as course,
        next_day,
        {{ blank_to_null('confounders') }}  as confounders,
        {{ blank_to_null('memo') }}         as memo
    from {{ ref('symptom_log') }}
),

s as (
    select
        md5(concat_ws('|',
            cast(symptom_date as string), sport,
            coalesce(cast(activity_id as string), ''), area,
            coalesce(side, ''), coalesce(memo, '')
        ))                                  as symptom_key,
        *
    from raw
),

candidates as (
    select s.symptom_key, a.activity_id
    from s
    join {{ ref('stg_activities') }} a
      on a.start_date_key = s.symptom_date
     and a.sport = s.sport
     and a.distance_km > 0
    where s.activity_id is null
),

resolved as (
    select
        symptom_key,
        case when count(*) = 1 then max(activity_id) end as activity_id,
        count(*)                                           as n_candidates
    from candidates
    group by symptom_key
)

select
    s.symptom_key,
    s.symptom_date,
    s.sport,
    coalesce(s.activity_id, r.activity_id)  as activity_id,
    case
        when s.activity_id is not null then 'explicit'
        when r.n_candidates = 1        then 'date_sport'
        when r.n_candidates > 1        then 'ambiguous'
        else                                'unmatched'
    end                                     as link_method,
    s.area,
    s.side,
    s.severity,
    s.onset,
    s.course,
    s.next_day,
    s.confounders,
    s.memo
from s
left join resolved r on s.symptom_key = r.symptom_key

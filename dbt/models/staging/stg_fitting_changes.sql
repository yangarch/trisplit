-- 피팅 변경 이력 (equipment/bikes/fitting/changes.md → seed fitting_changes).
--
-- 적용된 것만 — 계획은 구간을 나누지 않는다. 적용일이 없는 행(시점 미상)은 시드 단계에서 이미 빠졌다.
-- effective_date = "이날 라이딩부터 이 세팅". target: chichi · kiki (자전거) / 신발 (모든 자전거).

with raw as (
    -- 문자열 칸의 빈 값은 seed 가 'None' 으로 싣는다 → NULL 로 (macros/blank_to_null.sql)
    select
        effective_date,
        target,
        item,
        {{ blank_to_null('side') }}    as side,
        {{ blank_to_null('before') }}  as before,
        {{ blank_to_null('after') }}   as after,
        status,
        {{ blank_to_null('source') }}  as source,
        {{ blank_to_null('memo') }}    as memo
    from {{ ref('fitting_changes') }}
)

select
    *,
    -- 구간 이름표. 예: "바높이 −20mm", "클릿각도(좌) Look 회색 4.5°"
    concat(item, case when side is not null then concat('(', side, ')') else '' end,
           ' ', coalesce(after, ''))        as change_label
from raw
where status in ('적용', '원복')

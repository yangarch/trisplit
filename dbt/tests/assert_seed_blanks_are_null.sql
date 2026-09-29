{#
  시드의 빈 칸이 문자열 'None' 으로 새지 않았는지.

  dbt-spark(session) seed 는 빈 문자열 칸을 'None' 으로 싣는다. staging 이 blank_to_null 로
  막는데, 새 칸을 추가하면서 빠뜨리면 다시 샌다. 처음 겪었을 때 다른 테스트 11개는 전부
  통과했고 이름표에 "(None)" 이 찍힌 것을 눈으로 찾았다 — 값을 직접 보는 테스트가 필요하다.
#}

select 'stg_symptoms' as model, symptom_date as day, area as what
from {{ ref('stg_symptoms') }}
where 'None' in (side, onset, course, confounders, memo)

union all

select 'stg_fitting_changes', effective_date, change_label
from {{ ref('stg_fitting_changes') }}
where 'None' in (side, before, after, source, memo)
   or change_label like '%None%'

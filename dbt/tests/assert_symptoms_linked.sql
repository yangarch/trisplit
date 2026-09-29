{{ config(severity='warn') }}

{#
  증상 로그의 모든 행이 활동에 붙었는지.

  activity 칸을 비워 두면 날짜+종목으로 자동 연결되는데, 그날 같은 종목이 둘이면(ambiguous)
  또는 그날 활동이 없으면(unmatched — 날짜 오타, 아직 수집 전) 분석에서 조용히 빠진다.
  "기록했는데 분석에 안 나오는" 상태를 경고로 드러낸다.

  severity=warn: 운동 직후 기록하고 수집이 아직 안 됐을 수 있다. 다음 실행에서 풀린다.
#}

select symptom_date, sport, area, link_method, memo
from {{ ref('stg_symptoms') }}
where link_method in ('ambiguous', 'unmatched')

{#
  시드의 빈 문자열 칸을 NULL 로.

  dbt-spark(method: session) 의 seed 는 CSV 의 빈 칸을 Python None 으로 읽은 뒤
  **문자열 'None' 으로 적재한다** (문자열 컬럼만. 숫자·날짜 컬럼은 NULL 로 들어간다).
  그대로 두면 `side is not null` 이 참이 되어 "안장높이(None) −2mm" 같은 이름표가 생기고,
  collect_list 로 모은 칸에 'None' 이 섞인다. 테스트는 전부 통과했다 — 값만 틀렸다.
  시드를 읽는 staging 에서 문자열 칸마다 이 매크로를 거친다.
#}
{% macro blank_to_null(col) -%}
    case when trim({{ col }}) in ('', 'None') then null else {{ col }} end
{%- endmacro %}

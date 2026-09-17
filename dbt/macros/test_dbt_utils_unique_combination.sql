{#
  여러 컬럼의 조합이 유일한지 검사. (연도 × 종목처럼 복합키인 마트용)
#}
{% test dbt_utils_unique_combination(model, combination_of_columns) %}

{%- set cols = combination_of_columns | join(', ') -%}

select {{ cols }}, count(*) as n
from {{ model }}
group by {{ cols }}
having count(*) > 1

{% endtest %}

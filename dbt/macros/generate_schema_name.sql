{#
  dbt 기본 동작은 target.schema 에 custom schema 를 덧붙인다 (gold + gold = gold_gold).
  여기서는 custom schema 를 그대로 쓴다 — 마트는 tc.gold.* 로 떨어져야 한다.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

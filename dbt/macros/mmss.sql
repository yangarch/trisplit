{#
  초 → "m:ss" 문자열. NULL 이면 NULL. (수영 100m 페이스 표기)
#}
{% macro mmss(sec) -%}
    case when {{ sec }} is not null then concat(
        cast(floor({{ sec }} / 60) as int), ':',
        lpad(cast(cast({{ sec }} % 60 as int) as string), 2, '0')) end
{%- endmacro %}

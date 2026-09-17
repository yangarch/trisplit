{#
  값이 범위 안에 있는지 검사하는 제너릭 테스트.
  dbt_utils 패키지를 쓰면 되지만, 의존성 하나 때문에 packages.yml 을 두기보다
  직접 정의했다 (이 프로젝트에서 필요한 건 이거 하나다).

  NULL 은 통과시킨다 — 결측 여부는 not_null 이 따로 본다.
  두 테스트를 섞으면 실패 원인이 흐려진다.
#}
{% test between(model, column_name, min_value, max_value) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})

{% endtest %}

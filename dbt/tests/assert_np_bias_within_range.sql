{#
  NP 직접계산의 회귀 감지.

  3단계에서 Strava 값 대비 편향을 +8.04W 로 **측정했다**.
  평활 창이나 4제곱 평균을 잘못 건드리면 이 값이 크게 움직인다 —
  20초 창이면 +11.6, 60초면 +2.2 로 바뀌는 것을 확인했다.

  편향 자체를 없애려는 테스트가 아니다. Strava 값은 자체 평활을 쓰는 다른 지표라
  0 으로 맞추는 게 오히려 틀렸다. **측정된 범위를 벗어나는지**만 본다.

  상한은 `np_bias_max_w` 변수 (기본 15W, 측정값 8.04W 대비 여유).
#}

select
    count(*)                                        as rides,
    round(avg(np_computed - np_strava), 2)          as bias_w,
    {{ var('np_bias_max_w') }}                      as allowed_bias_w
from {{ ref('mart_ride_power') }}
where np_strava is not null
having abs(avg(np_computed - np_strava)) > {{ var('np_bias_max_w') }}

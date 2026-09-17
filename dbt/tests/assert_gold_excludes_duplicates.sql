{#
  Gold 마트가 중복 활동을 실제로 빼고 집계하는지 검증.

  중복 판정(Silver)과 필터(staging)가 따로 있어서, 한쪽만 고치면 조용히 어긋난다.
  종목별 거리 합이 silver 의 비중복 합과 1km 이상 벌어지면 실패한다.

  이게 없으면 2단계에서 잡은 412.9km 이중 계상이 리팩터링 한 번에 되살아날 수 있다.
#}

with gold as (
    select sport, sum(distance_km) as km
    from {{ ref('mart_sport_totals') }}
    group by sport
),

expected as (
    select sport, sum(distance_km) as km
    from {{ source('silver', 'activities') }}
    where not is_duplicate
    group by sport
)

select
    coalesce(g.sport, e.sport) as sport,
    g.km as gold_km,
    e.km as expected_km,
    abs(coalesce(g.km, 0) - coalesce(e.km, 0)) as diff_km
from gold g
full outer join expected e on g.sport = e.sport
where abs(coalesce(g.km, 0) - coalesce(e.km, 0)) > 1.0

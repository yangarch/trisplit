{{ config(severity='warn') }}

{#
  신선도 — 최신 활동이 오래되면 경고.

  launchd 가 매일 22:00 에 동기화를 돌리는데, 토큰 만료나 API 한도로 조용히 멈출 수 있다.
  파이프라인은 "성공" 이라고 보고하면서 낡은 데이터를 계속 내보내는 상태가 된다.

  severity=warn 인 이유: 며칠 쉬면 자연히 오래된다. 사고가 아니라 신호다.
#}

select
    max(start_date_key)                                     as latest_activity,
    datediff(current_date(), max(start_date_key))           as days_stale,
    {{ var('freshness_max_days') }}                         as max_allowed_days
from {{ ref('stg_activities') }}
having datediff(current_date(), max(start_date_key)) > {{ var('freshness_max_days') }}

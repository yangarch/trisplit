{#
  "오늘" — 한국 시각 기준 날짜.

  컨테이너와 Spark 세션은 UTC 다. current_date() 를 그대로 쓰면 KST 00~09시에 도는 실행
  (웹훅으로 아침 운동을 올린 직후 등)에서 날짜가 하루 밀려, 오늘 운동이 "어제" 로 집계된다.
  세션 시간대를 통째로 바꾸면 기존 타임스탬프 → 날짜 변환이 달라질 수 있어, 필요한 곳에서만 명시한다.
  (활동 날짜 start_date_key 는 Strava 의 현지 시각에서 온 것이라 이 문제와 무관하다.)
#}
{% macro today_kst() -%}
    to_date(from_utc_timestamp(current_timestamp(), 'Asia/Seoul'))
{%- endmacro %}

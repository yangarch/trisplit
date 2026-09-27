-- 파워가 있는 사이클 시계열. 파워 분석 전체의 입력.
--
-- watts 는 streams 소스에서 온 활동에만 있다 (GPX 는 파워를 담지 못한다).
-- 중복 판정된 활동은 stg_trackpoints 에서 이미 빠져 있다.

select
    activity_id,
    point_ts,
    watts
from {{ ref('stg_trackpoints') }}
where sport = 'cycling'
  and watts is not null

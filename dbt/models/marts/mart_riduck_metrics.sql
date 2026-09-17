-- overview.py 「3. 사이클 디테일 > 라이덕 지표」 이식.
--
-- 라이덕은 Strava description 에 훈련부하(체력/피로/균형)·R파워·훈련량·피크파워를 써넣는다.
-- overview.py 는 이걸 Python 정규식으로 파싱했다 (`reports/_scripts/riduck.py`).
-- 여기서는 dbt 안에서 파싱해(stg_riduck) 마트로 만든다 —
-- 파싱 로직이 이걸 쓰는 지표와 같은 곳에 있고, 4단계에서 dbt test 로 검증할 수 있다.
--
-- ⚠️ 라이덕의 추정 FTP(implied_ftp)는 ftp-log.md 의 값과 다르다.
--    CLAUDE.md 규칙상 분석 앵커는 ftp-log.md 이고, 이 값은 대조용이다.
--
-- ⚠️ implied_ftp 는 riduck.py 와 값이 1 건 다르다 (252 vs 253).
--    R파워 202 / 강도 0.8 = 252.5 의 반올림 차이다 —
--    Python round() 는 banker's rounding(짝수로), SQL round() 는 반올림 올림.
--    둘 다 맞고 훈련 지표로서 의미 없는 차이라 SQL 기본 동작을 따른다.

select
    activity_id,
    start_date_key,
    year,
    month,
    sport,
    gear_label,
    distance_km,
    moving_h,

    fitness,
    fatigue,
    form,

    r_power,
    intensity,
    load,
    recovery_h,
    training_type,
    implied_ftp,

    peak_5min_w,
    peak_20min_w,
    peak_40min_w,
    peak_1h_w

from {{ ref('stg_riduck') }}

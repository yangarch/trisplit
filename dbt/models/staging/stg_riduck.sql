-- 라이덕(Riduck) 이 Strava 활동 **description** 에 써넣는 분석 텍스트를 구조화한다.
-- (코멘트가 아니라 description 이다 — `reports/_scripts/riduck.py` 와 같은 소스)
--
-- 원본 형식:
--   🌳 체력 50, 피로 153, 균형 -103
--   📊 R파워 173, 강도 0.71, 훈련량 749
--   ✨ 회복시간 172 시간
--   ⚡ 20분 246w (100%) 🏆 PR
--
-- 정규식은 **raw string 리터럴(r'...')** 로 쓴다.
-- 일반 문자열이면 Spark SQL 이 백슬래시를 이스케이프로 먹어 \d 가 d 가 된다 —
-- Silver 적재 때 이것 때문에 컬럼 하나가 통째로 NULL 이 된 적이 있다.

with src as (
    select
        activity_id,
        start_date_key,
        year,
        month,
        sport,
        gear_label,
        distance_km,
        moving_h,
        description
    from {{ ref('stg_activities') }}
    where description is not null
      -- riduck.py 의 is_riduck() 과 정확히 같은 판정:
      --   (훈련상태 and R파워) or 'riduck' in desc.lower()
      -- 뒤쪽 or 조건을 빠뜨렸다가 라이딩 9건을 놓친 적이 있다.
      and (
            (description like '%훈련상태%' and description like '%R파워%')
            or lower(description) like '%riduck%'
      )
      -- 종목을 사이클로 제한하지 않는다. 라이덕은 러닝/수영에도 기록을 남기고(12건),
      -- 버리면 정보 손실이다. overview.py 와 맞추려면 소비하는 쪽에서 sport 로 거르면 된다.
),

parsed as (
    select
        activity_id,
        start_date_key,
        year,
        month,
        sport,
        gear_label,
        distance_km,
        moving_h,

        cast(nullif(regexp_extract(description, r'체력\s*(-?\d+)', 1), '') as int)   as fitness,
        cast(nullif(regexp_extract(description, r'피로\s*(-?\d+)', 1), '') as int)   as fatigue,
        cast(nullif(regexp_extract(description, r'균형\s*(-?\d+)', 1), '') as int)   as form,

        cast(nullif(regexp_extract(description, r'R파워\s*(\d+)', 1), '') as int)      as r_power,
        cast(nullif(regexp_extract(description, r'강도\s*([\d.]+)', 1), '') as double) as intensity,
        cast(nullif(regexp_extract(description, r'훈련량\s*(\d+)', 1), '') as int)     as load,
        cast(nullif(regexp_extract(description, r'회복시간\s*(\d+)\s*시간', 1), '') as int) as recovery_h,

        nullif(regexp_extract(description, r'([가-힣A-Za-z][가-힣A-Za-z ]*훈련)\s*\(([^)]*)\)', 0), '') as training_type,

        cast(nullif(regexp_extract(description, r'5분\s+(\d+)w', 1), '') as int)    as peak_5min_w,
        cast(nullif(regexp_extract(description, r'20분\s+(\d+)w', 1), '') as int)   as peak_20min_w,
        cast(nullif(regexp_extract(description, r'40분\s+(\d+)w', 1), '') as int)   as peak_40min_w,
        cast(nullif(regexp_extract(description, r'1시간\s+(\d+)w', 1), '') as int)  as peak_1h_w
    from src
)

select
    *,
    -- riduck.py 의 implied_ftp(): R파워 / 강도 로 라이덕 내부 FTP 를 역산한다.
    case when r_power is not null and intensity > 0
         then cast(round(r_power / intensity) as int)
    end as implied_ftp
from parsed

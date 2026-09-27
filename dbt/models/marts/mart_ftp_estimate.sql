-- FTP 추정 — 두 방법으로 계산하고 ftp-log.md 의 현재값과 대조한다.
--
-- 왜 필요한가:
--   `activities/cycling/ftp-log.md` 의 현재값 283W 는 "라이덕 자동추정, 신뢰도 △" 로
--   기록돼 있고 실측 테스트를 미뤄 두고 있다. 기존 데이터로 추정치를 뽑으면
--   테스트 없이 신뢰 구간을 좁힐 수 있다.
--
-- 방법 1 — 20분 × 0.95 (클래식)
--   가장 널리 쓰이는 근사. 20분 최대 노력 기록이 있을 때 유효하다.
--
-- 방법 2 — CP (Critical Power) 모델
--   P(t) = W′/t + CP  →  P 를 1/t 에 대해 선형회귀하면 기울기가 W′, 절편이 CP.
--   모델이 잘 맞는 2~20분 구간만 쓴다 (그 밖은 무산소/유산소 기여가 달라져 어긋난다).
--   CP 는 통상 FTP 보다 약간 높게 나온다.
--
-- ⚠️ **두 방법 모두 "최대 노력 기록이 데이터에 있다" 를 전제한다.**
--    테스트를 한 적이 없고 평소 라이딩만 있으면, 구간 최고값은 실제 능력보다 낮다.
--    그래서 이 값들은 **하한(lower bound)** 으로 읽어야 한다 —
--    "최소 이 정도는 된다" 이지 "이것이 FTP" 가 아니다.
--    판단은 ftp-log.md 가 앵커라는 CLAUDE.md 규칙을 따른다.

{% set cp_durations = [120, 300, 480, 720, 1200] %}

with curve as (
    select duration_s, window_label, best_watts, best_date
    from {{ ref('mart_power_curve') }}
),

-- 방법 1
classic as (
    select
        window_label,
        best_watts                as p20,
        best_watts * 0.95         as ftp_20min_x095,
        best_date                 as p20_date
    from curve
    where duration_s = 1200
),

-- 방법 2: 1/t 에 대한 P 의 선형회귀
cp_points as (
    select
        window_label,
        1.0 / duration_s as x,
        best_watts       as y
    from curve
    where duration_s in ({{ cp_durations | join(', ') }})
),

cp_fit as (
    select
        window_label,
        count(*)                                            as n_points,
        -- 기울기 = W′ (무산소 용량, 줄), 절편 = CP (와트)
        (count(*) * sum(x * y) - sum(x) * sum(y))
            / nullif(count(*) * sum(x * x) - sum(x) * sum(x), 0) as w_prime,
        (sum(y) * sum(x * x) - sum(x) * sum(x * y))
            / nullif(count(*) * sum(x * x) - sum(x) * sum(x), 0) as cp
    from cp_points
    group by window_label
    having count(*) >= 3          -- 점이 3개 미만이면 회귀가 무의미
),

ftp_log as (
    select ftp_watts as ftp_log_watts from {{ ref('stg_ftp') }} where is_current
)

select
    c.window_label,
    c.p20,
    c.p20_date,
    c.ftp_20min_x095,
    f.cp                                            as cp_watts,
    f.w_prime                                       as w_prime_joules,
    f.n_points                                      as cp_fit_points,
    l.ftp_log_watts,
    c.ftp_20min_x095 - l.ftp_log_watts              as diff_20min_vs_log,
    f.cp - l.ftp_log_watts                          as diff_cp_vs_log,
    -- 두 방법이 서로 크게 어긋나면 데이터에 최대 노력이 없다는 신호다
    abs(c.ftp_20min_x095 - f.cp)                    as method_spread
from classic c
left join cp_fit f using (window_label)
cross join ftp_log l

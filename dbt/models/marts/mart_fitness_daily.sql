-- 일별 훈련부하 — intervals.icu 의 CTL(체력, 42일 지수평균) · ATL(피로, 7일 지수평균).
-- 균형(form) = CTL − ATL. intervals.icu 화면의 "Form" 과 같은 정의이고, 라이덕의 "균형" 과 같은 개념이다.
--
-- 숫자 척도는 라이덕과 다르다 — 부하 계산(파워 기반 TSS·심박 등)이 서비스마다 달라
-- 9/29 이전 라이덕 값과 이어 붙여 추세로 읽으면 안 된다.

select
    fitness_date,
    ctl,
    atl,
    round(ctl - atl, 1) as form,
    ramp_rate,
    load,
    ride_eftp
from {{ ref('stg_fitness') }}

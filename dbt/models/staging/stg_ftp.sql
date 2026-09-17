-- 분석 앵커 FTP. overview.py 는 ftp-log.md 의 "현재값" 하나를 전 기간에 적용한다.
-- 6단계 패리티 검증을 위해 같은 규칙을 쓴다.
-- (이력 기반으로 시점별 FTP 를 적용하는 쪽이 더 정확하지만, 그건 패리티 확인 후에.)

select
    effective_date,
    ftp_watts,
    w_per_kg,
    method,
    confidence,
    is_current
from {{ ref('ftp_log') }}

"""
실패 알림 — ntfy 로 폰에 푸시한다.

맥 launchd 시절엔 레이크하우스가 실패하면 옛 overview.py 로 폴백했다. 홈서버로 옮기며
폴백을 없앴으므로(중복 413km 를 경고 없이 내던 폴백이었다), 실패하면 리포트가 전날 것
그대로 남는다 — 알리지 않으면 "실패했는데 멀쩡해 보이는" 상태가 된다.

태스크 콜백이라 retries 를 다 쓴 **최종 실패** 에만 울린다 (재시도 전 실패에는 안 울림).

토픽은 compose 가 내보내는 NTFY_TOPIC. **호출 시점에** 읽는다 — DAG 파싱 시점에 읽으면
직렬화된 DAG 에 값이 굳는다 (DAG 파일 상단 LAKEHOUSE 주석과 같은 이유).
비어 있으면 조용히 건너뛴다 — 알림 설정이 없다고 파이프라인이 죽으면 안 된다.

ntfy.sh 는 공개 서버다. 토픽을 아는 사람은 누구나 구독할 수 있으므로
메시지에는 태스크 이름만 넣고 데이터(거리·파워 등)는 넣지 않는다.

한글 제목은 헤더(Title:)로 보내면 urllib 가 latin-1 로 인코딩하다 UnicodeEncodeError 를 낸다
→ JSON 본문 발행(POST https://ntfy.sh/, topic 필드)을 쓴다.
"""
from __future__ import annotations

import json
import os
import urllib.request

NTFY_URL = "https://ntfy.sh/"


def notify_failure(context) -> None:
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        print("[ntfy] NTFY_TOPIC 없음 — 알림 건너뜀")
        return

    ti = context["ti"]
    body = {
        "topic": topic,
        "title": f"파이프라인 실패: {ti.task_id}",
        "message": f"{ti.dag_id}\n{ti.run_id}\n시도 {ti.try_number}회 — Airflow 로그 확인",
        "priority": 4,
        "tags": ["warning"],
    }
    req = urllib.request.Request(
        NTFY_URL,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        print(f"[ntfy] 실패 알림 전송: {ti.task_id}")
    except Exception as e:  # 알림 실패가 태스크 결과를 바꾸면 안 된다
        print(f"[ntfy] 전송 실패: {e}")

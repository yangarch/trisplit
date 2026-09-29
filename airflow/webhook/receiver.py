"""
Strava 웹훅 수신기 — 업로드 이벤트를 모아서 Airflow 의 strava_event DAG 를 트리거한다.

경로:
  Strava ─HTTPS─▶ 메인 서버 nginx ─(mirror 로 복제)─▶ 이 수신기 (LAN)
  Strava 앱의 웹훅 구독은 앱당 하나라 이미 다른 서비스가 쓰고 있다. 그 서비스의 nginx
  location 에 mirror 를 걸어 이벤트를 **복사** 받는다 — 원래 서비스의 응답에는 영향이 없다
  (mirror 서브요청의 응답은 버려진다). 그래서 구독 검증(GET hub.challenge)도 원래 서비스가 한다.
  여기의 GET 처리는 구독을 직접 가질 때를 위한 것이다.

거르는 것 (공유 앱이라 다른 사용자의 이벤트도 들어온다):
  - owner_id 가 내 athlete_id 가 아님
  - subscription_id 가 설정값과 다름 (설정했을 때만)
  - object_type 이 activity 가 아님 / aspect_type 이 create·update 가 아님 (delete 는 야간 보정 몫)

Strava 는 이벤트에 서명을 붙이지 않는다. 주소를 알면 누구나 가짜 이벤트를 보낼 수 있지만,
여기서는 "확인해 보라" 는 신호로만 쓴다 — 실제 데이터는 Strava API 에서 다시 받는다.
가짜 이벤트가 할 수 있는 최악은 파이프라인을 한 번 더 돌리는 것이고, 그것도 debounce 가 묶는다.

debounce:
  여러 건을 연달아 올리면(예: 하루 라이딩을 5개로 나눠 저장) 이벤트도 연달아 온다.
  마지막 이벤트 뒤 DEBOUNCE_S 동안 조용하면 모아 둔 id 를 한 번에 보낸다.
  계속 들어와도 첫 이벤트로부터 MAX_WAIT_S 가 지나면 보낸다.

Strava 는 2초 안에 200 을 요구한다 → 응답을 먼저 보내고 트리거는 별도 스레드에서.
표준 라이브러리만 사용.
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PATH = "/strava/webhook"
PORT = int(os.environ.get("PORT", "8091"))

ATHLETE_ID = int(os.environ["STRAVA_ATHLETE_ID"])  # 필수 — 없으면 남의 이벤트를 거를 수 없다
SUBSCRIPTION_ID = int(os.environ.get("STRAVA_SUBSCRIPTION_ID") or 0)
VERIFY_TOKEN = os.environ.get("STRAVA_VERIFY_TOKEN", "")

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://apiserver:8080")
AIRFLOW_USER = os.environ["AIRFLOW_USER"]
AIRFLOW_PASSWORD = os.environ["AIRFLOW_PASSWORD"]
TARGET_DAG = "strava_event"

DEBOUNCE_S = int(os.environ.get("DEBOUNCE_S", "90"))
MAX_WAIT_S = int(os.environ.get("MAX_WAIT_S", "300"))
MAX_PENDING = 50  # 이상한 폭주에 대비한 상한

_lock = threading.Lock()
_pending: set[int] = set()
_first_at = 0.0
_last_at = 0.0


def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


# ---- Airflow ----

def _airflow_token() -> str:
    req = urllib.request.Request(
        f"{AIRFLOW_URL}/auth/token",
        data=json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())["access_token"]


def trigger(ids: list[int]) -> None:
    body = {"logical_date": None, "conf": {"activity_ids": ids}}
    req = urllib.request.Request(
        f"{AIRFLOW_URL}/api/v2/dags/{TARGET_DAG}/dagRuns",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {_airflow_token()}"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        run = json.loads(r.read())
    log(f"[trigger] {TARGET_DAG} {run.get('dag_run_id')} ids={ids}")


# ---- debounce ----

def flusher() -> None:
    global _first_at
    while True:
        time.sleep(5)
        with _lock:
            if not _pending:
                continue
            now = time.time()
            quiet = now - _last_at >= DEBOUNCE_S
            overdue = now - _first_at >= MAX_WAIT_S
            if not (quiet or overdue):
                continue
            ids = sorted(_pending)
            _pending.clear()
            _first_at = 0.0
        try:
            trigger(ids)
        except (urllib.error.URLError, OSError, KeyError, ValueError) as e:
            # 놓쳐도 22:00 보정 실행이 받는다. 다음 이벤트 때 같이 가도록 되돌려 둔다.
            log(f"[!] 트리거 실패: {e} — ids 를 대기열로 되돌림")
            with _lock:
                _pending.update(ids)
                _first_at = _first_at or time.time()


def enqueue(activity_id: int) -> None:
    global _first_at, _last_at
    with _lock:
        if len(_pending) >= MAX_PENDING:
            log(f"[!] 대기열 상한 {MAX_PENDING} — {activity_id} 버림 (야간 보정이 받는다)")
            return
        now = time.time()
        _pending.add(activity_id)
        _first_at = _first_at or now
        _last_at = now


def accept(ev: dict) -> str | None:
    """받을 이벤트면 None, 아니면 거른 이유."""
    if ev.get("owner_id") != ATHLETE_ID:
        return "다른 사용자"
    if SUBSCRIPTION_ID and ev.get("subscription_id") != SUBSCRIPTION_ID:
        return "구독 불일치"
    if ev.get("object_type") != "activity":
        return f"object_type={ev.get('object_type')}"
    if ev.get("aspect_type") not in ("create", "update"):
        return f"aspect_type={ev.get('aspect_type')}"
    if not isinstance(ev.get("object_id"), int):
        return "object_id 없음"
    return None


# ---- HTTP ----

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: dict | None = None) -> None:
        data = json.dumps(body or {}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        if u.path == "/healthz":
            with _lock:
                return self._send(200, {"ok": True, "pending": len(_pending)})
        if u.path != PATH:
            return self._send(404)
        # 구독 검증 — 구독을 직접 가질 때만 쓰인다 (mirror 구성에서는 원래 서비스가 응답)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if VERIFY_TOKEN and q.get("hub.mode") == "subscribe" and q.get("hub.verify_token") == VERIFY_TOKEN:
            return self._send(200, {"hub.challenge": q.get("hub.challenge", "")})
        return self._send(403)

    def do_POST(self):  # noqa: N802
        if urlparse(self.path).path != PATH:
            return self._send(404)
        try:
            n = min(int(self.headers.get("Content-Length") or 0), 64 * 1024)
            ev = json.loads(self.rfile.read(n) or b"{}")
        except (ValueError, OSError):
            return self._send(400)
        # 먼저 응답한다 (Strava 2초 제한). 판단·적재는 그 뒤.
        self._send(200)
        why = accept(ev)
        if why:
            log(f"[skip] {why}: {ev.get('aspect_type')} {ev.get('object_type')} {ev.get('object_id')}")
            return
        log(f"[event] {ev['aspect_type']} activity {ev['object_id']} updates={ev.get('updates')}")
        enqueue(ev["object_id"])

    def log_message(self, *_):  # 기본 접근 로그 대신 위의 log() 만 쓴다
        pass


if __name__ == "__main__":
    threading.Thread(target=flusher, daemon=True).start()
    log(f"[start] :{PORT}{PATH} athlete={ATHLETE_ID} debounce={DEBOUNCE_S}s max_wait={MAX_WAIT_S}s")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

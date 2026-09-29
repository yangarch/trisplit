#!/usr/bin/env python3
"""
training-companion Strava OAuth — 1회용 인증 스크립트.

사용법:
  1) config.example.json 을 $TC_ROOT/activities/strava/api/config.json 으로 복사
  2) Strava 앱의 client_id / client_secret 를 config.json 에 넣기
  3) python oauth.py 실행 → 브라우저에서 인증 → 콜백 자동 캡처 → config.json 에 토큰 저장

  브라우저가 필요하므로 서버가 아니라 로컬에서 돌린 뒤 config.json 을 서버로 옮긴다.

요구 사항:
  - Strava 앱의 Authorization Callback Domain 이 localhost 여야 함.
  - Python 3.9+ (표준 라이브러리만 사용, 추가 설치 불필요).
"""
from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# 토큰은 코드가 아니라 데이터 폴더에 둔다 (fetch.py 와 같은 위치)
CONFIG_PATH = Path(__file__).resolve().parents[3] / "activities/strava/api/config.json"
PORT = 8721
REDIRECT_URI = f"http://localhost:{PORT}/callback"
SCOPE = "read,activity:read_all"

_received: dict = {}


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return
        query = urllib.parse.parse_qs(parsed.query)
        _received["code"] = (query.get("code") or [None])[0]
        _received["error"] = (query.get("error") or [None])[0]
        _received["scope"] = (query.get("scope") or [None])[0]

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        body = (
            "<h2>✅ 인증 완료</h2><p>터미널로 돌아가세요.</p>"
            if _received["code"]
            else f"<h2>❌ 인증 실패</h2><p>{_received.get('error')}</p>"
        )
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, *_):  # 콘솔 로그 억제
        pass


def main() -> int:
    if not CONFIG_PATH.exists():
        print(f"[!] {CONFIG_PATH} 없음. config.example.json 을 복사해서 채우세요.")
        return 1

    config = json.loads(CONFIG_PATH.read_text())
    client_id = config.get("client_id")
    client_secret = config.get("client_secret")
    if not client_id or not client_secret:
        print("[!] config.json 의 client_id / client_secret 비어있습니다.")
        return 1

    auth_url = "https://www.strava.com/oauth/authorize?" + urllib.parse.urlencode(
        {
            "client_id": client_id,
            "redirect_uri": REDIRECT_URI,
            "response_type": "code",
            "approval_prompt": "force",
            "scope": SCOPE,
        }
    )

    server = HTTPServer(("127.0.0.1", PORT), CallbackHandler)
    print(f"[*] 콜백 대기: {REDIRECT_URI}")
    print(f"[*] 브라우저 자동 오픈 시도. 안 열리면 아래 URL 직접 접속:\n    {auth_url}\n")
    webbrowser.open(auth_url)

    while "code" not in _received:
        server.handle_request()
    server.server_close()

    if _received.get("error") or not _received.get("code"):
        print(f"[!] 인증 실패: {_received.get('error')}")
        return 1

    print("[*] authorization_code → 토큰 교환 중...")
    body = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": _received["code"],
            "grant_type": "authorization_code",
        }
    ).encode()
    with urllib.request.urlopen(
        "https://www.strava.com/oauth/token", data=body, timeout=15
    ) as resp:
        token_data = json.loads(resp.read())

    athlete = token_data.get("athlete") or {}
    config.update(
        {
            "access_token": token_data["access_token"],
            "refresh_token": token_data["refresh_token"],
            "expires_at": token_data["expires_at"],
            "athlete_id": athlete.get("id", 0),
            "scope": _received.get("scope") or SCOPE,
        }
    )
    CONFIG_PATH.write_text(
        json.dumps(config, indent=2, ensure_ascii=False) + "\n"
    )

    print(
        f"[✓] 인증 완료: {athlete.get('firstname', '')} {athlete.get('lastname', '')} "
        f"(athlete_id={athlete.get('id')})"
    )
    print(f"    토큰 저장 위치: {CONFIG_PATH}")
    print(
        "\n[!] 잊지 말기: Callback Domain 을 localhost 로 바꿨다면 원래 값으로 복원하세요\n"
        "    (같은 Strava 앱을 다른 서비스와 공유하는 경우)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
Strava API 클라이언트 (표준 라이브러리만 사용).

- access_token 만료 시 refresh_token 으로 자동 갱신 (회전된 새 refresh_token 즉시 config.json 에 저장)
- 두 종류 레이트리밋 모두 추적:
    standard: X-RateLimit-Usage / X-RateLimit-Limit          (200 / 2000)
    read:     X-ReadRateLimit-Usage / X-ReadRateLimit-Limit  (100 / 1000)
  더 빡빡한 쪽으로 throttle.
- 단기 한도 95%+ 도달 시 다음 15분 윈도우까지 자동 대기 (proactive)
- 429 응답 시 다음 윈도우까지 대기 후 1회 자동 재시도 (reactive)
"""
from __future__ import annotations

import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

API_BASE = "https://www.strava.com/api/v3"
TOKEN_URL = "https://www.strava.com/oauth/token"

# RateLimitTuple: (short_usage, daily_usage, short_limit, daily_limit)
RateLimitTuple = tuple[int, int, int, int]


class StravaError(Exception):
    pass


class StravaClient:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config: dict = json.loads(config_path.read_text())
        self.rate_limit_standard: RateLimitTuple | None = None
        self.rate_limit_read: RateLimitTuple | None = None

    # ---- config / token ----

    def _save_config(self) -> None:
        self.config_path.write_text(
            json.dumps(self.config, indent=2, ensure_ascii=False) + "\n"
        )

    def _refresh_if_needed(self, force: bool = False) -> None:
        if not force and self.config["expires_at"] - 300 > time.time():
            return
        body = urllib.parse.urlencode(
            {
                "client_id": self.config["client_id"],
                "client_secret": self.config["client_secret"],
                "grant_type": "refresh_token",
                "refresh_token": self.config["refresh_token"],
            }
        ).encode()
        with urllib.request.urlopen(TOKEN_URL, data=body, timeout=15) as resp:
            tokens = json.loads(resp.read())
        self.config["access_token"] = tokens["access_token"]
        self.config["refresh_token"] = tokens["refresh_token"]
        self.config["expires_at"] = tokens["expires_at"]
        self._save_config()

    # ---- rate limit tracking ----

    @staticmethod
    def _parse_pair(usage: str | None, limit: str | None) -> RateLimitTuple | None:
        if not usage or not limit:
            return None
        try:
            su, du = (int(x) for x in usage.split(","))
            sl, dl = (int(x) for x in limit.split(","))
            return (su, du, sl, dl)
        except (ValueError, AttributeError):
            return None

    def _update_rate_limit(self, headers) -> None:
        std = self._parse_pair(
            headers.get("X-RateLimit-Usage"), headers.get("X-RateLimit-Limit")
        )
        if std:
            self.rate_limit_standard = std
        rd = self._parse_pair(
            headers.get("X-ReadRateLimit-Usage"),
            headers.get("X-ReadRateLimit-Limit"),
        )
        if rd:
            self.rate_limit_read = rd

    @property
    def last_rate_limit(self) -> RateLimitTuple | None:
        """가장 빡빡한 limit 하나 (호환용)."""
        return self.rate_limit_read or self.rate_limit_standard

    def short_pressure(self) -> float:
        ratios = []
        for rl in (self.rate_limit_standard, self.rate_limit_read):
            if rl:
                su, _, sl, _ = rl
                if sl:
                    ratios.append(su / sl)
        return max(ratios) if ratios else 0.0

    def daily_pressure(self) -> float:
        ratios = []
        for rl in (self.rate_limit_standard, self.rate_limit_read):
            if rl:
                _, du, _, dl = rl
                if dl:
                    ratios.append(du / dl)
        return max(ratios) if ratios else 0.0

    @staticmethod
    def _seconds_to_next_window() -> int:
        """현재 시각에서 다음 15분 윈도우 경계까지 남은 초 (+30초 버퍼)."""
        now = time.time()
        period = 15 * 60
        nxt = math.ceil(now / period) * period
        return max(int(nxt - now) + 30, 30)

    # ---- HTTP ----

    def get(self, path: str, params: dict | None = None) -> Any:
        self._refresh_if_needed()
        url = f"{API_BASE}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)

        def _request() -> Any:
            req = urllib.request.Request(
                url,
                headers={"Authorization": f"Bearer {self.config['access_token']}"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                self._update_rate_limit(resp.headers)
                return json.loads(resp.read())

        try:
            return _request()
        except urllib.error.HTTPError as e:
            if e.code == 401:
                self._refresh_if_needed(force=True)
                return _request()
            if e.code == 429:
                # 응답 헤더로 limit 갱신 후 다음 윈도우까지 대기, 1회 재시도
                self._update_rate_limit(e.headers)
                wait = self._seconds_to_next_window()
                print(
                    f"    [429] rate limit hit "
                    f"(short={self.short_pressure():.0%}, daily={self.daily_pressure():.0%}). "
                    f"{wait}s 대기 후 재시도.",
                    flush=True,
                )
                time.sleep(wait)
                try:
                    return _request()
                except urllib.error.HTTPError as e2:
                    if e2.code == 429:
                        self._update_rate_limit(e2.headers)
                        raise StravaError(
                            f"재시도 후에도 429. daily_pressure={self.daily_pressure():.0%}. "
                            "일일 한도 도달로 추정. UTC 자정 이후 재개 필요."
                        ) from e2
                    raise
            body = e.read().decode(errors="replace")
            raise StravaError(f"HTTP {e.code} {path}: {body}") from e

    def throttle_sleep(self) -> None:
        """단기 사용률에 따라 적응적 대기. 95%+ 면 다음 윈도우까지 통째 대기."""
        p = self.short_pressure()
        if p >= 0.95:
            wait = self._seconds_to_next_window()
            print(
                f"    [throttle] 단기 한도 {p:.0%} 도달. {wait}s 대기 (다음 15분 윈도우).",
                flush=True,
            )
            time.sleep(wait)
        elif p >= 0.8:
            time.sleep(5)
        elif p >= 0.5:
            time.sleep(2)
        else:
            time.sleep(0.4)

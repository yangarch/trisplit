#!/usr/bin/env python3
"""
intervals.icu 수집기 — Garmin 기록을 intervals.icu 경유로 받아 **기존 원본 계약**으로 변환한다.

왜 바꿨나:
  2026-09-30, Strava 가 Standard 등급 API 를 유료 구독 전용으로 바꾸면서(무료 3개월 종료)
  API 앱이 Inactive 가 됐다. 기록의 원천은 Garmin 이고 Strava 는 중간 경유지였을 뿐이라,
  Garmin → intervals.icu(공식 연동, 개인 API 키 무료) → 여기 로 경로를 바꾼다.

계약 (이 아래 파이프라인은 소스를 모른다):
  activities/intervals/raw/<date>_<icu_id>.json         intervals.icu 응답 원문 (보존용)
  activities/intervals/normalized/<date>_<aid>.json     Strava activity detail 형식으로 변환 → bronze_activities
  lakehouse/raw/streams/<date>_<aid>.json.gz            Strava 스트림 형식으로 변환 → bronze_streams
  activities/intervals/state.json                       무엇을 어떤 내용으로 받았나

  activity_id = 10**12 + intervals 번호 ("i192065295" → 1000192065295).
    Strava id(≤ 약 2.1e10) 와 겹치지 않게 자리를 띄운다. 증상 로그의 옛 행은 Strava id 를 그대로 쓴다.

컷오버:
  Strava 로 받은 마지막 활동이 2026-09-29 라 **2026-09-30(현지) 부터** 받는다.
  같은 활동이 두 소스에 다 있으면 이중 집계된다 — Strava 쪽 external_id(garmin_ping_…)와
  Garmin 활동 id 가 서로 다른 체계라 id 로는 못 거른다. 날짜로 나눈다.

변환에서 만들어 넣는 것 (intervals.icu 스트림에 없다):
  grade_smooth  고도/거리 차분, 약 30m 구간 기울기(%) — 클라임 판정용
  moving        속도 > 0.5 m/s — Strava 의 정지 판정과 같은 용도
없어지는 것: 라이덕 분석(description 의 훈련부하) — Strava 연동 서비스라 경로에 없다.
  랩/스플릿 — 이번 단계에선 빈 배열 (수영 랩은 후속).

API: 개인 키 Basic 인증(username "API_KEY"), 선수 id 자리에 0.
  ⚠️ User-Agent 를 꼭 지정한다 — Python 기본값(Python-urllib)은 Cloudflare 가 1010 으로 막는다.
  한도 하루 5,000 · 15분 2,500. 429 면 Retry-After 만큼 쉬고 한 번 더.
표준 라이브러리만 사용.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

LAKEHOUSE_DIR = Path(__file__).resolve().parents[2]
TC_ROOT = LAKEHOUSE_DIR.parent
BASE = TC_ROOT / "activities/intervals"
RAW_DIR = BASE / "raw"
NORM_DIR = BASE / "normalized"
STATE_PATH = BASE / "state.json"
STREAMS_DIR = LAKEHOUSE_DIR / "raw/streams"

API = "https://intervals.icu/api/v1"
UA = "trisplit-lakehouse/1.0 (personal; +https://github.com/yangarch/trisplit)"
CUTOVER = os.environ.get("INTERVALS_SINCE", "2026-09-30")
ID_OFFSET = 10**12

# 변경 판정에 쓰는 필드 — icu_ctl/atl 처럼 매일 바뀌는 파생값은 넣지 않는다 (매번 재수집하게 된다)
FINGERPRINT_KEYS = ("name", "description", "type", "distance", "moving_time", "elapsed_time",
                    "start_date", "file_type", "total_elevation_gain", "icu_ignore_power", "gear")


class IntervalsError(Exception):
    pass


def _auth() -> str:
    key = os.environ.get("INTERVALS_API_KEY", "").strip()
    if not key:
        raise IntervalsError("INTERVALS_API_KEY 없음 (airflow/.env → compose 환경변수)")
    return "Basic " + base64.b64encode(f"API_KEY:{key}".encode()).decode()


def get(path: str) -> object:
    req = urllib.request.Request(API + path, headers={"Authorization": _auth(), "User-Agent": UA})
    for attempt in (1, 2):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt == 1:
                wait = int(e.headers.get("Retry-After") or 60)
                print(f"    [429] {wait}s 대기 후 재시도", flush=True)
                time.sleep(wait)
                continue
            body = e.read().decode(errors="replace")[:300]
            raise IntervalsError(f"HTTP {e.code} {path}: {body}") from e
    raise IntervalsError(f"재시도 실패 {path}")


# ---- 변환 ----

def activity_id(icu_id: str) -> int:
    return ID_OFFSET + int(icu_id.lstrip("i"))


def fingerprint(a: dict) -> str:
    return hashlib.sha1(json.dumps({k: a.get(k) for k in FINGERPRINT_KEYS},
                                   sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _streams_by_type(streams: list[dict]) -> dict[str, dict]:
    return {s["type"]: s for s in streams or []}


def _grade(dist: list, alt: list, window_m: float = 30.0) -> list:
    """앞뒤 window_m 구간의 고도차 / 거리차 (%). 고도·거리 없으면 빈 배열."""
    n = min(len(dist), len(alt))
    if n == 0:
        return []
    out, j = [], 0
    for i in range(n):
        if dist[i] is None or alt[i] is None:
            out.append(None)
            continue
        while j < i and (dist[j] is None or dist[i] - dist[j] > window_m):
            j += 1
        dd = (dist[i] - dist[j]) if dist[j] is not None else 0
        if dd < 5 or alt[j] is None:
            out.append(out[-1] if out else 0.0)
        else:
            out.append(round(max(-30.0, min(30.0, (alt[i] - alt[j]) / dd * 100)), 1))
    return out


def normalize_streams(aid: int, a: dict, streams: list[dict]) -> dict:
    """intervals.icu streams.json → bronze_streams 가 읽는 Strava 스트림 형식."""
    st = _streams_by_type(streams)

    def data(k):
        return (st.get(k) or {}).get("data") or []

    lat, lng = data("latlng"), (st.get("latlng") or {}).get("data2") or []
    # 빈 지점을 **빼면 안 된다** — bronze_streams 는 스트림들을 인덱스로 짝짓는다(arrays_zip).
    # 하나라도 짧아지면 그 뒤 위치가 다른 시각에 붙는다. null 로 자리를 지킨다.
    latlng = ([[x, y] if x is not None and y is not None else None for x, y in zip(lat, lng)]
              if lat and lng else [])
    dist, alt, vel = data("distance"), data("altitude"), data("velocity_smooth")

    def ints(xs):
        return [None if v is None else int(round(v)) for v in xs]

    out = {
        "time": {"data": ints(data("time"))},
        "latlng": {"data": latlng},
        "distance": {"data": dist},
        "altitude": {"data": alt},
        "velocity_smooth": {"data": vel},
        "heartrate": {"data": ints(data("heartrate"))},
        "cadence": {"data": ints(data("cadence"))},
        "watts": {"data": ints(data("watts"))},
        "temp": {"data": ints(data("temp"))},
        "grade_smooth": {"data": _grade(dist, alt)},
        "moving": {"data": [None if v is None else v > 0.5 for v in vel]},
    }
    return {
        "activity_id": aid,
        "date": a["start_date_local"][:10],
        "type": a.get("type"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "intervals.icu",
        "streams": out,
    }


def normalize_activity(aid: int, a: dict, norm_streams: dict) -> dict:
    """intervals.icu 활동 → bronze_activities 가 읽는 Strava activity detail 형식."""
    ll = [p for p in norm_streams["streams"]["latlng"]["data"] if p]  # GPS 잡히기 전 null 은 건너뛴다
    has_power = bool(a.get("power_meter")) or bool(norm_streams["streams"]["watts"]["data"])
    joules = a.get("icu_joules")
    start_utc = a.get("start_date")
    utc_offset = None
    if start_utc and a.get("start_date_local"):
        utc_offset = (datetime.fromisoformat(a["start_date_local"])
                      - datetime.fromisoformat(start_utc.replace("Z", ""))).total_seconds()
    gear = a.get("gear") or {}
    return {
        "id": aid,
        "name": a.get("name"),
        "type": a.get("type"),
        "sport_type": a.get("type"),
        "start_date": start_utc,
        "start_date_local": (a.get("start_date_local") or "") + "Z",  # Strava 표기와 맞춘다
        "timezone": a.get("timezone"),
        "utc_offset": utc_offset,
        "distance": a.get("distance"),
        "moving_time": a.get("moving_time"),
        "elapsed_time": a.get("elapsed_time"),
        "total_elevation_gain": a.get("total_elevation_gain"),
        "elev_high": a.get("max_altitude"),
        "elev_low": a.get("min_altitude"),
        "average_speed": a.get("average_speed"),
        "max_speed": a.get("max_speed"),
        "average_cadence": a.get("average_cadence"),
        "average_temp": a.get("average_temp"),
        "has_heartrate": a.get("has_heartrate"),
        "average_heartrate": a.get("average_heartrate"),
        "max_heartrate": a.get("max_heartrate"),
        "average_watts": a.get("icu_average_watts"),
        "weighted_average_watts": a.get("icu_weighted_avg_watts"),
        "kilojoules": None if joules is None else joules / 1000.0,
        "device_watts": has_power,
        "calories": a.get("calories"),
        "perceived_exertion": a.get("perceived_exertion"),
        "gear_id": gear.get("id") if isinstance(gear, dict) else None,
        "device_name": a.get("device_name"),
        "trainer": bool(a.get("trainer")) or a.get("type") == "VirtualRide",
        "commute": bool(a.get("commute")),
        "description": a.get("description"),
        "external_id": f"garmin:{a['external_id']}" if a.get("external_id") else None,
        "start_latlng": ll[0] if ll else [],
        "end_latlng": ll[-1] if ll else [],
        "laps": [],
        "splits_metric": [],
        # bronze 스키마 밖이지만 _raw 에 남는다 — 나중에 쓸 수 있게
        "_intervals": {"id": a["id"], "source": a.get("source"), "avg_lr_balance": a.get("avg_lr_balance"),
                       "crank_length": a.get("crank_length"), "icu_training_load": a.get("icu_training_load"),
                       "icu_ctl": a.get("icu_ctl"), "icu_atl": a.get("icu_atl")},
    }


# ---- 상태 ----

def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"seen": {}, "last_run_at": None}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n")


# ---- 명령 ----

def cmd_sync(args: argparse.Namespace) -> int:
    for d in (RAW_DIR, NORM_DIR, STREAMS_DIR):
        d.mkdir(parents=True, exist_ok=True)
    state = load_state()
    newest = (date.today() + timedelta(days=2)).isoformat()
    acts = get(f"/athlete/0/activities?oldest={CUTOVER}&newest={newest}")
    acts = [a for a in acts if (a.get("start_date_local") or "")[:10] >= CUTOVER]
    todo = [a for a in acts if args.refresh or state["seen"].get(a["id"]) != fingerprint(a)]
    print(f"[*] intervals.icu {CUTOVER}~ 활동 {len(acts)}건, 새로/바뀐 것 {len(todo)}건")

    for i, a in enumerate(sorted(todo, key=lambda x: x["start_date_local"]), 1):
        aid = activity_id(a["id"])
        day = a["start_date_local"][:10]
        print(f"  [{i}/{len(todo)}] {day} {a.get('type'):12s} {(a.get('distance') or 0)/1000:6.1f}km "
              f"{a.get('device_name') or ''} → {aid}")
        if args.dry:
            continue
        # 요가·수동 입력처럼 스트림이 없는 활동은 404 일 수 있다 — 빈 스트림으로 (활동 요약은 받는다)
        try:
            streams = get(f"/activity/{a['id']}/streams.json") if a.get("stream_types") else []
        except IntervalsError as e:
            if "HTTP 404" not in str(e):
                raise
            streams = []
        ns = normalize_streams(aid, a, streams if isinstance(streams, list) else [])
        (RAW_DIR / f"{day}_{a['id']}.json").write_text(json.dumps(a, ensure_ascii=False, indent=1) + "\n")
        with gzip.open(STREAMS_DIR / f"{day}_{aid}.json.gz", "wt", encoding="utf-8") as f:
            json.dump(ns, f, ensure_ascii=False)
        (NORM_DIR / f"{day}_{aid}.json").write_text(
            json.dumps(normalize_activity(aid, a, ns), ensure_ascii=False, indent=1) + "\n")
        state["seen"][a["id"]] = fingerprint(a)
        save_state(state)  # 활동마다 저장 — 중간에 죽어도 받은 것은 다시 받지 않는다
        time.sleep(0.3)

    state["last_run_at"] = datetime.now(timezone.utc).isoformat()
    if not args.dry:
        save_state(state)
    print(f"[✓] 완료. 누적 {len(state['seen'])}건")
    # 마지막 줄은 기계용 — Airflow BashOperator 가 마지막 줄을 XCom 으로 넘기고,
    # intervals_poll DAG 가 이 값으로 파이프라인을 트리거할지 정한다.
    print(f"NEW={0 if args.dry else len(todo)}")
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    st = load_state()
    print(f"컷오버: {CUTOVER} · 받은 활동 {len(st['seen'])}건 · 마지막 실행 {st.get('last_run_at')}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="intervals.icu 수집기")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sync", help="컷오버 이후 새로/바뀐 활동 수집")
    s.add_argument("--refresh", action="store_true", help="전부 다시 받기")
    s.add_argument("--dry", action="store_true", help="대상만 출력")
    s.set_defaults(func=cmd_sync)
    sub.add_parser("status").set_defaults(func=cmd_status)
    args = p.parse_args()
    try:
        return args.func(args)
    except IntervalsError as e:
        print(f"[!] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

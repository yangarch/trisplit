#!/usr/bin/env python3
"""
Strava per-point 스트림 재수집 — Bronze 레이어용.

왜 별도 스크립트인가:
  기존 `activities/strava/api/fetch.py` 는 launchd 가 매일 22:00 에 돌리는 운영 경로다.
  거기에 스트림 수집을 끼워 넣으면 매일 동기화가 느려지고 실패 지점이 늘어난다.
  그래서 기존 파일은 손대지 않고, 클라이언트만 import 해서 재사용한다.

왜 필요한가:
  `gpx_writer.py:53` 이 "GPX 표준 확장이 없다"는 이유로 per-point watts 를 버린다.
  JSON 에도 스트림은 저장되지 않아서, 파워는 활동 평균/NP 만 남아 있다.
  → 파워 기반 분석이 활동 단위에 갇혀 있음. 원본 스트림을 그대로 받아두면 복구된다.

저장:
  lakehouse/raw/streams/<YYYY-MM-DD>_<activity_id>.json.gz   # 원본 응답 그대로 (무가공)
  lakehouse/ingest/streams_state.json                        # 재개용 상태 (독립)

  기존 state.json 과 섞지 않는다 — 한쪽이 깨져도 다른 쪽에 영향이 없어야 한다.

사용법:
  python3 lakehouse/ingest/fetch_streams.py status
  python3 lakehouse/ingest/fetch_streams.py fetch            # 파워 있는 활동만 (기본)
  python3 lakehouse/ingest/fetch_streams.py fetch --all-gps  # GPS 있는 활동 전부
  python3 lakehouse/ingest/fetch_streams.py fetch --limit 50
  python3 lakehouse/ingest/fetch_streams.py fetch --dry

일일 한도(read 1,000)에 걸리면 상태를 저장하고 정상 종료한다. 다시 실행하면 이어서 받는다.
표준 라이브러리만 사용.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

LAKEHOUSE_DIR = Path(__file__).resolve().parent.parent
TC_ROOT = LAKEHOUSE_DIR.parent
STRAVA_API_DIR = TC_ROOT / "activities/strava/api"
RAW_GPX_DIR = TC_ROOT / "activities/strava/raw-gpx"

OUT_DIR = LAKEHOUSE_DIR / "raw/streams"
STATE_PATH = Path(__file__).resolve().parent / "streams_state.json"
CONFIG_PATH = STRAVA_API_DIR / "config.json"

# 기존 fetch.py 의 STREAM_KEYS 보다 넓다.
#   추가: distance(누적거리), velocity_smooth(속도), grade_smooth(경사), moving(정지판정)
#   watts 가 핵심 — 이게 지금 어디에도 저장돼 있지 않다.
STREAM_KEYS = (
    "time,latlng,distance,altitude,velocity_smooth,heartrate,cadence,watts,temp,grade_smooth,moving"
)

sys.path.insert(0, str(STRAVA_API_DIR))
from strava_client import StravaClient, StravaError  # noqa: E402


# ---- state ----

def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"fetched_ids": [], "no_stream_ids": [], "gone_ids": [], "last_run_at": 0}


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n")


# ---- 대상 선정 ----

def scan_candidates(all_gps: bool) -> list[dict]:
    """raw-gpx 의 활동 JSON 을 훑어 스트림 수집 대상을 고른다."""
    out = []
    for jf in sorted(RAW_GPX_DIR.glob("*.json")):
        try:
            d = json.loads(jf.read_text())
        except json.JSONDecodeError:
            continue
        has_gps = bool(d.get("start_latlng"))
        has_power = d.get("average_watts") is not None
        # 실내 활동은 latlng 이 없어도 watts/hr 스트림은 존재한다 → 파워 기준이 우선.
        if not (has_power or (all_gps and has_gps)):
            continue
        sd = (d.get("start_date_local") or d.get("start_date") or "")[:10]
        if not sd:
            continue
        out.append(
            {
                "id": d["id"],
                "date": sd,
                "type": d.get("type"),
                "name": d.get("name", ""),
                "has_power": has_power,
                "distance_km": (d.get("distance") or 0) / 1000,
            }
        )
    out.sort(key=lambda a: a["date"])
    return out


def out_path(item: dict) -> Path:
    return OUT_DIR / f"{item['date']}_{item['id']}.json.gz"


# ---- 커맨드 ----

def cmd_fetch(args: argparse.Namespace) -> int:
    state = load_state()
    state.setdefault("gone_ids", [])  # 구버전 상태 파일 호환
    done = set(state["fetched_ids"]) | set(state["no_stream_ids"]) | set(state["gone_ids"])

    candidates = scan_candidates(args.all_gps)
    # 파일이 이미 있으면 상태와 무관하게 건너뛴다 (상태 파일 유실 대비)
    todo = [c for c in candidates if c["id"] not in done and not out_path(c).exists()]
    remaining_total = len(todo)
    if args.limit:
        todo = todo[: args.limit]

    scope = "GPS 있는 활동 전부" if args.all_gps else "파워 있는 활동"
    print(f"[*] 대상: {scope}")
    print(f"[*] 후보 {len(candidates)}건 / 남은 것 {remaining_total}건 (완료 {len(done)}건)")
    if args.limit and remaining_total > len(todo):
        print(f"[*] --limit {args.limit} 적용 → 이번 실행은 {len(todo)}건만")

    if not todo:
        print("[✓] 받을 것 없음.")
        return 0

    if args.dry:
        for c in todo:
            flag = "W" if c["has_power"] else "-"
            print(f"  [{flag}] {c['date']} {c['type']:14s} {c['distance_km']:>7.1f}km  {c['name'][:50]}")
        print(f"\n[dry] {len(todo)}건. 예상 API 호출 {len(todo)}회 (read 일일 한도 1,000).")
        return 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    client = StravaClient(CONFIG_PATH)
    saved = 0

    for i, c in enumerate(todo, 1):
        print(
            f"  [{i}/{len(todo)}] {c['date']} {c['type']:14s} {c['name'][:45]}",
            flush=True,
        )
        try:
            streams = client.get(
                f"/activities/{c['id']}/streams",
                {"keys": STREAM_KEYS, "key_by_type": "true"},
            )
        except StravaError as e:
            # 404 = Strava 에서 삭제된 활동. 로컬엔 JSON 이 남아 있지만 스트림은 못 받는다.
            # 재시도해도 영영 안 되므로 기록하고 넘어간다 —
            # 이걸 치명적 에러로 잡으면 뒤에 남은 활동이 전부 막힌다.
            if "HTTP 404" in str(e):
                state["gone_ids"].append(c["id"])
                save_state(state)
                print(f"    → 404 (Strava 에서 삭제됨) — 건너뜀")
                continue
            print(f"    [!] 실패: {e}")
            print("    상태 저장 후 종료. 다시 실행하면 이어서 받습니다.")
            break

        if not streams:
            # 스트림이 아예 없는 활동 (수동 입력 등) — 재시도해도 소용없으므로 기록해 둔다.
            state["no_stream_ids"].append(c["id"])
            print("    → 스트림 없음 (스킵 기록)")
        else:
            payload = {
                "activity_id": c["id"],
                "date": c["date"],
                "type": c["type"],
                "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
                "stream_keys_requested": STREAM_KEYS,
                "streams": streams,
            }
            with gzip.open(out_path(c), "wt", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            state["fetched_ids"].append(c["id"])
            saved += 1

            keys = sorted(streams.keys()) if isinstance(streams, dict) else []
            npt = 0
            if isinstance(streams, dict):
                first = next(iter(streams.values()), None)
                if isinstance(first, dict):
                    npt = len(first.get("data") or [])
            has_w = "watts" if "watts" in keys else "no-watts"
            print(f"    → {npt:,} points, {len(keys)} streams [{has_w}]")

        state["last_run_at"] = int(time.time())
        save_state(state)

        if client.daily_pressure() >= 0.97:
            print(
                f"    [!] 일일 한도 {client.daily_pressure():.0%} 도달. 상태 저장됨. "
                "UTC 자정(KST 09:00) 이후 다시 실행하면 이어서 받습니다."
            )
            break

        client.throttle_sleep()

    done_now = set(state["fetched_ids"]) | set(state["no_stream_ids"]) | set(state["gone_ids"])
    remaining = len([c for c in candidates if c["id"] not in done_now])
    print(f"\n[✓] 이번 실행 {saved}건 저장. 누적 {len(state['fetched_ids'])}건. 남은 것 {remaining}건.")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    state = load_state()
    state.setdefault("gone_ids", [])
    candidates = scan_candidates(args.all_gps)
    done = set(state["fetched_ids"]) | set(state["no_stream_ids"]) | set(state["gone_ids"])
    files = list(OUT_DIR.glob("*.json.gz")) if OUT_DIR.exists() else []
    size = sum(f.stat().st_size for f in files)

    print(f"수집 대상 후보:      {len(candidates)}건 ({'GPS 전부' if args.all_gps else '파워 있는 활동'})")
    print(f"수집 완료:           {len(state['fetched_ids'])}건")
    print(f"스트림 없음(스킵):   {len(state['no_stream_ids'])}건")
    print(f"404 삭제됨(스킵):    {len(state['gone_ids'])}건")
    print(f"남은 것:             {len([c for c in candidates if c['id'] not in done])}건")
    print(f"저장 파일:           {len(files)}개 / {size / 1024 / 1024:.1f} MB")
    if state.get("last_run_at"):
        ts = datetime.fromtimestamp(state["last_run_at"], tz=timezone.utc).astimezone()
        print(f"마지막 실행:         {ts.isoformat()}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Strava per-point 스트림 재수집 (Bronze용)")
    sub = p.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("fetch", help="스트림 수집 (재개 가능)")
    f.add_argument("--all-gps", action="store_true", help="파워 없어도 GPS 있는 활동 전부")
    f.add_argument("--limit", type=int, help="이번 실행에서 받을 최대 건수")
    f.add_argument("--dry", action="store_true", help="호출 없이 대상만 출력")
    f.set_defaults(func=cmd_fetch)

    s = sub.add_parser("status", help="수집 진행 상황")
    s.add_argument("--all-gps", action="store_true")
    s.set_defaults(func=cmd_status)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

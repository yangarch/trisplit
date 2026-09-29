#!/usr/bin/env python3
"""
Strava 활동 수집 CLI.

명령:
  python3 fetch.py status
  python3 fetch.py sync                              # 마지막 sync 이후 신규 활동만
  python3 fetch.py sync --since 30d                  # 최근 30일 중 아직 안 받은 것
  python3 fetch.py sync --since 2d --refresh         # 최근 2일을 이미 받은 것까지 다시 받기
  python3 fetch.py sync --since 2026-01-01           # 특정 날짜 이후
  python3 fetch.py sync --all                        # 전체 히스토리 (느림)
  python3 fetch.py sync --dry                        # 호출 안 하고 대상만 출력
  python3 fetch.py one <activity_id> [...]           # 지정 활동 강제 재수집
  python3 fetch.py check-riduck <activity_id> [...]  # 라이덕 분석이 붙었는지 (0=준비됨)

저장 위치 (코드와 분리된 데이터 폴더, $TC_ROOT = 이 저장소의 부모 디렉토리):
  activities/strava/raw-gpx/<YYYY-MM-DD>_<activity_id>.json   # 활동 상세 (라이덕 분석은 description 에)
  activities/strava/raw-gpx/<YYYY-MM-DD>_<activity_id>.gpx    # GPS 트랙 (실내 활동은 미생성)
  activities/strava/api/config.json                          # OAuth 토큰 (git 제외, 회전됨)
  activities/strava/api/state.json                           # 동기화 상태

왜 --refresh 가 필요한가:
  sync 는 한 번 받은 활동을 다시 받지 않는다(synced_ids). 그런데 라이덕 같은 분석 서비스는
  업로드 **뒤에** description 을 고쳐 쓰고, 사람도 제목·설명을 나중에 고친다.
  업로드 직후 받으면 그 뒤의 수정이 영영 반영되지 않는다 → 야간에 최근 며칠을 다시 받아 덮는다.

표준 라이브러리만 사용. 추가 설치 필요 없음.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gpx_writer import streams_to_gpx  # noqa: E402
from strava_client import StravaClient, StravaError  # noqa: E402

# 코드(이 저장소)와 데이터(부모 디렉토리의 activities/)를 분리한다.
# 다른 ingest 스크립트와 같은 규칙: LAKEHOUSE_DIR.parent == TC_ROOT.
LAKEHOUSE_DIR = Path(__file__).resolve().parents[2]
TC_ROOT = LAKEHOUSE_DIR.parent
STRAVA_DIR = TC_ROOT / "activities/strava"
CONFIG_PATH = STRAVA_DIR / "api/config.json"
STATE_PATH = STRAVA_DIR / "api/state.json"
GPX_DIR = STRAVA_DIR / "raw-gpx"

STREAM_KEYS = "latlng,time,altitude,heartrate,cadence,watts,temp"

SPORT_MAP = {
    "Ride": "cycling",
    "VirtualRide": "cycling",
    "EBikeRide": "cycling",
    "GravelRide": "cycling",
    "MountainBikeRide": "cycling",
    "Run": "running",
    "TrailRun": "running",
    "VirtualRun": "running",
    "Swim": "swimming",
}


# ---- state ----

def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"last_synced_at": 0, "last_activity_start_at": 0, "synced_ids": []}


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n")


# ---- helpers ----

def parse_since(s: str) -> int:
    if s.endswith("d") and s[:-1].isdigit():
        days = int(s[:-1])
        return int((datetime.now(tz=timezone.utc) - timedelta(days=days)).timestamp())
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def iso_to_epoch(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def has_riduck(detail: dict) -> bool:
    """라이덕 분석이 description 에 붙었는지.

    판정은 dbt/models/staging/stg_riduck.sql 과 **같은 규칙**을 쓴다 —
    여기서 "붙었다" 고 판단했는데 파서가 못 읽으면 기다린 의미가 없다.
    """
    desc = detail.get("description") or ""
    return ("훈련상태" in desc and "R파워" in desc) or "riduck" in desc.lower()


def expects_riduck(detail: dict) -> bool:
    """라이덕이 분석할 활동인가 — 라이딩 전부 (파워계 유무 무관, 라이덕은 둘 다 분석한다).
    수영·러닝은 기다릴 이유가 없다. 라이덕이 안 붙는 라이딩이 있어도 센서 상한(20분)에서 넘어간다."""
    return SPORT_MAP.get(detail.get("type")) == "cycling"


def list_activities(client: StravaClient, after_epoch: int) -> list[dict]:
    out: list[dict] = []
    page = 1
    while True:
        params = {"page": page, "per_page": 100}
        if after_epoch:
            params["after"] = after_epoch
        items = client.get("/athlete/activities", params)
        if not items:
            break
        out.extend(items)
        if len(items) < 100:
            break
        page += 1
        client.throttle_sleep()
    out.sort(key=lambda a: a["start_date"])
    return out


def fetch_activity(client: StravaClient, summary: dict) -> dict:
    activity_id = summary["id"]
    start_local = summary.get("start_date_local") or summary.get("start_date") or ""
    date_str = start_local[:10] or "unknown"
    base = GPX_DIR / f"{date_str}_{activity_id}"
    GPX_DIR.mkdir(parents=True, exist_ok=True)

    detail = client.get(f"/activities/{activity_id}", {"include_all_efforts": "false"})
    base.with_suffix(".json").write_text(
        json.dumps(detail, indent=2, ensure_ascii=False) + "\n"
    )
    client.throttle_sleep()

    has_gps = bool(detail.get("start_latlng"))
    written = {"json": str(base.with_suffix(".json"))}

    if has_gps:
        streams = client.get(
            f"/activities/{activity_id}/streams",
            {"keys": STREAM_KEYS, "key_by_type": "true"},
        )
        gpx = streams_to_gpx(detail, streams)
        if gpx:
            base.with_suffix(".gpx").write_text(gpx)
            written["gpx"] = str(base.with_suffix(".gpx"))
        client.throttle_sleep()

    return {
        **written,
        "type": detail.get("type"),
        "sport": SPORT_MAP.get(detail.get("type"), "other"),
        "name": detail.get("name"),
        "distance_km": round(detail.get("distance", 0) / 1000, 2),
        "moving_min": round(detail.get("moving_time", 0) / 60, 1),
        "riduck": has_riduck(detail),
    }


# ---- commands ----

def cmd_sync(args: argparse.Namespace) -> int:
    if args.refresh and not (args.since or args.all):
        # 범위 없이 refresh 하면 "마지막 활동 이후" 만 보게 되어 의미가 없다
        print("[!] --refresh 는 --since 또는 --all 과 함께 써야 합니다.")
        return 2

    client = StravaClient(CONFIG_PATH)
    state = load_state()

    if args.all:
        after = 0
        scope = "all-time"
    elif args.since:
        after = parse_since(args.since)
        scope = args.since
    else:
        after = state.get("last_activity_start_at", 0)
        scope = "신규"

    print(f"[*] 조회 범위: {scope} (after={after}){' — 재수집' if args.refresh else ''}")
    summaries = list_activities(client, after)
    synced_ids = set(state.get("synced_ids", []))
    if args.refresh:
        new_items = summaries
        print(f"[*] 대상 활동: {len(new_items)}건 (이미 받은 것 포함 재수집)")
    else:
        new_items = [a for a in summaries if a["id"] not in synced_ids]
        print(f"[*] 대상 활동: {len(new_items)}건 (전체 {len(summaries)}건, 이미 받은 것 제외)")

    if not new_items:
        state["last_synced_at"] = int(time.time())
        save_state(state)
        return 0

    if args.dry:
        for a in new_items:
            print(
                f"  - {a['start_date_local'][:10]} {a['type']:14s} "
                f"{round(a.get('distance', 0)/1000, 1):>7.1f}km  {a.get('name', '')[:60]}"
            )
        return 0

    for i, summary in enumerate(new_items, 1):
        head = (
            f"  [{i}/{len(new_items)}] {summary['start_date_local'][:10]} "
            f"{summary['type']:14s} {summary.get('name', '')[:50]}"
        )
        print(head)
        try:
            res = fetch_activity(client, summary)
        except StravaError as e:
            print(f"    [!] 실패: {e}")
            print("    상태 저장 후 종료. 나중에 다시 sync 하면 이어서 받습니다.")
            break
        synced_ids.add(summary["id"])
        latest = max(
            state.get("last_activity_start_at", 0),
            iso_to_epoch(summary["start_date"]),
        )
        state["synced_ids"] = sorted(synced_ids)
        state["last_activity_start_at"] = latest
        state["last_synced_at"] = int(time.time())
        save_state(state)

        ext = "gpx+json" if res.get("gpx") else "json only (no GPS)"
        print(f"    → {res['sport']:8s} {res['distance_km']:>6.2f}km  [{ext}]")

        if client.daily_pressure() >= 0.97:
            print(
                f"    [!] 일일 한도 {client.daily_pressure():.0%} 도달. "
                "상태 저장됨. UTC 자정 (KST 09:00) 이후 다시 sync 하면 이어서 받습니다."
            )
            return 0

    print(f"[✓] 완료. 누적 동기화 {len(synced_ids)}건.")
    return 0


def cmd_one(args: argparse.Namespace) -> int:
    client = StravaClient(CONFIG_PATH)
    state = load_state()
    ids = set(state.get("synced_ids", []))
    for activity_id in args.activity_ids:
        summary = client.get(f"/activities/{activity_id}")
        res = fetch_activity(client, summary)
        tag = "라이덕 ✓" if res["riduck"] else "라이덕 -"
        print(f"[✓] {activity_id} {res['sport']} {res['distance_km']}km [{tag}] {res['name']}")
        ids.add(activity_id)
        state["last_activity_start_at"] = max(
            state.get("last_activity_start_at", 0),
            iso_to_epoch(summary["start_date"]),
        )
    state["synced_ids"] = sorted(ids)
    state["last_synced_at"] = int(time.time())
    save_state(state)
    return 0


def cmd_check_riduck(args: argparse.Namespace) -> int:
    """모든 활동이 '준비됨' 이면 0, 하나라도 라이덕을 기다리는 중이면 1.

    준비됨 = 라이덕 대상이 아니거나(수영·러닝 등 라이딩 외), 이미 분석이 붙었음.
    Airflow BashSensor 가 종료코드로 판정한다.
    """
    client = StravaClient(CONFIG_PATH)
    waiting = []
    for activity_id in args.activity_ids:
        try:
            d = client.get(f"/activities/{activity_id}", {"include_all_efforts": "false"})
        except StravaError as e:
            # 삭제·비공개 전환 등으로 못 읽으면 기다려도 소용없다 → 준비됨으로 취급
            print(f"  {activity_id}: 조회 실패 ({e}) — 기다리지 않음")
            continue
        if not expects_riduck(d):
            print(f"  {activity_id}: {d.get('type')} — 라이덕 대상 아님")
        elif has_riduck(d):
            print(f"  {activity_id}: 라이덕 분석 있음")
        else:
            print(f"  {activity_id}: 라이덕 대기 중")
            waiting.append(activity_id)
        client.throttle_sleep()
    return 1 if waiting else 0


def cmd_status(_args: argparse.Namespace) -> int:
    state = load_state()
    if state.get("last_synced_at"):
        ts = datetime.fromtimestamp(state["last_synced_at"], tz=timezone.utc).astimezone()
        print(f"마지막 sync 시각:    {ts.isoformat()}")
    else:
        print("마지막 sync 시각:    (아직 없음)")
    print(f"누적 동기화 활동:    {len(state.get('synced_ids', []))}건")
    if state.get("last_activity_start_at"):
        ts = datetime.fromtimestamp(state["last_activity_start_at"], tz=timezone.utc).astimezone()
        print(f"가장 최근 활동:      {ts.isoformat()}")

    gpx_count = len(list(GPX_DIR.glob("*.gpx"))) if GPX_DIR.exists() else 0
    json_count = len(list(GPX_DIR.glob("*.json"))) if GPX_DIR.exists() else 0
    print(f"raw-gpx 파일:        gpx {gpx_count} / json {json_count}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Strava 활동 수집기")
    sub = p.add_subparsers(dest="cmd", required=True)

    sync_p = sub.add_parser("sync", help="신규 활동 동기화")
    sync_p.add_argument("--since", help="기간 (예: 30d) 또는 날짜 (YYYY-MM-DD)")
    sync_p.add_argument("--all", action="store_true", help="전체 히스토리")
    sync_p.add_argument("--refresh", action="store_true", help="이미 받은 활동도 다시 받기 (--since/--all 필요)")
    sync_p.add_argument("--dry", action="store_true", help="실제 수집 없이 대상만 출력")
    sync_p.add_argument("-y", "--yes", action="store_true", help="확인 프롬프트 건너뛰기")
    sync_p.set_defaults(func=cmd_sync)

    one_p = sub.add_parser("one", help="지정 활동 (재수집)")
    one_p.add_argument("activity_ids", type=int, nargs="+")
    one_p.set_defaults(func=cmd_one)

    ck_p = sub.add_parser("check-riduck", help="라이덕 분석 준비 여부 (0=준비됨, 1=대기)")
    ck_p.add_argument("activity_ids", type=int, nargs="+")
    ck_p.set_defaults(func=cmd_check_riduck)

    status_p = sub.add_parser("status", help="동기화 상태")
    status_p.set_defaults(func=cmd_status)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

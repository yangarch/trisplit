#!/usr/bin/env python3
"""
intervals.icu 일별 훈련부하 → dbt seed.

  activities/intervals/wellness.json  (ingest/intervals/fetch.py wellness)  → dbt/seeds/intervals_fitness.csv

라이덕 체력/피로/균형을 대신한다. 라이덕은 Strava description 에 실려 왔는데,
수집 경로가 intervals.icu 로 바뀌며(2026-09-30) 9/29 에서 멈췄다.

wellness 응답에는 체중·HRV·수면 같은 다른 항목도 있지만 **훈련부하 칸만** 시드로 옮긴다 —
쓰지 않는 개인 건강 수치를 테이블에 늘어놓지 않는다 (원문은 wellness.json 에 남아 있다).

원본이 없으면(공개 저장소를 받아 돌리는 경우) 헤더만 있는 빈 시드를 쓴다.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

LAKEHOUSE = Path(__file__).resolve().parent.parent
ROOT = LAKEHOUSE.parent
WELLNESS = ROOT / "activities/intervals/wellness.json"
OUT = LAKEHOUSE / "dbt/seeds/intervals_fitness.csv"

COLS = ["fitness_date", "ctl", "atl", "ramp_rate", "load", "ride_eftp"]


def rnd(v, n=2):
    return "" if v is None else round(float(v), n)


def main() -> int:
    rows = []
    if WELLNESS.exists():
        for r in json.loads(WELLNESS.read_text(encoding="utf-8")):
            ride = next((s for s in r.get("sportInfo") or [] if s.get("type") == "Ride"), {})
            rows.append({
                "fitness_date": r["id"],
                "ctl": rnd(r.get("ctl")),
                "atl": rnd(r.get("atl")),
                "ramp_rate": rnd(r.get("rampRate")),
                # ctlLoad = 그날 훈련부하 합 (CTL 계산에 들어간 값). atlLoad 와 같다.
                "load": rnd(r.get("ctlLoad"), 0),
                "ride_eftp": rnd(ride.get("eftp"), 0),
            })
    else:
        print(f"[~] {WELLNESS} 없음 — 빈 시드")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=COLS)
        wr.writeheader()
        wr.writerows(sorted(rows, key=lambda r: r["fitness_date"]))
    print(f"[✓] {OUT.name} {len(rows)}일" + (f" ({rows[0]['fitness_date']}~{rows[-1]['fitness_date']})" if rows else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())

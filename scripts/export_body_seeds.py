#!/usr/bin/env python3
"""
증상 로그 · 피팅 변경 이력 (마크다운 표) → dbt seed.

  body/symptoms/log.md              → dbt/seeds/symptom_log.csv
  equipment/bikes/fitting/changes.md → dbt/seeds/fitting_changes.csv

ftp-log.md 와 같은 이유로 시드로 물질화한다 — 원본은 사람이 대화하며 고치는 마크다운이고,
Gold 가 마크다운 파싱에 의존하지 않게 한다.

원본이 없으면(공개 저장소를 받아 돌리는 경우) **헤더만 있는 빈 시드**를 쓴다.
모델은 그대로 돌고 빈 결과를 낸다 — 개인 기록이 없다고 파이프라인이 죽으면 안 된다.

날짜가 없는 피팅 행(시점 미상)은 구간을 나눌 수 없어 제외하고 알린다.
형식이 깨진 행은 **실패시킨다** — 조용히 버리면 "기록했는데 분석에 안 나오는" 상태가 된다.
"""
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

LAKEHOUSE = Path(__file__).resolve().parent.parent
ROOT = LAKEHOUSE.parent
SEEDS = LAKEHOUSE / "dbt/seeds"

SYMPTOMS_MD = ROOT / "body/symptoms/log.md"
FITTING_MD = ROOT / "equipment/bikes/fitting/changes.md"

ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")

SPORTS = {"사이클": "cycling", "러닝": "running", "수영": "swimming"}
AREAS = {
    "무릎내측", "무릎외측", "무릎앞", "안장", "손저림", "삼두어깨", "목", "허리", "둔근",
    "경련-종아리", "경련-허벅지", "발", "가슴", "기타",
}
SIDES = {"좌", "우", "양", "-", ""}
COURSES = {"자기완화", "지속", "악화", "운동후", ""}
TARGETS = {"chichi", "kiki", "신발"}
ITEMS = {
    "안장높이", "안장전후", "안장모델", "바높이", "바모델", "스템", "크랭크",
    "클릿각도", "클릿위치", "신발핏", "페달", "기타",
}
STATES = {"적용", "원복", "계획"}

SYMPTOM_COLS = ["symptom_date", "sport", "activity_id", "area", "side", "severity",
                "onset", "course", "next_day", "confounders", "memo"]
FITTING_COLS = ["effective_date", "target", "item", "side", "before", "after",
                "status", "source", "memo"]


def table_rows(path: Path, first_cell: re.Pattern) -> list[list[str]]:
    """마크다운 표에서 첫 칸이 패턴에 맞는 행만 칸 목록으로."""
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells and first_cell.match(cells[0]):
            rows.append(cells)
    return rows


def int_or_blank(v: str, lo: int, hi: int, what: str, where: str) -> str:
    if v == "":
        return ""
    if not v.isdigit() or not (lo <= int(v) <= hi):
        raise ValueError(f"{where}: {what} '{v}' 는 {lo}~{hi} 정수여야 함")
    return v


def export_symptoms() -> list[dict]:
    if not SYMPTOMS_MD.exists():
        print(f"[~] {SYMPTOMS_MD} 없음 — 빈 시드")
        return []
    out = []
    for c in table_rows(SYMPTOMS_MD, ISO):
        where = f"증상 {c[0]} {c[3] if len(c) > 3 else ''}"
        if len(c) != len(SYMPTOM_COLS):
            raise ValueError(f"{where}: 칸 {len(c)}개 (기대 {len(SYMPTOM_COLS)})")
        date, sport, aid, area, side, sev, onset, course, nxt, conf, memo = c
        if sport not in SPORTS:
            raise ValueError(f"{where}: 종목 '{sport}'")
        if area not in AREAS:
            raise ValueError(f"{where}: 부위 '{area}' — 머리말의 어휘만 쓴다")
        if side not in SIDES:
            raise ValueError(f"{where}: 좌우 '{side}'")
        if course not in COURSES:
            raise ValueError(f"{where}: 경과 '{course}'")
        if aid and not aid.isdigit():
            raise ValueError(f"{where}: activity '{aid}'")
        out.append({
            "symptom_date": date,
            "sport": SPORTS[sport],
            "activity_id": aid,
            "area": area,
            "side": "" if side == "-" else side,
            "severity": int_or_blank(sev, 0, 10, "강도", where),
            "onset": onset,
            "course": course,
            "next_day": int_or_blank(nxt, 0, 10, "다음날", where),
            "confounders": conf,
            "memo": memo,
        })
    return out


def export_fitting() -> list[dict]:
    if not FITTING_MD.exists():
        print(f"[~] {FITTING_MD} 없음 — 빈 시드")
        return []
    out = []
    # 이력 표의 행: 첫 칸이 날짜이거나 비어 있다 (시점 미상). 규칙 표(대상/항목…)는 제외.
    for c in table_rows(FITTING_MD, re.compile(r"^(\d{4}-\d{2}-\d{2})?$")):
        if len(c) != len(FITTING_COLS):
            continue  # 다른 표
        date, target, item, side, before, after, status, source, memo = c
        where = f"피팅 {date or '(날짜 없음)'} {target} {item}"
        if target not in TARGETS:
            raise ValueError(f"{where}: 대상 '{target}'")
        if item not in ITEMS:
            raise ValueError(f"{where}: 항목 '{item}'")
        if side not in SIDES:
            raise ValueError(f"{where}: 좌우 '{side}'")
        if status not in STATES:
            raise ValueError(f"{where}: 상태 '{status}'")
        if not date:
            if status != "계획":
                print(f"[~] 적용일 없음, 구간 분석에서 제외: {target} {item} {after}")
            continue
        out.append({
            "effective_date": date, "target": target, "item": item,
            "side": "" if side == "-" else side, "before": before, "after": after,
            "status": status, "source": source, "memo": memo,
        })
    return out


def write(name: str, cols: list[str], rows: list[dict]) -> None:
    SEEDS.mkdir(parents=True, exist_ok=True)
    path = SEEDS / name
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    print(f"[✓] {path.name}: {len(rows)}행")


def main() -> int:
    try:
        symptoms = export_symptoms()
        fitting = export_fitting()
    except ValueError as e:
        print(f"[!] 형식 오류 — {e}", file=sys.stderr)
        return 1
    write("symptom_log.csv", SYMPTOM_COLS, symptoms)
    write("fitting_changes.csv", FITTING_COLS, fitting)
    return 0


if __name__ == "__main__":
    sys.exit(main())

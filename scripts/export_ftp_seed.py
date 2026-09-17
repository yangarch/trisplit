#!/usr/bin/env python3
"""
`activities/cycling/ftp-log.md` → dbt seed (`dbt/seeds/ftp_log.csv`).

FTP 는 사람이 손으로 관리하는 마크다운이 원본이다(CLAUDE.md 의 "가장 최근 FTP 사용" 규칙).
파이프라인이 그 파일을 직접 읽게 하면 Gold 가 마크다운 파싱에 의존하게 되므로,
시드로 한 번 물질화해서 dbt 가 테이블로 다루게 한다.

ftp-log.md 를 갱신하면 이 스크립트를 다시 돌리고 `dbt seed` 한다.
"""
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FTP_LOG = ROOT / "activities/cycling/ftp-log.md"
OUT = Path(__file__).resolve().parent.parent / "dbt/seeds/ftp_log.csv"

ROW_RE = re.compile(
    r"^\|\s*(?P<date>[^|]+?)\s*\|\s*(?P<ftp>\d+)\s*\|\s*(?P<wkg>[^|]*?)\s*\|"
    r"\s*(?P<method>[^|]*?)\s*\|\s*(?P<conf>[^|]*?)\s*\|"
)
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def main() -> int:
    if not FTP_LOG.exists():
        print(f"[!] {FTP_LOG} 없음", file=sys.stderr)
        return 1

    text = FTP_LOG.read_text()

    # "현재값" — overview.py 가 분석 앵커로 쓰는 값. 같은 패턴으로 뽑아 일치를 보장한다.
    m = re.search(r"##\s*현재값[\s\S]+?\*\*(\d+)\s*W\*\*", text)
    current_ftp = int(m.group(1)) if m else None

    rows = []
    for line in text.splitlines():
        mm = ROW_RE.match(line)
        if not mm:
            continue
        date = mm.group("date").strip()
        if not ISO_RE.match(date):
            # "(시점 미상, 2025년)" 같은 항목 — 시계열에 못 넣는다. 건너뛰되 알린다.
            print(f"[~] 날짜 파싱 불가, 시드에서 제외: {date!r} (FTP {mm.group('ftp')}W)")
            continue
        wkg = mm.group("wkg").strip()
        rows.append({
            "effective_date": date,
            "ftp_watts": int(mm.group("ftp")),
            "w_per_kg": wkg if re.match(r"^[\d.]+$", wkg) else "",
            "method": mm.group("method").strip().replace('"', ""),
            "confidence": mm.group("conf").strip(),
            "is_current": "true" if current_ftp and int(mm.group("ftp")) == current_ftp else "false",
        })

    if not rows:
        print("[!] 이력 행을 하나도 못 읽음", file=sys.stderr)
        return 1

    rows.sort(key=lambda r: r["effective_date"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"[✓] {OUT} — {len(rows)}행 (현재값 {current_ftp}W)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

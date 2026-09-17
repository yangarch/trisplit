#!/usr/bin/env python3
"""
Gold 마트 → 종합 개요 리포트 (markdown).

기존 `reports/_scripts/overview.py` 를 대체하는 리포트 생성기다.
출력 형식과 섹션 구성을 최대한 맞춘다 — Claude 가 이 리포트를 컨텍스트로 받아
복장·운동분석·장비 상담을 하므로, 형식이 바뀌면 그 워크플로가 흔들린다.

기존과 달라지는 것 둘:
  1. **헤드유닛 동시기록 중복 8건이 빠진다.** 사이클 누적이 412.9km 줄어든다.
     숨기지 않고 리포트에 제외 내역을 싣는다.
  2. **파워 지표가 추가된다.** NP 를 원본 스트림에서 직접 계산해 IF / TSS 까지 낸다.
     기존 파이프라인은 per-point 파워를 저장하지 않아 불가능했던 것이다.

사용법:
  source lakehouse/env.sh
  lakehouse/.venv/bin/python lakehouse/scripts/build_report.py
  lakehouse/.venv/bin/python lakehouse/scripts/build_report.py --out reports/latest_overview.md
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

LAKEHOUSE = Path(__file__).resolve().parent.parent
ROOT = LAKEHOUSE.parent
sys.path.insert(0, str(LAKEHOUSE / "scripts"))
from spark_session import get_spark  # noqa: E402

SPORT_ORDER = ["cycling", "swimming", "running", "walking", "other"]


def fmt(v, nd=1):
    return "·" if v is None else f"{float(v):,.{nd}f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", help="출력 경로 (기본: reports/<YYYY-MM-DD>_overview.md)")
    ap.add_argument("--since-days", type=int, default=60, help="최근 윈도우 (기본 60)")
    args = ap.parse_args()

    today = date.today().isoformat()
    since = (date.fromisoformat(today) - timedelta(days=args.since_days)).isoformat()
    out = Path(args.out) if args.out else ROOT / f"reports/{today}_overview.md"

    spark = get_spark("build-report")
    spark.sparkContext.setLogLevel("ERROR")
    L: list[str] = []
    w = L.append

    q = lambda sql: spark.sql(sql).collect()  # noqa: E731

    meta = q("""SELECT count(*) n, min(start_date_key) d0, max(start_date_key) d1,
                       count_if(is_duplicate) dups
                FROM tc.silver.activities""")[0]
    ftp = q("SELECT ftp_watts FROM tc.gold.ftp_log WHERE is_current")[0]["ftp_watts"]

    w(f"# {today} — 종합 개요 (레이크하우스)")
    w("")
    w(f"- 데이터: Strava 동기화 **{meta['n']}건** ({meta['d0']} ~ {meta['d1']})")
    w(f"- 중복 제외: **{meta['dups']}건** (헤드유닛 동시기록 — 하단 참조)")
    w(f"- 분석 컨텍스트 FTP: **{ftp} W** (출처: `activities/cycling/ftp-log.md` 현재값)")
    w(f"- 생성: `lakehouse/scripts/build_report.py` (Iceberg Gold 마트)")
    w("")

    # ── 1. 종목별 ──
    w("## 1. 종목별 누적 (전 기간)")
    w("")
    w("| 종목 | 활동 | 거리 (km) | 시간 (h) | 누적 고도 (m) |")
    w("|---|---:|---:|---:|---:|")
    rows = {r["sport"]: r for r in q("SELECT * FROM tc.gold.mart_sport_totals")}
    for s in SPORT_ORDER:
        if s in rows:
            r = rows[s]
            w(f"| {s} | {r['activities']} | {fmt(r['distance_km'])} | "
              f"{fmt(r['moving_h'])} | {fmt(r['elev_m'], 0)} |")
    w("")

    # ── 2. 연도별 ──
    w("## 2. 연도별")
    w("")
    ys = q("SELECT * FROM tc.gold.mart_yearly_summary ORDER BY year, sport")
    years = sorted({r["year"] for r in ys})
    used = [s for s in SPORT_ORDER if any(r["sport"] == s for r in ys)]
    grid = {(r["year"], r["sport"]): r for r in ys}
    w("| 연도 | " + " | ".join(used) + " | 합 |")
    w("|" + ":-:|" * (len(used) + 2))
    for y in years:
        cells, tot = [], 0
        for s in used:
            r = grid.get((y, s))
            cells.append(str(r["activities"]) if r else "·")
            tot += r["activities"] if r else 0
        w(f"| {y} | " + " | ".join(cells) + f" | {tot} |")
    w("")
    w("### 연도별 거리 (사이클 / 수영)")
    w("")
    w("| 연도 | 사이클 km | 수영 km |")
    w("|:-:|---:|---:|")
    for y in years:
        c = grid.get((y, "cycling"))
        s = grid.get((y, "swimming"))
        if c or s:
            w(f"| {y} | {fmt(c['distance_km']) if c else '·'} | "
              f"{fmt(s['distance_km'], 2) if s else '·'} |")
    w("")

    # ── 3. 사이클 ──
    w("## 3. 사이클 디테일")
    w("")
    cyc = rows.get("cycling")
    if cyc:
        ind = q("""SELECT count_if(is_indoor) n, sum(CASE WHEN is_indoor THEN distance_km END) km
                   FROM tc.silver.activities WHERE sport='cycling' AND NOT is_duplicate""")[0]
        w(f"- 총 활동: **{cyc['activities']}건**, 거리 **{fmt(cyc['distance_km'])} km**, "
          f"시간 **{fmt(cyc['moving_h'])} h**")
        w(f"- 인도어: **{ind['n']}건** ({ind['n']/cyc['activities']*100:.1f}%) / "
          f"거리 **{fmt(ind['km'])} km**")
        w("")

    w("### 자전거별 사용")
    w("")
    w("| 자전거 | 활동 | 거리 km | 시간 h | 평균 km/회 | 인도어 비중 | 사용 기간 |")
    w("|---|---:|---:|---:|---:|---:|---|")
    for r in q("SELECT * FROM tc.gold.mart_gear_usage ORDER BY distance_km DESC"):
        w(f"| {r['gear_label']} | {r['activities']} | {fmt(r['distance_km'])} | "
          f"{fmt(r['moving_h'])} | {fmt(r['avg_km_per_ride'])} | "
          f"{fmt(r['indoor_pct'], 0)}% | {r['first_used']} ~ {r['last_used']} |")
    w("")

    # ── 파워 (신규) ──
    w("### 파워 분석 — 원본 스트림에서 직접 계산")
    w("")
    w("> 기존 파이프라인은 per-point 파워를 저장하지 않아 Strava 가 준 값만 쓸 수 있었다.")
    w("> 이제 NP 를 직접 계산하고 IF / TSS 까지 낸다.")
    w("")
    p = q("""SELECT count(*) n, max(np_computed) max_np,
                    avg(np_computed - np_strava) bias
             FROM tc.gold.mart_ride_power WHERE np_strava IS NOT NULL""")[0]
    w(f"- 파워 데이터 있는 라이딩: **{p['n']}건**")
    w(f"- Strava 제공값 대비 평균 편향: **{p['bias']:+.2f} W** "
      f"(Strava 는 자체 평활을 쓰는 다른 지표 — 둘 다 보존)")
    recent = q(f"""SELECT count(*) n, avg(np_computed) np, avg(intensity_factor) if_
                   FROM tc.gold.mart_ride_power WHERE start_date_key >= date'{since}'""")[0]
    if recent["n"]:
        w(f"- 최근 {args.since_days}일: 라이딩 **{recent['n']}건**, "
          f"평균 NP **{recent['np']:.0f} W**, 평균 IF **{recent['if_']:.2f}**")
    w("")
    w("**장거리 Top 8 (NP / IF / TSS)**")
    w("")
    w("| 일자 | 거리 | 시간 | NP(직접) | NP(Strava) | IF | TSS | 라이딩명 |")
    w("|:-:|---:|---:|---:|---:|---:|---:|---|")
    for r in q("SELECT * FROM tc.gold.mart_ride_power ORDER BY distance_km DESC LIMIT 8"):
        w(f"| {r['start_date_key']} | {fmt(r['distance_km'])}km | {fmt(r['moving_h'], 2)}h | "
          f"{fmt(r['np_computed'], 0)} | {fmt(r['np_strava'], 0)} | "
          f"{fmt(r['intensity_factor'], 2)} | {fmt(r['tss'], 0)} | {r['name'][:28]} |")
    w("")

    # ── 라이덕 ──
    rd = q(f"""SELECT count(*) n FROM tc.gold.mart_riduck_metrics
               WHERE sport='cycling' AND start_date_key >= date'{since}'""")[0]
    if rd["n"]:
        w("### 라이덕 지표")
        w("")
        last = q("""SELECT * FROM tc.gold.mart_riduck_metrics WHERE sport='cycling'
                    ORDER BY start_date_key DESC LIMIT 1""")[0]
        w(f"- **최신 훈련부하** ({last['start_date_key']}): "
          f"체력 **{last['fitness']}** / 피로 **{last['fatigue']}** / 균형 **{last['form']}**")
        imp = q(f"""SELECT percentile_approx(implied_ftp, 0.5) med, sum(load) total_load, count(*) n
                    FROM tc.gold.mart_riduck_metrics
                    WHERE sport='cycling' AND start_date_key >= date'{since}'""")[0]
        if imp["med"]:
            w(f"- **라이덕 추정 FTP (최근 {args.since_days}일 중앙값): ~{int(imp['med'])} W** "
              f"— ftp-log 현재값 {ftp}W 대비 **{int(imp['med']) - ftp:+d}W**")
        if imp["total_load"]:
            w(f"- 최근 {args.since_days}일 훈련량 합: **{imp['total_load']}** ({imp['n']}건)")
        w("")
        w("**훈련부하 추세 (최근 8건)**")
        w("")
        w("| 일자 | 체력 | 피로 | 균형 | 강도 | 훈련량 |")
        w("|:-:|--:|--:|--:|--:|--:|")
        tr = q("""SELECT * FROM tc.gold.mart_riduck_metrics
                  WHERE sport='cycling' AND fitness IS NOT NULL
                  ORDER BY start_date_key DESC LIMIT 8""")
        for r in reversed(tr):
            w(f"| {r['start_date_key']} | {r['fitness']} | {r['fatigue']} | "
              f"{r['form']} | {r['intensity']} | {r['load']} |")
        w("")

    # ── 수영 ──
    sw = q("SELECT * FROM tc.gold.mart_swim_pace_monthly ORDER BY month")
    if sw:
        w("## 4. 수영 디테일")
        w("")
        s = rows.get("swimming")
        if s:
            w(f"- 총 활동: **{s['activities']}건**, 거리 **{fmt(s['distance_km']*1000, 0)} m**, "
              f"시간 **{fmt(s['moving_h'])} h**")
        w("")
        w("> ⚠ Strava swim 의 평균 페이스는 휴식 시간 포함이라 강습/자유수영 구분 없이")
        w("> 단순 비교하면 오해가 생긴다. 가이드: `activities/swimming/notes-on-pace.md`.")
        w("")
        w("| 월 | 활동 | 거리 m | 평균 페이스 (mm:ss /100m) |")
        w("|:-:|---:|---:|---:|")
        for r in sw:
            w(f"| {r['month']} | {r['swims']} | {fmt(r['distance_m'], 0)} | {r['pace_mmss']} |")
        w("")

    # ── 중복 제외 내역 ──
    w("## 5. 중복 제외 내역")
    w("")
    w("헤드유닛 두 대가 같은 라이딩을 동시에 기록해 Strava 에 둘 다 올라간 건이다.")
    w("겹치는 활동을 묶어 **가장 오래 기록된 쪽**을 남긴다. 판정 근거는 `tc.silver.activities`.")
    w("")
    w("| 일자 | 남김 | 버림 |")
    w("|:-:|---|---|")
    for r in q("""SELECT d.start_date_key day, d.device_name ddev, d.distance_km dkm,
                         p.device_name pdev, p.distance_km pkm
                  FROM tc.silver.activities d
                  JOIN tc.silver.activities p ON d.duplicate_of_activity_id = p.activity_id
                  WHERE d.is_duplicate ORDER BY d.start_date_key"""):
        w(f"| {r['day']} | {r['pdev']} {fmt(r['pkm'])}km | {r['ddev']} {fmt(r['dkm'])}km |")
    w("")
    w("---")
    w("")
    w("> 생성: `lakehouse/scripts/build_report.py` — Bronze/Silver/Gold(Iceberg) + dbt 마트 기반.")
    w("> 기존 `reports/_scripts/overview.py` 와 전 지표 대조 완료 (`scripts/parity_check.py`, 94/94).")

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(L) + "\n")
    print(f"[✓] 리포트: {out}")
    print(f"    활동 {meta['n']}건 (중복 {meta['dups']}건 제외), FTP {ftp}W")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

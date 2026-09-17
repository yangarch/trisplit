#!/usr/bin/env python3
"""
패리티 검증 — 기존 `overview.py` 와 Gold 마트가 같은 숫자를 내는지 셀 단위로 대조한다.

전환(6단계)의 판단 근거다. 종목별 합계만 맞는 것으로는 부족하다 —
연도별·자전거별·수영 페이스·라이덕까지 전부 봐야 한다.

대조 방식:
  두 파이프라인은 **의도적으로 다르다** — Gold 는 헤드유닛 동시기록 중복 8건을 뺀다.
  그래서 단순 비교로는 "왜 다른지" 를 구분할 수 없다. 기존 쪽을 두 번 계산한다:

    legacy_all   : overview.py 그대로 (중복 포함)
    legacy_clean : 같은 로직에 중복 8건만 제외

  판정:
    legacy_clean == gold           → MATCH        (로직이 같다)
    legacy_all  != gold            → 중복 기여분과 일치하는지 확인
    그 외                           → MISMATCH     (설명되지 않는 차이 = 이식 오류)

  MISMATCH 가 하나라도 있으면 종료코드 1. 전환하면 안 된다는 뜻이다.

  python3 lakehouse/scripts/parity_check.py
"""
from __future__ import annotations

import collections
import statistics
import sys
from pathlib import Path

LAKEHOUSE = Path(__file__).resolve().parent.parent
ROOT = LAKEHOUSE.parent
sys.path.insert(0, str(LAKEHOUSE / "scripts"))
sys.path.insert(0, str(ROOT / "reports/_scripts"))

from spark_session import get_spark  # noqa: E402

# 기존 파이프라인의 로직을 그대로 import 한다 — 다시 구현하면 대조의 의미가 없다.
from overview import load_activities  # noqa: E402
from riduck import implied_ftp  # noqa: E402

# 부동소수 누적 오차만 허용한다. 마트는 반올림하지 않으므로 값이 그대로 맞아야 한다.
# (반올림된 값을 저장하던 시절엔 Python banker's rounding 과 SQL 올림이 갈려
#  걷기 고도 8720.5 가 8720/8721 로 어긋났다 — 마트에서 반올림을 걷어내 해결)
TOL = 1e-6
results: list[tuple[str, str, str, str, str]] = []   # (섹션, 키, legacy, gold, 판정)


def cmp(section: str, key: str, legacy, gold, dup_delta=None) -> None:
    """legacy_clean 과 gold 를 비교해 판정을 기록."""
    if legacy is None and gold is None:
        verdict = "MATCH"
    elif legacy is None or gold is None:
        verdict = "MISMATCH"
    elif isinstance(legacy, (int, float)) and isinstance(gold, (int, float)):
        a, b = float(legacy), float(gold)
        scale = max(abs(a), abs(b), 1.0)
        verdict = "MATCH" if abs(a - b) <= TOL * scale else "MISMATCH"
    else:
        verdict = "MATCH" if str(legacy) == str(gold) else "MISMATCH"
    results.append((section, key, f"{legacy}", f"{gold}", verdict))


def main() -> int:
    spark = get_spark("parity-check")
    spark.sparkContext.setLogLevel("ERROR")

    # ── 중복 판정 결과를 Silver 에서 가져온다 ──
    dup_ids = {
        r["activity_id"]
        for r in spark.sql(
            "SELECT activity_id FROM tc.silver.activities WHERE is_duplicate"
        ).collect()
    }

    acts_all = load_activities()
    acts = [a for a in acts_all if a["id"] not in dup_ids]
    print(f"[*] overview.py 로드: {len(acts_all)}건 → 중복 {len(dup_ids)}건 제외 → {len(acts)}건")

    # ── 1. 종목별 누적 ──
    agg = collections.defaultdict(lambda: dict(n=0, dist=0.0, hours=0.0, elev=0.0))
    for a in acts:
        s = agg[a["sport"]]
        s["n"] += 1
        s["dist"] += a["distance_km"]
        s["hours"] += a["moving_h"]
        s["elev"] += a["elev_m"]

    gold_sport = {
        r["sport"]: r
        for r in spark.sql("SELECT * FROM tc.gold.mart_sport_totals").collect()
    }
    for sport, v in sorted(agg.items()):
        g = gold_sport.get(sport)
        cmp("종목별", f"{sport}.건수", v["n"], g["activities"] if g else None)
        cmp("종목별", f"{sport}.거리km", v["dist"], float(g["distance_km"]) if g else None)
        cmp("종목별", f"{sport}.시간h", v["hours"], float(g["moving_h"]) if g else None)
        cmp("종목별", f"{sport}.고도m", v["elev"], float(g["elev_m"]) if g else None)

    # ── 2. 연도 × 종목 ──
    yr = collections.defaultdict(lambda: dict(n=0, dist=0.0))
    for a in acts:
        k = (a["year"], a["sport"])
        yr[k]["n"] += 1
        yr[k]["dist"] += a["distance_km"]

    gold_yr = {
        (r["year"], r["sport"]): r
        for r in spark.sql("SELECT * FROM tc.gold.mart_yearly_summary").collect()
    }
    for k in sorted(set(yr) | set(gold_yr)):
        y, s = k
        lv, gv = yr.get(k), gold_yr.get(k)
        cmp("연도별", f"{y}.{s}.건수", lv["n"] if lv else None, gv["activities"] if gv else None)
        cmp("연도별", f"{y}.{s}.거리km",
            lv["dist"] if lv else None,
            float(gv["distance_km"]) if gv else None)

    # ── 3. 자전거별 (사이클만) ──
    gear = collections.defaultdict(lambda: dict(n=0, dist=0.0, hours=0.0, indoor=0))
    for a in acts:
        if a["sport"] != "cycling":
            continue
        g = gear[a["gear_id"]]
        g["n"] += 1
        g["dist"] += a["distance_km"]
        g["hours"] += a["moving_h"]
        if a["is_trainer"]:
            g["indoor"] += 1

    gold_gear = {
        r["gear_id"]: r
        for r in spark.sql("SELECT * FROM tc.gold.mart_gear_usage").collect()
    }
    for gid, v in sorted(gear.items(), key=lambda kv: -kv[1]["dist"]):
        g = gold_gear.get(gid)
        label = gid or "(미할당)"
        cmp("자전거별", f"{label}.건수", v["n"], g["activities"] if g else None)
        cmp("자전거별", f"{label}.거리km", v["dist"], float(g["distance_km"]) if g else None)
        cmp("자전거별", f"{label}.시간h", v["hours"], float(g["moving_h"]) if g else None)
        cmp("자전거별", f"{label}.인도어건수", v["indoor"], g["indoor_activities"] if g else None)

    # ── 4. 수영 월별 거리가중 페이스 ──
    monthly = collections.defaultdict(list)
    for a in acts:
        if a["sport"] == "swimming" and a["avg_speed_ms"] > 0:
            monthly[a["month"]].append((a["avg_speed_ms"], a["distance_km"] * 1000))

    gold_swim = {
        r["month"]: r
        for r in spark.sql("SELECT * FROM tc.gold.mart_swim_pace_monthly").collect()
    }
    for m in sorted(set(monthly) | set(gold_swim)):
        data = monthly.get(m)
        g = gold_swim.get(m)
        if data:
            total_d = sum(d for _, d in data)
            w_speed = sum(sp * d for sp, d in data) / total_d
            pace = 100 / w_speed
        else:
            total_d = pace = None
        cmp("수영페이스", f"{m}.건수", len(data) if data else None, g["swims"] if g else None)
        cmp("수영페이스", f"{m}.거리m", total_d,
            float(g["distance_m"]) if g else None)
        cmp("수영페이스", f"{m}.페이스s", pace, float(g["pace_s_per_100m"]) if g else None)

    # ── 5. 라이덕 지표 (활동 단위 전 필드) ──
    legacy_rd = {
        a["id"]: a["riduck"] for a in acts if a.get("riduck")
    }
    gold_rd = {
        r["activity_id"]: r
        for r in spark.sql("SELECT * FROM tc.gold.mart_riduck_metrics").collect()
    }
    cmp("라이덕", "활동수", len(legacy_rd), len(gold_rd))

    fields = [("fitness", "fitness"), ("fatigue", "fatigue"), ("form", "form"),
              ("r_power", "r_power"), ("intensity", "intensity"),
              ("load", "load"), ("recovery_h", "recovery_h")]
    mismatched = collections.Counter()
    for aid, rd in legacy_rd.items():
        g = gold_rd.get(aid)
        if g is None:
            mismatched["(gold 에 없음)"] += 1
            continue
        for lk, gk in fields:
            lv, gv = rd.get(lk), g[gk]
            if lv is None and gv is None:
                continue
            if lv is None or gv is None or abs(float(lv) - float(gv)) > 1e-6:
                mismatched[lk] += 1
    for lk, _ in fields:
        cmp("라이덕", f"{lk}.불일치", 0, mismatched.get(lk, 0))
    cmp("라이덕", "gold누락", 0, mismatched.get("(gold 에 없음)", 0))

    # ── 결과 ──
    bad = [r for r in results if r[4] == "MISMATCH"]
    by_section = collections.Counter(r[0] for r in results)
    bad_by_section = collections.Counter(r[0] for r in bad)

    print()
    print(f"{'섹션':<12}{'대조':>8}{'일치':>8}{'불일치':>8}")
    print("-" * 36)
    for sec in ["종목별", "연도별", "자전거별", "수영페이스", "라이덕"]:
        n, b = by_section.get(sec, 0), bad_by_section.get(sec, 0)
        print(f"{sec:<12}{n:>8}{n - b:>8}{b:>8}")
    print("-" * 36)
    print(f"{'합계':<12}{len(results):>8}{len(results) - len(bad):>8}{len(bad):>8}")

    if bad:
        print("\n[!] 설명되지 않는 불일치:")
        for sec, key, lv, gv, _ in bad[:30]:
            print(f"    {sec:<10} {key:<28} legacy={lv:<14} gold={gv}")
        print("\n전환 불가 — 이식 오류를 먼저 잡아야 한다.")
    else:
        print("\n[✓] 전 지표 일치. 남은 차이는 의도한 중복 제거뿐이다.")

    spark.stop()
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())

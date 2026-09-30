"""
"지금 상태" 문단 — 메인 페이지(HTML)와 상담용 마크다운이 같이 쓴다.

LLM 이 아니라 **규칙으로** 쓴다. 매일 밤·업로드마다 도는 파이프라인에 외부 API 호출을 넣지 않고,
같은 숫자면 항상 같은 문장이 나오게 한다 (숫자와 문장이 어긋나는 종류의 버그가 없다).

판정 기준을 지어내지 않는다 — 라이덕 균형을 "과훈련" 같은 말로 바꾸지 않고 숫자와
"피로가 체력보다 높다/낮다" 라는 사실만 쓴다. 해석은 대화에서.
"""
from __future__ import annotations

from datetime import date

SPORT_KO = {"cycling": "사이클", "swimming": "수영", "running": "러닝"}
WEEKDAY_KO = "월화수목금토일"


def _trend(recent: float | None, prev: float | None) -> str:
    recent, prev = recent or 0.0, prev or 0.0
    if prev == 0:
        return "직전 4주 없음" if recent else ""
    pct = (recent - prev) / prev * 100
    if abs(pct) < 10:
        return "직전 4주와 비슷"
    return f"직전 4주 대비 {pct:+.0f}%"


def sport_sentence(r: dict) -> str | None:
    """종목 한 줄. r = mart_training_status 행."""
    ko = SPORT_KO[r["sport"]]
    if not r["last_date"]:
        return None
    if r["n_recent"]:
        s = f"{ko} {r['n_recent']}회 {(r['km_recent'] or 0):,.1f}km"
        t = _trend(r["km_recent"], r["km_prev"])
        if t:
            s += f"({t})"
        if r["sport"] == "cycling" and (r["longest_recent_km"] or 0) >= 100:
            s += f", 최장 {r['longest_recent_km']:,.0f}km"
        return s
    return f"{ko}은 {r['days_since']}일째 없음 (마지막 {r['last_date']})"


def riduck_sentence(rd: dict | None, today: date) -> str | None:
    """라이덕 체력/피로/균형 — 가장 최근 값."""
    if not rd or rd.get("fitness") is None:
        return None
    form = rd["form"]
    rel = "피로가 체력보다 높다" if form < 0 else ("체력이 피로보다 높다" if form > 0 else "체력과 피로가 같다")
    s = f"라이덕 훈련부하는 체력 {rd['fitness']} · 피로 {rd['fatigue']} · 균형 {form:+d} — {rel}"
    age = (today - rd["start_date_key"]).days
    if age > 7:
        s += f" (마지막 라이딩 {rd['start_date_key']} 기준, {age}일 전)"
    return s


def knee_sentence(k: dict | None) -> str | None:
    if not k:
        return None
    return (f"무릎(좌 내측) 최근 기록은 {k['ride_date']} {k['knee_medial_l']}"
            + (f", 다음날 {k['knee_medial_l_next']}" if k.get("knee_medial_l_next") is not None else ""))


def status_paragraph(status: list[dict], riduck: dict | None, knee: dict | None, today: date) -> str:
    order = {"cycling": 0, "swimming": 1, "running": 2}
    parts = [s for s in (sport_sentence(r) for r in sorted(status, key=lambda r: order[r["sport"]])) if s]
    out = "최근 4주 " + ", ".join(parts) + "." if parts else "최근 4주 기록이 없다."
    for extra in (riduck_sentence(riduck, today), knee_sentence(knee)):
        if extra:
            out += " " + extra + "."
    return out


def weekday(d: date) -> str:
    return WEEKDAY_KO[d.weekday()]


def pace(seconds: float | None, unit: str) -> str:
    if not seconds:
        return ""
    m, s = divmod(int(round(seconds)), 60)
    return f"{m}:{s:02d}/{unit}"

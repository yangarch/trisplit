"""Time-aware cycling pacing evidence. Standard library; no network/model calls.

Inputs stay local. Missing power is never zero-filled; a real zero stays valid.
The output describes observed riding, never a measured physiological limit.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from html import escape
import math
from statistics import median

DEFAULTS = {
    "window_s": 300, "stride_s": 60, "max_gap_s": 5,
    "min_coverage": .98, "min_moving": .95,
    "min_pedaling": .8, "route_tolerance_m": 180,
    "distance_tolerance": .15, "grade_tolerance_pct": 1.5,
    "lookback_days": 180, "min_prior_active_s": 5400,
    "min_load_difference_kj": 150, "max_prior_unknown_fraction": .15,
}


def number(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def prefix(values):
    result = [0.0]
    for value in values:
        result.append(result[-1] + value)
    return result


def haversine(a, b):
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2-lat1)/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin((lon2-lon1)/2)**2
    return 6371000 * 2 * math.asin(min(1, math.sqrt(h)))


def prepare(meta, points, ftp=None, config=None):
    """Summarize one activity into sliding, contiguous wall-time windows.

    Points: t (seconds), watts, moving, cadence, speed_ms, lat/lon,
    distance_m, ele_m, heartrate, temp_c. Intervals use a left hold only
    for gaps <= max_gap_s. Longer intervals remain unknown, including stops
    whose recording was paused. Last sample has zero duration.
    """
    cfg = DEFAULTS | (config or {})
    if meta.get("is_indoor"):
        return {"meta": meta, "windows": [], "rejections": {"indoor": 1}}
    if meta.get("device_watts") is False:
        return {"meta": meta, "windows": [], "rejections": {"estimated_power": 1}}
    by_time = {}
    duplicate_timestamps = 0
    for point in points:
        t = number(point.get("t"))
        if t is None:
            continue
        t = int(t)
        if t in by_time:
            # Conflicting duplicate timestamps cannot count twice or silently win.
            duplicate_timestamps += 1
            by_time[t] = {"t": t, "watts": None}
        else:
            by_time[t] = point
    times = sorted(by_time)
    if len(times) < 2:
        return {"meta": meta, "windows": [], "rejections": {"no_stream": 1}}
    # t is relative to activity start, including an initially missing stream.
    origin, span = 0, times[-1]
    if times[0] < 0:
        return {"meta": meta, "windows": [], "rejections": {"invalid_time": 1}}
    if span < cfg["window_s"] or span > 48 * 3600:
        return {"meta": meta, "windows": [], "rejections": {"duration": 1}}
    names = ("valid", "power", "moving", "motion_known", "pedaling", "cadence",
             "cadence_valid", "hr", "hr_valid", "temp", "temp_valid")
    arrays = {name: [0.0] * span for name in names}
    gps, distance, elevation = [None] * span, [None] * span, [None] * span
    for left, right in zip(times, times[1:]):
        dt = right - left
        if dt <= 0 or dt > cfg["max_gap_s"]:
            continue
        point, end_point = by_time[left], by_time[right]
        watts = number(point.get("watts"))
        valid = watts is not None and watts >= 0
        cadence, hr, temp = (number(point.get(k)) for k in ("cadence", "heartrate", "temp_c"))
        speed = number(point.get("speed_ms"))
        motion = point.get("moving")
        if motion is None:
            motion = speed >= .5 if speed is not None else (cadence > 0 if cadence is not None else None)
        lat, lon = number(point.get("lat")), number(point.get("lon"))
        lat2, lon2 = number(end_point.get("lat")), number(end_point.get("lon"))
        coord_ok = (lat is not None and lon is not None and -90 <= lat <= 90
                    and -180 <= lon <= 180 and (lat != 0 or lon != 0))
        for t in range(left-origin, right-origin):
            frac = (t + origin - left) / dt
            arrays["valid"][t] = int(valid)
            arrays["power"][t] = watts if valid else 0
            arrays["motion_known"][t] = int(motion is not None)
            arrays["moving"][t] = int(bool(motion))
            arrays["pedaling"][t] = int(cadence is not None and cadence > 0)
            for name, value in (("cadence", cadence), ("hr", hr), ("temp", temp)):
                if value is not None and (name == "temp" or value > 0):
                    arrays[name][t] = value
                    arrays[name+"_valid"][t] = 1
            if coord_ok:
                gps[t] = ((lat+(lat2-lat)*frac, lon+(lon2-lon)*frac)
                          if lat2 is not None and lon2 is not None else (lat, lon))
            for key, destination in (("distance_m", distance), ("ele_m", elevation)):
                v1, v2 = number(point.get(key)), number(end_point.get(key))
                if v1 is not None:
                    destination[t] = v1 + (v2-v1)*frac if v2 is not None else v1
    sums = {key: prefix(value) for key, value in arrays.items()}
    high, onset = [0] * span, [0] * span
    run = 0
    if ftp and number(ftp) and ftp > 0:
        for t in range(29, span):
            n = sums["valid"][t+1]-sums["valid"][t-29]
            high[t] = int(n == 30 and (sums["power"][t+1]-sums["power"][t-29])/30 > ftp)
            run = run+1 if high[t] else 0
            onset[t] = int(run == 20)  # twenty seconds of sustained >anchor 30s mean
    high_sum, onset_sum = prefix(high), prefix(onset)
    # Locate sustained early efforts so the rider can explain/change specific
    # moments even when repeated-route reference data is not yet available.
    early_end = min(span, 5400)
    events, begin = [], None
    for t in range(early_end + 1):
        is_high = t < early_end and high[t]
        if is_high and begin is None:
            begin = t
        if not is_high and begin is not None:
            if t-begin >= 20:
                observed_start = max(0, begin-29)
                events.append({"start_s": observed_start, "end_s": t,
                               "classified_above_s": t-begin,
                               "avg_watts": (sums["power"][t]-sums["power"][observed_start])/(t-observed_start),
                               "recorded_kj": (sums["power"][t]-sums["power"][observed_start])/1000})
            begin = None
    early = {"start_s": 0, "end_s": early_end,
             "power_coverage": sums["valid"][early_end]/early_end,
             "recorded_kj": sums["power"][early_end]/1000,
             "above_anchor_s": high_sum[early_end] if ftp else None,
             "sustained_events": events if ftp else []}
    windows, rejected = [], Counter()
    d = cfg["window_s"]
    active_total = sums["moving"][-1]
    for start in range(0, span-d+1, cfg["stride_s"]):
        end = start+d
        total = lambda key: sums[key][end]-sums[key][start]
        coverage, moving, pedaling = total("valid")/d, total("moving")/d, total("pedaling")/d
        reason = None
        if coverage < cfg["min_coverage"]:
            reason = "power_gap"
        elif moving < cfg["min_moving"]:
            reason = "stops_or_unknown_motion"
        elif pedaling < cfg["min_pedaling"]:
            reason = "coasting_or_missing_cadence"
        anchors = [gps[t] for t in (start, start+d//4, start+d//2, start+3*d//4, end-1)]
        if reason is None and any(x is None for x in anchors):
            reason = "missing_gps"
        dist0, dist1 = distance[start], distance[end-1]
        ele0, ele1 = elevation[start], elevation[end-1]
        meters = dist1-dist0 if dist0 is not None and dist1 is not None else None
        grade = 100*(ele1-ele0)/meters if meters and meters > 0 and ele0 is not None and ele1 is not None else None
        if reason is None and (meters is None or meters < 300 or grade is None):
            reason = "distance_or_elevation"
        if reason is None and grade < -.5:
            reason = "descent"
        if reason:
            rejected[reason] += 1
            continue
        mean = lambda key: total(key)/total(key+"_valid") if total(key+"_valid") >= d*.9 else None
        prior = sums["valid"][start]
        windows.append({
            "activity_id": str(meta["activity_id"]), "start_s": start, "end_s": end,
            "duration_s": d, "avg_watts": total("power")/total("valid"),
            "coverage": coverage, "moving_fraction": moving,
            "pedaling_fraction": pedaling, "distance_m": meters, "grade_pct": grade,
            "heartrate": mean("hr"), "cadence": mean("cadence"), "temp_c": mean("temp"),
            "prior_recorded_kj": sums["power"][start]/1000,
            "prior_unknown_s": start-prior, "prior_active_s": sums["moving"][start],
            "prior_unknown_fraction": (start-prior)/start if start else 0,
            "active_fraction": sums["moving"][start]/active_total if active_total else 0,
            "prior_above_anchor_s": high_sum[start] if ftp else None,
            "prior_surge_count": int(onset_sum[start]) if ftp else None,
            "anchors": anchors,
        })
    return {"meta": meta, "windows": windows, "rejections": dict(rejected), "early_efforts": early,
            "quality": {"elapsed_s": span, "recorded_power_s": int(sums["valid"][-1]),
                        "observed_moving_s": int(active_total),
                        "power_coverage": sums["valid"][-1]/span,
                        "duplicate_timestamps": duplicate_timestamps}}


def route_distance(a, b, cfg):
    if abs(a["distance_m"]/b["distance_m"]-1) > cfg["distance_tolerance"]:
        return None
    if abs(a["grade_pct"]-b["grade_pct"]) > cfg["grade_tolerance_pct"]:
        return None
    distances = [haversine(x, y) for x, y in zip(a["anchors"], b["anchors"])]
    if max(distances) > cfg["route_tolerance_m"]:
        return None
    return sum(distances)


def review(prepared, target_id=None, ftp=None, config=None):
    cfg = DEFAULTS | (config or {})
    rides = [r for r in prepared if r.get("windows")]
    candidates = [r for r in rides if float(r["meta"].get("moving_time", 0)) >= 10800]
    if target_id is not None:
        target = next((r for r in prepared if str(r["meta"]["activity_id"]) == str(target_id)), None)
    else:
        target = max(candidates, key=lambda r: (r["meta"]["date"], str(r["meta"]["activity_id"])), default=None)
    base = {"schema_version": 1, "question": "초반 힘 배분과 후반의 같은 구간 출력은 어떻게 연결되는가?",
            "settings": cfg, "ftp_anchor": ftp,
            "claim_scope": "실제로 낸 출력의 관찰 비교이며 최대 능력·피로 원인·보급 부족을 입증하지 않는다.",
            "limitations": ["기온·바람·보급·주행 의도·당일 상태를 통제한 실험이 아니다.",
                            "같은 자전거도 파워미터 교체·교정 상태까지 보장하지 않는다.",
                            "기록 없는 시간의 누적 일량은 알 수 없으므로 recorded_kj는 하한이다.",
                            "FTP 앵커는 출력 분류 기준이며 해당 날짜의 실측 FTP라는 뜻이 아니다."],
            "target": None, "comparisons": [], "comparison_count": 0}
    if target is None:
        return base | {"status": "no_eligible_ride", "next_question": "파워·GPS·케이던스가 있는 3시간 이상 야외 라이딩이 필요한가요?"}
    meta = target["meta"]
    base["target"] = {k: meta[k] for k in ("activity_id", "name", "date", "start_utc", "moving_time",
                                         "elapsed_time", "gear_id", "is_indoor", "device_watts") if k in meta}
    base["quality"] = target.get("quality", {})
    base["early_efforts"] = target.get("early_efforts")
    base["rejections"] = target.get("rejections", {})
    if not meta.get("gear_id"):
        base["limitations"].append("자전거 식별이 없어 다른 날짜의 활동은 비교하지 않는다. 동일 활동 내부만 사용한다.")
    target_date = date.fromisoformat(meta["date"][:10])
    references = []
    for ride in rides:
        m = ride["meta"]
        age = (target_date-date.fromisoformat(m["date"][:10])).days
        if not 0 <= age <= cfg["lookback_days"]:
            continue
        if str(m["activity_id"]) != str(meta["activity_id"]) and (not meta.get("gear_id") or m.get("gear_id") != meta.get("gear_id")):
            continue
        if str(m["activity_id"]) != str(meta["activity_id"]) and age == 0:
            # Without precise starts, another same-day ride is not a safe reference.
            if not m.get("start_utc") or not meta.get("start_utc") or m["start_utc"] >= meta["start_utc"]:
                continue
        for window in ride["windows"]:
            references.append((window, m))
    comparisons, last_end = [], -1
    for window in target["windows"]:
        if window["start_s"] < last_end or window["active_fraction"] < .5 or window["prior_active_s"] < cfg["min_prior_active_s"]:
            continue
        if window["prior_unknown_fraction"] > cfg["max_prior_unknown_fraction"]:
            continue
        matched = []
        for ref, ref_meta in references:
            if ref["prior_unknown_fraction"] > cfg["max_prior_unknown_fraction"]:
                continue
            if ref["activity_id"] == window["activity_id"] and ref["end_s"] > window["start_s"]-cfg["window_s"]:
                continue
            # Compare geometrically similar segments with materially less recorded prior work.
            if window["prior_recorded_kj"]-ref["prior_recorded_kj"] < cfg["min_load_difference_kj"]:
                continue
            distance = route_distance(window, ref, cfg)
            if distance is None:
                continue
            if window["temp_c"] is not None and ref["temp_c"] is not None and abs(window["temp_c"]-ref["temp_c"]) > 5:
                continue
            matched.append((distance, ref, ref_meta))
        # Geometry first, not the reference's highest power (avoids cherry-picking peaks).
        matched.sort(key=lambda x: (x[0], -date.fromisoformat(x[2]["date"][:10]).toordinal(), x[1]["start_s"]))
        picked, seen = [], set()
        for score, ref, ref_meta in matched:
            if ref["activity_id"] in seen:
                continue
            seen.add(ref["activity_id"])
            picked.append((ref, ref_meta, score))
            if len(picked) == 5:
                break
        if not picked:
            continue
        ref_watts = median(x[0]["avg_watts"] for x in picked)
        public = lambda win: {k: v for k, v in win.items() if k != "anchors"}
        comparisons.append({"segment": public(window), "reference_median_watts": ref_watts,
                            "observed_change_pct": 100*(window["avg_watts"]/ref_watts-1) if ref_watts > 0 else None,
                            "references": [{"segment": public(ref), "date": m["date"], "name": m.get("name", ""),
                                            "anchor_distance_sum_m": score} for ref, m, score in picked]})
        last_end = window["end_s"]
    base["comparisons"] = comparisons
    base["comparison_count"] = len(comparisons)
    if comparisons:
        base["status"] = "observations_available"
        base["next_question"] = "표의 비교 구간은 비슷한 목적으로 달렸나요? 보급·바람·기온·휴식 중 평소와 달랐던 것이 있나요?"
        base["next_experiment"] = "초반 기준초과가 의도하지 않은 노력이었음을 확인했다면, 비슷한 코스·주행 목적의 다음 라이딩에서 그 구간의 힘 배분을 바꾸고 후반 같은 구간의 출력·심박·체감을 비교한다."
    else:
        base["status"] = "no_comparable_segments"
        base["next_question"] = "같은 경로의 비교 근거는 아직 없습니다. 아래 초반 기준초과 구간은 계획한 오르막/훈련이었나요, 따라가다 높아진 출력이었나요? 반복 코스와 주행 목적을 기록하면 다음 비교가 가능합니다."
    return base


def elapsed(seconds):
    return f"{int(seconds)//3600}:{int(seconds)%3600//60:02d}:{int(seconds)%60:02d}"


def display_rows(data):
    rows = []
    for pair in data["comparisons"][:5]:
        w = pair["segment"]
        prior = median(r["segment"]["prior_recorded_kj"] for r in pair["references"])
        refs = ", ".join(f"{r['date']} ({elapsed(r['segment']['start_s'])})" for r in pair["references"])
        delta = f"{pair['observed_change_pct']:+.1f}%" if pair["observed_change_pct"] is not None else "미산출"
        rows.append([elapsed(w["start_s"]), f"{w['avg_watts']:.0f}",
                     f"{pair['reference_median_watts']:.0f}", delta,
                     f"{w['prior_recorded_kj']:.0f} / {prior:.0f}", refs])
    return rows


HEADERS = ["대상 시작", "출력 W", "비교 중앙값 W", "출력 차이", "이전 기록 kJ (대상/비교)", "비교 날짜 (시작)"]


def early_lines(data):
    early = data.get("early_efforts")
    if not early:
        return []
    anchor = data.get("ftp_anchor")
    lines = [f"초반 {early['end_s']/60:.0f}분: 기록된 일량 {early['recorded_kj']:.0f}kJ, 파워 커버리지 {early['power_coverage']:.1%}."]
    if anchor:
        lines.append(f"30초 평균 > 기준 {anchor:g}W 판정 시간 {early['above_anchor_s']/60:.1f}분. 20초 이상 이어진 이벤트 {len(early['sustained_events'])}회. 기준은 현재 앵커이며 그날의 실측 FTP를 뜻하지 않음.")
        for e in early["sustained_events"][:5]:
            lines.append(f"{elapsed(e['start_s'])}~{elapsed(e['end_s'])}: 관측구간 평균 {e['avg_watts']:.0f}W, 기준초과 판정 {e['classified_above_s']}초. 의도한 노력인지 확인할 후보.")
    else:
        lines.append("기준 출력 미설정: 기준초과 이벤트는 산출하지 않음.")
    lines.append("30초 평활로 경계가 달라지고 이벤트의 관측구간이 겹칠 수 있음. 횟수와 일량을 합해 추가 소모량으로 해석하지 않음.")
    return lines


def context_lines(data):
    cfg = data["settings"]
    target = data["target"]
    lines = [f"대상: {target['date']} · {target.get('name', '')}",
             f"비교 가능 {data['comparison_count']}개 구간 중 최대 5개를 시간순으로 표시. 출력이 나쁜 구간만 고르지 않음.",
             f"{cfg['window_s']//60}분 창의 진행 방향·위치 5곳·거리·평균 경사를 비교. 같은 경로를 완전히 보장하지 않음."]
    if not target.get("gear_id"):
        lines.append("자전거 미할당: 다른 활동과 비교하지 않고 동일 활동 내부만 비교.")
    if target.get("device_watts") is None:
        lines.append("파워 실측/추정 여부 미확인. 센서 측정 여부를 확인한 뒤 해석할 것.")
    lines.append("누적 kJ는 기록된 파워의 적분으로, 누락된 시간의 일을 포함하지 않는 하한.")
    lines.append(f"비교 조건: 최근 {cfg['lookback_days']}일, 파워 {cfg['min_coverage']:.0%}·이동 {cfg['min_moving']:.0%}·페달링 {cfg['min_pedaling']:.0%} 이상. 내리막 제외. 이전 파워 누락 {cfg['max_prior_unknown_fraction']:.0%} 초과는 제외.")
    quality = data.get("quality", {})
    if quality:
        lines.append(f"전체 경과시간 중 유효 파워 {quality['power_coverage']:.1%}. 일시정지와 수집 누락은 구별할 수 없음.")
    source = data.get("source", {})
    if source.get("note"):
        lines.append(source["note"])
    return lines


def effort_lines(data):
    lines = []
    for pair in data["comparisons"][:5]:
        w = pair["segment"]
        if w["prior_above_anchor_s"] is not None:
            lines.append(f"{elapsed(w['start_s'])} 이전: 기준 {data['ftp_anchor']:g}W를 넘는 30초 평균 {w['prior_above_anchor_s']/60:.1f}분, 20초 이상 지속 이벤트 {w['prior_surge_count']}회.")
        ref_hr = [r["segment"]["heartrate"] for r in pair["references"] if r["segment"]["heartrate"] is not None]
        if w["heartrate"] is not None and ref_hr:
            lines.append(f"{elapsed(w['start_s'])} 구간 평균 심박: 대상 {w['heartrate']:.0f}, 비교 {median(ref_hr):.0f} bpm. 심박 차이만으로 원인을 확정하지 않음.")
    return lines


def markdown(data):
    clean = lambda x: str(x).replace("|", "／").replace("\n", " ").replace("\r", " ")
    lines = ["### 장거리 힘 배분 복기", "", data["claim_scope"], ""]
    if not data.get("target"):
        return "\n".join(lines + [data["next_question"], ""])
    lines += ["- " + clean(s) for s in context_lines(data)] + [""]
    lines += ["**초반 힘 사용 — 다음 라이딩에서 확인할 구간**", ""]
    lines += ["- " + clean(s) for s in early_lines(data)] + [""]
    if data["comparisons"]:
        lines += ["| " + " | ".join(HEADERS) + " |", "|---|---:|---:|---:|---:|---|"]
        lines += ["| " + " | ".join(clean(v) for v in row) + " |" for row in display_rows(data)]
        lines += [""] + ["- " + clean(s) for s in effort_lines(data)]
    lines += ["", "**확인할 맥락:** " + data["next_question"]]
    if data.get("next_experiment"):
        lines += ["", "**다음 비교 후보:** " + data["next_experiment"]]
    lines += [""] + ["- " + clean(s) for s in data["limitations"]]
    lines += ["", "AI 상담: 관찰을 먼저 설명하고 필요한 맥락을 질문한다. 사용자 답변 전에는 원인이나 훈련 처방을 확정하지 않는다.", ""]
    return "\n".join(lines)


def html(data):
    chunks = ['<section class="pacing-review"><h2>장거리 힘 배분 복기</h2><div class="card">',
              '<p>' + escape(data["claim_scope"]) + '</p>']
    if data.get("target"):
        chunks += ['<ul>'] + ['<li>' + escape(s) + '</li>' for s in context_lines(data)] + ['</ul>']
        chunks += ['<h3>초반 힘 사용 — 다음 라이딩에서 확인할 구간</h3><ul>']
        chunks += ['<li>' + escape(s) + '</li>' for s in early_lines(data)] + ['</ul>']
        if data["comparisons"]:
            chunks += ['<div style="overflow-x:auto"><table><thead><tr>']
            chunks += ['<th>' + escape(h) + '</th>' for h in HEADERS]
            chunks += ['</tr></thead><tbody>']
            for row in display_rows(data):
                chunks += ['<tr>'] + ['<td>' + escape(v) + '</td>' for v in row] + ['</tr>']
            chunks += ['</tbody></table></div><ul>']
            chunks += ['<li>' + escape(s) + '</li>' for s in effort_lines(data)] + ['</ul>']
    chunks += ['<p><b>확인할 맥락:</b> ' + escape(data["next_question"]) + '</p>']
    if data.get("next_experiment"):
        chunks += ['<p><b>다음 비교 후보:</b> ' + escape(data["next_experiment"]) + '</p>']
    chunks += ['<details><summary>해석 범위와 한계</summary><ul>']
    chunks += ['<li>' + escape(s) + '</li>' for s in data["limitations"]]
    chunks += ['</ul></details></div></section>']
    return "".join(chunks)

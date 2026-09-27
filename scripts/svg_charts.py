"""
인라인 SVG 차트 생성 — 의존성 0, JS 0.

왜 인라인 SVG 인가:
  리포트는 야간 배치가 하루 한 번 만드는 정적 페이지다. 차트 라이브러리를 CDN 에서
  받아오면 오프라인·LAN 전용 환경에서 깨지고, 서버(2014 i5)에 상주 프로세스를 얹을 이유도 없다.

왜 툴팁이 아니라 값 레이블인가:
  **폰에는 hover 가 없다.** 터치 환경에서 툴팁은 뜨지 않으므로, 값을 직접 그려 넣는 쪽이
  실제로 읽힌다. 보조로 `<title>` 을 달아 데스크톱에서는 네이티브 툴팁도 뜨게 한다.
  덕분에 JS 가 한 줄도 필요 없다.

색은 CSS 변수를 참조한다 — 페이지가 다크/라이트를 전환하면 차트도 같이 따라간다.
"""
from __future__ import annotations

import math
from html import escape


def _fmt(v: float, nd: int = 0) -> str:
    return f"{v:,.{nd}f}"


def _open(w: int, h: int, cls: str) -> str:
    return (
        f'<svg class="chart {cls}" viewBox="0 0 {w} {h}" '
        f'preserveAspectRatio="xMidYMid meet" role="img">'
    )


def power_curve(
    series: dict[str, list[tuple[int, float]]],
    *,
    labels: dict[int, str],
    width: int = 760,
    height: int = 320,
) -> str:
    """파워 커브 — x 는 구간 길이(로그 스케일), y 는 와트.

    로그 스케일을 쓰는 이유: 5초와 4시간이 같은 축에 있다. 선형으로 그리면
    짧은 구간이 전부 왼쪽 끝에 뭉쳐 아무것도 안 보인다.
    """
    pad_l, pad_r, pad_t, pad_b = 52, 16, 18, 42
    iw, ih = width - pad_l - pad_r, height - pad_t - pad_b

    all_pts = [p for pts in series.values() for p in pts]
    if not all_pts:
        return '<p class="empty">데이터 없음</p>'

    xs = [d for d, _ in all_pts]
    ys = [w for _, w in all_pts]
    lx0, lx1 = math.log10(min(xs)), math.log10(max(xs))
    y0, y1 = 0.0, max(ys) * 1.08

    def px(d: int) -> float:
        return pad_l + (math.log10(d) - lx0) / (lx1 - lx0) * iw

    def py(w: float) -> float:
        return pad_t + ih - (w - y0) / (y1 - y0) * ih

    out = [_open(width, height, "power-curve")]

    # y 격자
    step = 50 if y1 <= 400 else 100
    v = 0
    while v <= y1:
        y = py(v)
        out.append(f'<line class="grid" x1="{pad_l}" y1="{y:.1f}" x2="{width-pad_r}" y2="{y:.1f}"/>')
        out.append(f'<text class="axis" x="{pad_l-8}" y="{y+4:.1f}" text-anchor="end">{v}</text>')
        v += step

    # x 눈금 (구간 라벨).
    # 로그 스케일이라 오른쪽 끝(1~4시간)이 촘촘해 라벨이 붙는다 — 실제로 겹쳤다.
    # 직전에 그린 라벨과 최소 간격을 못 지키면 눈금만 찍고 라벨은 건너뛴다.
    last_lbl_x = -1e9
    min_gap = 34.0
    for d in sorted({d for d, _ in all_pts}):
        x = px(d)
        out.append(f'<line class="tick" x1="{x:.1f}" y1="{pad_t+ih}" x2="{x:.1f}" y2="{pad_t+ih+4}"/>')
        if d in labels and (x - last_lbl_x) >= min_gap:
            out.append(
                f'<text class="axis" x="{x:.1f}" y="{pad_t+ih+18:.1f}" '
                f'text-anchor="middle">{escape(labels[d])}</text>'
            )
            last_lbl_x = x

    for i, (name, pts) in enumerate(series.items()):
        pts = sorted(pts)
        if not pts:
            continue
        cls = f"s{i}"
        path = " ".join(
            f"{'M' if j == 0 else 'L'}{px(d):.1f},{py(w):.1f}" for j, (d, w) in enumerate(pts)
        )
        out.append(f'<path class="line {cls}" d="{path}"/>')
        for d, w in pts:
            out.append(
                f'<circle class="pt {cls}" cx="{px(d):.1f}" cy="{py(w):.1f}" r="3">'
                f"<title>{escape(labels.get(d, str(d)))} · {_fmt(w)}W · {escape(name)}</title>"
                f"</circle>"
            )
        # 값 레이블은 첫 시리즈(전 기간)에만 — 둘 다 붙이면 겹친다
        if i == 0:
            last_val_x = -1e9
            for d, w in pts:
                # 로그 스케일 오른쪽 끝은 점 간격이 좁아 값끼리 겹친다 → 최소 간격 미달이면 생략
                if px(d) - last_val_x < 26:
                    continue
                last_val_x = px(d)
                # 첫 점은 왼쪽 축 밖으로, 마지막 점은 오른쪽 밖으로 삐져나간다 → 안쪽으로 당긴다
                x, anchor = px(d), "middle"
                if x - pad_l < 14:
                    x, anchor = pad_l + 2, "start"
                elif (width - pad_r) - x < 14:
                    x, anchor = width - pad_r - 2, "end"
                out.append(
                    f'<text class="val" x="{x:.1f}" y="{py(w)-9:.1f}" '
                    f'text-anchor="{anchor}">{_fmt(w)}</text>'
                )

    # 범례는 오른쪽 위에 둔다 — 파워 커브는 우하향이라 그 구석이 항상 비어 있다.
    # 왼쪽 위에 두면 가장 높은 첫 점의 값 레이블과 겹친다 (실제로 겹쳤다).
    lx, ly = width - pad_r - 118, pad_t + 12
    for i, name in enumerate(series):
        out.append(f'<rect class="sw s{i}" x="{lx}" y="{ly-8+i*16}" width="10" height="10"/>')
        out.append(f'<text class="legend" x="{lx+16}" y="{ly+i*16}">{escape(name)}</text>')

    out.append("</svg>")
    return "".join(out)


def zone_bars(rows: list[tuple[str, float, float]], *, width: int = 760) -> str:
    """존 분포 — 가로 막대. rows = [(라벨, 분, 비율%)]"""
    if not rows:
        return '<p class="empty">데이터 없음</p>'
    row_h, gap, pad_l, pad_r, pad_t = 26, 6, 84, 92, 8
    height = pad_t * 2 + len(rows) * (row_h + gap)
    iw = width - pad_l - pad_r
    mx = max(p for _, _, p in rows) or 1

    out = [_open(width, height, "zones")]
    for i, (label, minutes, pct) in enumerate(rows):
        y = pad_t + i * (row_h + gap)
        w = max(1.0, pct / mx * iw)
        out.append(
            f'<text class="axis" x="{pad_l-8}" y="{y+row_h*0.68:.1f}" text-anchor="end">{escape(label)}</text>'
        )
        out.append(
            f'<rect class="bar z{i+1}" x="{pad_l}" y="{y}" width="{w:.1f}" height="{row_h}" rx="3">'
            f"<title>{escape(label)} · {_fmt(minutes)}분 · {pct:.1f}%</title></rect>"
        )
        out.append(
            f'<text class="val" x="{pad_l+w+8:.1f}" y="{y+row_h*0.68:.1f}">'
            f"{pct:.1f}% · {_fmt(minutes)}분</text>"
        )
    out.append("</svg>")
    return "".join(out)


def durability_slopes(
    rides: list[tuple[str, float, list[float], float]], *, width: int = 760
) -> str:
    """지속력 — 라이딩별 Q1→Q4 NP 기울기. rides = [(라벨, km, [q1..q4], 비율)]"""
    if not rides:
        return '<p class="empty">데이터 없음</p>'
    pad_l, pad_r, pad_t, pad_b = 52, 168, 22, 34
    ih = 200
    height = pad_t + ih + pad_b
    iw = width - pad_l - pad_r

    ys = [v for _, _, qs, _ in rides for v in qs if v]
    y0, y1 = min(ys) * 0.9, max(ys) * 1.05

    def px(q: int) -> float:
        return pad_l + (q - 1) / 3 * iw

    def py(v: float) -> float:
        return pad_t + ih - (v - y0) / (y1 - y0) * ih

    # 끝점 라벨은 y 가 비슷하면 서로 겹친다 (실제로 겹쳤다).
    # y 순으로 정렬해 최소 간격을 강제하고, 아래로 밀려 차트를 벗어나면 전체를 위로 당긴다.
    def spread(items: list[tuple[float, int]], gap: float = 13.0) -> dict[int, float]:
        ordered = sorted(items)
        placed: list[tuple[float, int]] = []
        for y, idx in ordered:
            if placed and y < placed[-1][0] + gap:
                y = placed[-1][0] + gap
            placed.append((y, idx))
        if placed:
            overflow = placed[-1][0] - (pad_t + ih)
            if overflow > 0:
                placed = [(y - overflow, i) for y, i in placed]
        return {i: y for y, i in placed}

    label_ys = spread(
        [
            (py([v for v in qs if v][-1]), i)
            for i, (_, _, qs, _) in enumerate(rides)
            if len([v for v in qs if v]) >= 2
        ]
    )

    out = [_open(width, height, "durability")]
    for q in range(1, 5):
        x = px(q)
        out.append(f'<line class="grid" x1="{x:.1f}" y1="{pad_t}" x2="{x:.1f}" y2="{pad_t+ih}"/>')
        out.append(
            f'<text class="axis" x="{x:.1f}" y="{pad_t+ih+18}" text-anchor="middle">Q{q}</text>'
        )
    out.append(f'<text class="axis" x="{pad_l-8}" y="{py(y1)+10:.1f}" text-anchor="end">{_fmt(y1)}W</text>')
    out.append(f'<text class="axis" x="{pad_l-8}" y="{py(y0):.1f}" text-anchor="end">{_fmt(y0)}W</text>')

    for i, (label, km, qs, ratio) in enumerate(rides):
        cls = f"d{i % 6}"
        pts = [(q, v) for q, v in enumerate(qs, 1) if v]
        if len(pts) < 2:
            continue
        path = " ".join(f"{'M' if j == 0 else 'L'}{px(q):.1f},{py(v):.1f}" for j, (q, v) in enumerate(pts))
        out.append(
            f'<path class="line {cls}" d="{path}"><title>{escape(label)} · {_fmt(km)}km · 비율 {ratio:.3f}</title></path>'
        )
        for q, v in pts:
            out.append(f'<circle class="pt {cls}" cx="{px(q):.1f}" cy="{py(v):.1f}" r="2.5"/>')
        lq, lv = pts[-1]
        ly = label_ys.get(i, py(lv))
        # 라벨을 밀어낸 만큼 선을 이어 어느 선의 라벨인지 잃지 않게 한다
        if abs(ly - py(lv)) > 1.5:
            out.append(
                f'<path class="line {cls} leader" d="M{px(lq):.1f},{py(lv):.1f} '
                f'L{px(lq)+6:.1f},{ly-4:.1f}" stroke-dasharray="2 2"/>'
            )
        out.append(
            f'<text class="slope-lbl {cls}" x="{px(lq)+9:.1f}" y="{ly:.1f}">'
            f"{escape(label)} {ratio:.2f}</text>"
        )
    out.append("</svg>")
    return "".join(out)


def year_bars(rows: list[tuple[str, float, float]], *, width: int = 760, height: int = 240) -> str:
    """연도별 거리 — 세로 막대 2계열. rows = [(연도, 사이클km, 수영km)]"""
    if not rows:
        return '<p class="empty">데이터 없음</p>'
    pad_l, pad_r, pad_t, pad_b = 58, 16, 24, 34
    iw, ih = width - pad_l - pad_r, height - pad_t - pad_b
    mx = max(max(c, s) for _, c, s in rows) or 1
    slot = iw / len(rows)
    bw = min(34.0, slot * 0.34)

    out = [_open(width, height, "years")]
    for g in range(5):
        v = mx * g / 4
        y = pad_t + ih - v / mx * ih
        out.append(f'<line class="grid" x1="{pad_l}" y1="{y:.1f}" x2="{width-pad_r}" y2="{y:.1f}"/>')
        out.append(f'<text class="axis" x="{pad_l-8}" y="{y+4:.1f}" text-anchor="end">{_fmt(v)}</text>')

    for i, (year, cyc, swim) in enumerate(rows):
        cx = pad_l + slot * (i + 0.5)
        for j, (v, cls, nm) in enumerate(((cyc, "s0", "사이클"), (swim, "s1", "수영"))):
            if not v:
                continue
            h = max(1.0, v / mx * ih)
            x = cx - bw + j * bw
            out.append(
                f'<rect class="bar {cls}" x="{x:.1f}" y="{pad_t+ih-h:.1f}" width="{bw-3:.1f}" height="{h:.1f}" rx="2">'
                f"<title>{escape(year)} {nm} {_fmt(v,1)}km</title></rect>"
            )
        out.append(
            f'<text class="axis" x="{cx:.1f}" y="{pad_t+ih+18}" text-anchor="middle">{escape(year)}</text>'
        )
        if cyc:
            out.append(
                f'<text class="val" x="{cx-bw/2:.1f}" y="{pad_t+ih-cyc/mx*ih-6:.1f}" '
                f'text-anchor="middle">{_fmt(cyc)}</text>'
            )
    out.append("</svg>")
    return "".join(out)

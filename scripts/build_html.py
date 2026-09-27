#!/usr/bin/env python3
"""
Gold 마트 → HTML 리포트 (인라인 SVG 차트).

`build_report.py` 가 만드는 마크다운은 **AI 상담 입력**용이고,
이 HTML 은 **사람이 브라우저·폰으로 보는** 용도다. 둘 다 같은 마트에서 나온다.

설계:
  · 의존성 0. CDN·JS 없음. LAN 에서 서빙하므로 외부 요청이 실패할 여지를 두지 않는다.
  · 차트는 인라인 SVG (scripts/svg_charts.py). 값을 직접 그려 넣는다 —
    폰에는 hover 가 없어 툴팁만으로는 읽을 수 없다.
  · 라이트/다크 자동 전환 (prefers-color-scheme).
  · 생성 시점에 숫자를 박는다. 별도 앱이 마트를 다시 조회하는 구조가 아니라
    "화면과 숫자가 어긋나는" 종류의 버그가 생기지 않는다.

사용법:
  source lakehouse/env.sh
  lakehouse/.venv/bin/python lakehouse/scripts/build_html.py --out /path/to/index.html
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from html import escape
from pathlib import Path

LAKEHOUSE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAKEHOUSE / "scripts"))
import svg_charts as sc  # noqa: E402
from spark_session import get_spark  # noqa: E402

CSS = """
:root{
  --bg:#fbfbfa; --fg:#1d1d1b; --muted:#6b6b66; --line:#e3e3df; --card:#fff;
  --s0:#c2410c; --s1:#0e7490; --warn:#b45309; --good:#15803d;
  --z1:#94a3b8; --z2:#60a5fa; --z3:#34d399; --z4:#fbbf24; --z5:#fb923c; --z6:#f87171; --z7:#c084fc;
}
@media (prefers-color-scheme:dark){
  :root{ --bg:#16161a; --fg:#ececea; --muted:#9a9a94; --line:#2c2c31; --card:#1e1e23;
         --s0:#fb923c; --s1:#38bdf8; --warn:#fbbf24; --good:#4ade80; }
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
  font:15px/1.6 -apple-system,BlinkMacSystemFont,"Apple SD Gothic Neo","Noto Sans KR",sans-serif}
.wrap{max-width:820px;margin:0 auto;padding:20px 16px 64px}
h1{font-size:1.5rem;margin:.2em 0 .1em}
h2{font-size:1.15rem;margin:2.2em 0 .6em;padding-bottom:.3em;border-bottom:1px solid var(--line)}
h3{font-size:1rem;margin:1.6em 0 .4em;color:var(--muted)}
.meta{color:var(--muted);font-size:.85rem;margin-bottom:.4em}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(118px,1fr));gap:10px;margin:18px 0}
.kpi,.card{min-width:0}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.kpi .n{font-size:1.35rem;font-weight:650;letter-spacing:-.02em}
.kpi .l{color:var(--muted);font-size:.76rem;margin-top:2px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;margin:12px 0;overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:.86rem}
th,td{padding:6px 8px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap}
th:first-child,td:first-child{text-align:left}
th{color:var(--muted);font-weight:600;font-size:.78rem}
tbody tr:last-child td{border-bottom:none}
.note{background:var(--card);border-left:3px solid var(--warn);border-radius:0 8px 8px 0;
  padding:10px 14px;margin:14px 0;font-size:.88rem;color:var(--muted)}
.note b{color:var(--fg)}
.chart{width:100%;height:auto;display:block}
.chart .grid{stroke:var(--line);stroke-width:1}
.chart .tick{stroke:var(--muted);stroke-width:1}
.chart .axis{fill:var(--muted);font-size:11px}
.chart .val{fill:var(--muted);font-size:10px}
.chart .legend{fill:var(--fg);font-size:11px}
.chart .line{fill:none;stroke-width:2;stroke-linejoin:round}
.chart .pt{stroke:var(--bg);stroke-width:1}
.line.s0,.pt.s0{stroke:var(--s0)} .pt.s0{fill:var(--s0)} rect.sw.s0{fill:var(--s0)} rect.bar.s0{fill:var(--s0)}
.line.s1,.pt.s1{stroke:var(--s1)} .pt.s1{fill:var(--s1)} rect.sw.s1{fill:var(--s1)} rect.bar.s1{fill:var(--s1)}
.bar.z1{fill:var(--z1)}.bar.z2{fill:var(--z2)}.bar.z3{fill:var(--z3)}.bar.z4{fill:var(--z4)}
.bar.z5{fill:var(--z5)}.bar.z6{fill:var(--z6)}.bar.z7{fill:var(--z7)}
.line.d0,.pt.d0,.slope-lbl.d0{stroke:var(--s0)} .pt.d0{fill:var(--s0)} .slope-lbl.d0{fill:var(--s0);stroke:none}
.line.d1,.pt.d1{stroke:var(--s1)} .pt.d1{fill:var(--s1)} .slope-lbl.d1{fill:var(--s1);stroke:none}
.line.d2,.pt.d2{stroke:var(--z4)} .pt.d2{fill:var(--z4)} .slope-lbl.d2{fill:var(--z4);stroke:none}
.line.d3,.pt.d3{stroke:var(--good)} .pt.d3{fill:var(--good)} .slope-lbl.d3{fill:var(--good);stroke:none}
.line.d4,.pt.d4{stroke:var(--z7)} .pt.d4{fill:var(--z7)} .slope-lbl.d4{fill:var(--z7);stroke:none}
.line.d5,.pt.d5{stroke:var(--muted)} .pt.d5{fill:var(--muted)} .slope-lbl.d5{fill:var(--muted);stroke:none}
.slope-lbl{font-size:10px}
.leader{stroke-width:1;opacity:.5}
.empty{color:var(--muted);font-size:.88rem;margin:8px 0}
@media (max-width:480px){
  .wrap{padding:16px 12px 48px}
  h1{font-size:1.3rem}
  .kpi .n{font-size:1.15rem}
  th,td{padding:5px 6px;font-size:.8rem}
}
footer{margin-top:40px;padding-top:14px;border-top:1px solid var(--line);color:var(--muted);font-size:.78rem}
code{background:var(--card);border:1px solid var(--line);border-radius:4px;padding:1px 5px;font-size:.85em}
"""

DUR_LABELS = {5:"5초",15:"15초",30:"30초",60:"1분",120:"2분",300:"5분",480:"8분",
              720:"12분",1200:"20분",1800:"30분",3600:"1시간",7200:"2시간",
              10800:"3시간",14400:"4시간"}


def f(v, nd=1, dash="·"):
    return dash if v is None else f"{float(v):,.{nd}f}"


def table(headers: list[str], rows: list[list]) -> str:
    h = "".join(f"<th>{escape(x)}</th>" for x in headers)
    b = "".join(
        "<tr>" + "".join(f"<td>{'' if c is None else escape(str(c))}</td>" for c in r) + "</tr>"
        for r in rows
    )
    return f'<div class="card"><table><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table></div>'


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=str(LAKEHOUSE.parent / "reports/index.html"))
    args = ap.parse_args()

    spark = get_spark("build-html")
    spark.sparkContext.setLogLevel("ERROR")
    q = lambda s: spark.sql(s).collect()  # noqa: E731
    P: list[str] = []
    w = P.append

    meta = q("""SELECT count(*) n, min(start_date_key) d0, max(start_date_key) d1,
                       count_if(is_duplicate) dups FROM tc.silver.activities""")[0]
    ftp = q("SELECT ftp_watts FROM tc.gold.ftp_log WHERE is_current")[0]["ftp_watts"]
    sports = {r["sport"]: r for r in q("SELECT * FROM tc.gold.mart_sport_totals")}
    cyc = sports.get("cycling")

    # viewport 메타가 없으면 모바일 브라우저가 폭 980px 을 가정하고 축소해 버린다.
    w('<meta charset="utf-8">')
    w('<meta name="viewport" content="width=device-width, initial-scale=1">')
    w(f"<title>훈련 리포트 {date.today().isoformat()}</title>")
    w(f"<style>{CSS}</style>")
    w('<div class="wrap">')
    w(f"<h1>훈련 리포트</h1>")
    w(f'<div class="meta">{meta["d0"]} ~ {meta["d1"]} · 활동 {meta["n"]:,}건 '
      f'(중복 {meta["dups"]}건 제외) · 생성 {date.today().isoformat()}</div>')

    # KPI
    w('<div class="kpis">')
    for label, val in [
        ("사이클 누적", f'{f(cyc["distance_km"])} km' if cyc else "·"),
        ("사이클 시간", f'{f(cyc["moving_h"])} h' if cyc else "·"),
        ("누적 고도", f'{f(cyc["elev_m"],0)} m' if cyc else "·"),
        ("앵커 FTP", f"{ftp} W"),
    ]:
        w(f'<div class="kpi"><div class="n">{escape(val)}</div><div class="l">{escape(label)}</div></div>')
    w("</div>")

    # ── 파워 커브 ──
    w("<h2>파워 커브</h2>")
    curve = q("SELECT * FROM tc.gold.mart_power_curve")
    series = {"전 기간": [], "최근 90일": []}
    key = {"all_time": "전 기간", "recent_90d": "최근 90일"}
    for r in curve:
        series[key[r["window_label"]]].append((r["duration_s"], float(r["best_watts"])))
    w(f'<div class="card">{sc.power_curve(series, labels=DUR_LABELS)}</div>')
    w('<div class="note">구간이 활동보다 길면 제외된다 — 10분 라이딩에서 '
      '"20분 최고 파워"가 나오면 그건 전체 평균일 뿐이다. '
      '그래서 <b>긴 구간은 유효 라이딩 수가 적다</b>.</div>')

    # ── FTP 추정 ──
    w("<h2>FTP 추정</h2>")
    est = q("SELECT * FROM tc.gold.mart_ftp_estimate ORDER BY window_label DESC")
    w(table(["범위", "20분 최고", "20분×0.95", "CP 모델", "W′", "ftp-log", "차이(CP)"],
            [[key.get(r["window_label"], r["window_label"]),
              f'{f(r["p20"],0)} W', f'{f(r["ftp_20min_x095"],0)} W',
              f'{f(r["cp_watts"],0)} W', f'{f(r["w_prime_joules"],0)} J',
              f'{r["ftp_log_watts"]} W',
              f'{f(r["diff_cp_vs_log"],0)} W'] for r in est]))
    a = next((r for r in est if r["window_label"] == "all_time"), None)
    if a and a["cp_watts"]:
        w(f'<div class="note"><b>두 방법이 {f(a["method_spread"],0)}W 안에서 일치</b>한다 '
          f'(20분×0.95 {f(a["ftp_20min_x095"],0)}W vs CP {f(a["cp_watts"],0)}W). '
          f'서로 다른 수식이 같은 답을 내면 데이터가 일관됐다는 뜻이다. '
          f'다만 두 방법 모두 <b>최대 노력 기록이 데이터에 있어야</b> 유효하다 — '
          f'없으면 하한으로 읽어야 한다. 판단 앵커는 <code>ftp-log.md</code>.</div>')

    # ── 존 분포 ──
    w("<h2>파워 존 분포 <span class='meta'>최근 90일</span></h2>")
    zones = q("""SELECT zone, zone_label, sum(seconds)/60.0 minutes,
                        100.0*sum(seconds)/sum(sum(seconds)) OVER () pct
                 FROM tc.gold.mart_power_zones
                 WHERE start_date_key >= date_add(current_date(), -90)
                 GROUP BY zone, zone_label ORDER BY zone""")
    w(f'<div class="card">{sc.zone_bars([(r["zone_label"], float(r["minutes"]), float(r["pct"])) for r in zones])}</div>')
    w(f'<div class="note">앵커 FTP {ftp}W 기준 Coggan 7존. '
      'Z1 에는 코스팅·정지(0W)가 포함된다 — 기성 도구와 같은 관행이다.</div>')

    # ── 지속력 ──
    w("<h2>지속력 <span class='meta'>3시간+ 라이딩, 경과시간 4분할 NP</span></h2>")
    dur = q("""SELECT name, start_date_key, distance_km, np_q1, np_q2, np_q3, np_q4,
                      durability_ratio
               FROM tc.gold.mart_ride_durability ORDER BY distance_km DESC LIMIT 6""")
    rides = [(f'{r["name"][:10]}', float(r["distance_km"]),
              [float(r[f"np_q{i}"]) if r[f"np_q{i}"] else None for i in (1, 2, 3, 4)],
              float(r["durability_ratio"])) for r in dur]
    w(f'<div class="card">{sc.durability_slopes(rides)}</div>')
    avg = q("SELECT avg(durability_ratio) a, count(*) n FROM tc.gold.mart_ride_durability")[0]
    w(f'<div class="note">3시간+ {avg["n"]}건 평균 비율 <b>{f(avg["a"],3)}</b>. '
      '1.0 이면 페이스 균일, 0.8 이하면 후반 페이드가 크다 — 보급·페이싱·체력 중 하나를 본다.</div>')
    w(table(["일자", "라이딩", "km", "Q1", "Q2", "Q3", "Q4", "비율"],
            [[str(r["start_date_key"]), r["name"][:22], f(r["distance_km"], 0),
              f(r["np_q1"], 0), f(r["np_q2"], 0), f(r["np_q3"], 0), f(r["np_q4"], 0),
              f(r["durability_ratio"], 3)] for r in dur]))

    # ── 연도별 ──
    w("<h2>연도별 거리</h2>")
    ys = q("""SELECT year,
                     max(CASE WHEN sport='cycling' THEN distance_km END) c,
                     max(CASE WHEN sport='swimming' THEN distance_km END) s
              FROM tc.gold.mart_yearly_summary GROUP BY year ORDER BY year""")
    w(f'<div class="card">{sc.year_bars([(str(r["year"]), float(r["c"] or 0), float(r["s"] or 0)) for r in ys])}</div>')

    # ── 종목별 ──
    w("<h2>종목별 누적</h2>")
    order = ["cycling", "swimming", "running", "walking", "other"]
    w(table(["종목", "활동", "거리 km", "시간 h", "고도 m"],
            [[s, sports[s]["activities"], f(sports[s]["distance_km"]),
              f(sports[s]["moving_h"]), f(sports[s]["elev_m"], 0)]
             for s in order if s in sports]))

    # ── 중복 제외 ──
    dups = q("""SELECT d.start_date_key day, d.device_name ddev, d.distance_km dkm,
                       p.device_name pdev, p.distance_km pkm
                FROM tc.silver.activities d
                JOIN tc.silver.activities p ON d.duplicate_of_activity_id = p.activity_id
                WHERE d.is_duplicate ORDER BY d.start_date_key""")
    if dups:
        w("<h2>중복 제외 내역</h2>")
        w('<div class="note">헤드유닛 두 대가 같은 라이딩을 동시에 기록해 둘 다 올라간 건이다. '
          '겹치는 활동을 묶어 <b>가장 오래 기록된 쪽</b>을 남긴다.</div>')
        w(table(["일자", "남김", "버림"],
                [[str(r["day"]), f'{r["pdev"]} {f(r["pkm"])}km', f'{r["ddev"]} {f(r["dkm"])}km']
                 for r in dups]))

    w('<footer>생성: <code>lakehouse/scripts/build_html.py</code> — '
      'Bronze/Silver/Gold(Iceberg) + dbt 마트 기반. 인라인 SVG, 외부 의존 없음.<br>'
      'AI 상담 입력용 마크다운: <code>reports/latest_overview.md</code></footer>')
    w("</div>")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    html = "\n".join(P)
    out.write_text(html, encoding="utf-8")
    print(f"[✓] HTML 리포트: {out}  ({len(html)/1024:.0f} KB)")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

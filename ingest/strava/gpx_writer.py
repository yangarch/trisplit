"""
Strava streams → GPX 1.1 직렬화.

Strava 는 공식적인 GPX export 가 없어서, /activities/{id}/streams 의 latlng/time/altitude/heartrate/cadence/watts/temp
스트림을 받아 GPX로 합성한다.

- 실내 활동(풀 수영, 인도어 라이드 등)은 latlng 가 없어서 빈 문자열 반환 → 호출 측에서 GPX 저장 스킵.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

GPX_NS = "http://www.topografix.com/GPX/1/1"
TPX_NS = "http://www.garmin.com/xmlschemas/TrackPointExtension/v1"

ET.register_namespace("", GPX_NS)
ET.register_namespace("gpxtpx", TPX_NS)


def _se(parent: ET.Element, ns: str, tag: str, text: str | None = None, **attrib) -> ET.Element:
    el = ET.SubElement(parent, f"{{{ns}}}{tag}", attrib=attrib)
    if text is not None:
        el.text = text
    return el


def _stream(streams: dict, key: str) -> list:
    s = streams.get(key)
    if not s:
        return []
    # /streams?key_by_type=true 응답 형태: {"latlng": {"data": [...], ...}, ...}
    if isinstance(s, dict):
        return s.get("data", []) or []
    return s or []


def streams_to_gpx(activity: dict, streams: dict) -> str:
    """
    activity: /activities/{id} 응답
    streams: /activities/{id}/streams?key_by_type=true 응답
    반환: GPX 문자열. GPS 데이터 없으면 빈 문자열.
    """
    latlng = _stream(streams, "latlng")
    if not latlng:
        return ""

    times = _stream(streams, "time")
    alts = _stream(streams, "altitude")
    hrs = _stream(streams, "heartrate")
    cads = _stream(streams, "cadence")
    temps = _stream(streams, "temp")
    # watts 는 GPX 표준 확장이 없어 일단 트랙 메타에 평균만 기록 (per-point 는 JSON 에)

    start_iso = activity.get("start_date") or ""
    try:
        start_time = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    except ValueError:
        start_time = datetime.now(tz=timezone.utc)

    gpx = ET.Element(
        f"{{{GPX_NS}}}gpx",
        attrib={"version": "1.1", "creator": "training-companion"},
    )

    metadata = _se(gpx, GPX_NS, "metadata")
    _se(metadata, GPX_NS, "name", activity.get("name", ""))
    _se(metadata, GPX_NS, "time", start_iso)

    trk = _se(gpx, GPX_NS, "trk")
    _se(trk, GPX_NS, "name", activity.get("name", ""))
    _se(trk, GPX_NS, "type", activity.get("type", ""))
    trkseg = _se(trk, GPX_NS, "trkseg")

    for i, ll in enumerate(latlng):
        if not ll or len(ll) < 2:
            continue
        lat, lng = ll[0], ll[1]
        trkpt = _se(
            trkseg,
            GPX_NS,
            "trkpt",
            lat=f"{lat:.6f}",
            lon=f"{lng:.6f}",
        )
        if i < len(alts) and alts[i] is not None:
            _se(trkpt, GPX_NS, "ele", f"{alts[i]:.1f}")
        if i < len(times):
            t = start_time + timedelta(seconds=times[i])
            _se(
                trkpt,
                GPX_NS,
                "time",
                t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            )

        ext_pairs: list[tuple[str, str]] = []
        if i < len(hrs) and hrs[i] is not None:
            ext_pairs.append(("hr", str(int(hrs[i]))))
        if i < len(cads) and cads[i] is not None:
            ext_pairs.append(("cad", str(int(cads[i]))))
        if i < len(temps) and temps[i] is not None:
            ext_pairs.append(("atemp", str(int(temps[i]))))
        if ext_pairs:
            ext = _se(trkpt, GPX_NS, "extensions")
            tpx = _se(ext, TPX_NS, "TrackPointExtension")
            for tag, val in ext_pairs:
                _se(tpx, TPX_NS, tag, val)

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(gpx, encoding="unicode")

#!/usr/bin/env python3
"""Durably enqueue validated bundles; retries are idempotent by activity ID."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from export_coach import atomic_json


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never forward the ingest bearer token to a redirect target.


def catalog_endpoint(url: str, token: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or not parsed.path.endswith("/api/workouts")):
        raise ValueError("Invalid coach endpoint")
    if len(token) < 32:
        raise ValueError("Coach ingest token must contain at least 32 characters")
    return url.removesuffix("/api/workouts") + "/api/swim-catalog"


def post_json(url: str, token: str, data: bytes) -> dict:
    request = urllib.request.Request(url, data=gzip.compress(data), headers={
        "Content-Type": "application/json", "Content-Encoding": "gzip", "Authorization": f"Bearer {token}"})
    with urllib.request.build_opener(NoRedirect).open(request, timeout=30) as response:
        return json.loads(response.read(4096))


def publish_swim_catalog(outbox: Path, url: str, token: str) -> int:
    endpoint = catalog_endpoint(url, token)
    data = (outbox / "swim-catalog.json").read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    state_path = outbox / "swim-catalog-published.json"
    if state_path.exists() and json.loads(state_path.read_text()) == digest:
        return 0
    try:
        reply = post_json(endpoint, token, data)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        raise RuntimeError("Coach swim catalog request failed") from None
    count = len(json.loads(data))
    if reply.get("status") != "stored" or reply.get("count") != count:
        raise RuntimeError("Coach did not acknowledge swim catalog")
    atomic_json(state_path, digest)
    return count


def publish(outbox: Path, url: str, token: str) -> int:
    catalog_endpoint(url, token)
    manifest = json.loads((outbox / "manifest.json").read_text())
    if manifest.get("schema_version") != 1:
        raise ValueError("Invalid manifest version")
    state_path = outbox / "published.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    count = 0
    for name in manifest["files"]:
        if not name.endswith(".json") or not name[:-5].isdigit():
            raise ValueError("Invalid bundle filename")
        data = (outbox / name).read_bytes()
        bundle = json.loads(data)
        activity_id = str(bundle["activity"]["id"])
        if name != f"{activity_id}.json" or bundle["provenance"]["pipeline_run"] != manifest["pipeline_run"]:
            raise ValueError("Bundle does not match manifest")
        # Input updates after analysis do not trigger repeat model calls or messages.
        if activity_id in state:
            continue
        try:
            reply = post_json(url, token, data)
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            # Never print URLs, HTTP error bodies or request headers.
            raise RuntimeError(f"Coach queue request failed for activity {activity_id}") from None
        if reply.get("status") not in ("queued", "already_queued", "already_processed", "before_cutover"):
            raise RuntimeError("Coach did not acknowledge durable enqueue")
        state[activity_id] = {"status": reply["status"], "sha256": hashlib.sha256(data).hexdigest()}
        atomic_json(state_path, state)
        count += 1
    if manifest.get("swim_catalog") is True:
        publish_swim_catalog(outbox, url, token)
    return count


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=os.environ.get("COACH_OUTBOX"))
    args = ap.parse_args()
    if not args.out:
        ap.error("COACH_OUTBOX or --out required")
    count = publish(Path(args.out), os.environ.get("COACH_API_URL", ""), os.environ.get("COACH_INGEST_TOKEN", ""))
    print(f"[coach] {count} activities acknowledged by private queue")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

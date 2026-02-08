#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
from pathlib import Path
from typing import Dict

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.flink.common import (  # noqa: E402
    default_rest_url,
    http_request,
    list_jobs_overview,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get Flink job status")
    parser.add_argument("--rest-url", default=default_rest_url(), help="Flink REST base URL")
    parser.add_argument("targets", nargs="*", help="job id or all")
    parser.add_argument("--job-id", default=None, help="job id")
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def job_status(rest_url: str, job_id: str) -> dict:
    url = urllib.parse.urljoin(rest_url, f"/jobs/{job_id}")
    status, resp = http_request(url, timeout=30)
    if status != 200:
        raise RuntimeError(f"flink job status failed: HTTP {status}")
    return json.loads(resp.decode("utf-8"))


def main() -> int:
    args = parse_args()
    requested_targets = list(args.targets)
    if args.job_id:
        requested_targets.append(args.job_id)

    if not requested_targets or any(t.lower() == "all" for t in requested_targets):
        jobs = list_jobs_overview(args.rest_url)
        if args.output_json:
            print(json.dumps({"status": "ok", "jobs": jobs}, ensure_ascii=True))
        else:
            if not jobs:
                print("no jobs found")
            for job in jobs:
                name = job.get("name") or ""
                jid = job.get("jid") or job.get("id") or ""
                state = job.get("state") or ""
                print(f"{name}: {state} ({jid})")
        return 0

    job_ids: Dict[str, str] = {}
    for raw in requested_targets:
        token = raw.strip()
        if not token:
            continue
        job_ids[token] = token

    payload: Dict[str, dict] = {}
    for job_id in job_ids.values():
        payload[job_id] = job_status(args.rest_url, job_id)
    if args.output_json:
        print(json.dumps({"status": "ok", "response": payload}, ensure_ascii=True))
    else:
        for job_id, detail in payload.items():
            state = detail.get("state") if isinstance(detail, dict) else None
            if state:
                print(f"{job_id}: {state}")
            else:
                print(f"{job_id}: status fetched")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=True))
        sys.exit(1)

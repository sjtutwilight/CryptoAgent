#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
from pathlib import Path
from typing import Dict

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.flink.common import (  # noqa: E402
    JOB_KEYWORDS,
    JOB_NAME_PATTERNS,
    default_rest_url,
    http_request,
    list_jobs_overview,
    resolve_job_name_patterns,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cancel Flink job")
    parser.add_argument("--rest-url", default=default_rest_url(), help="Flink REST base URL")
    parser.add_argument("targets", nargs="*", help="job id, keyword, or all")
    parser.add_argument("--job-id", default=None, help="job id")
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def cancel_job(rest_url: str, job_id: str) -> str:
    url = urllib.parse.urljoin(rest_url, f"/jobs/{job_id}/cancel")
    try:
        status, _ = http_request(url, method="PATCH", timeout=30)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            status = None
        else:
            raise
    else:
        if status == 202:
            return "canceled"
        if status != 404:
            raise RuntimeError(f"flink cancel failed: HTTP {status}")

    fallback_url = urllib.parse.urljoin(rest_url, f"/jobs/{job_id}")
    payload = json.dumps({"state": "CANCELED"}).encode("utf-8")
    try:
        status, _ = http_request(
            fallback_url,
            method="PATCH",
            data=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return "not_found"
        raise
    if status != 202:
        raise RuntimeError(f"flink cancel failed: HTTP {status}")
    return "canceled"


def main() -> int:
    args = parse_args()
    requested_targets = list(args.targets)
    if args.job_id:
        requested_targets.append(args.job_id)
    if not requested_targets:
        requested_targets.append("all")

    use_all = False
    keyword_targets: list[str] = []
    explicit_job_ids: list[str] = []
    for raw in requested_targets:
        token = raw.strip()
        if not token:
            continue
        lower = token.lower()
        if lower == "all":
            use_all = True
            continue
        if lower in JOB_KEYWORDS:
            keyword_targets.append(lower)
            continue
        explicit_job_ids.append(token)

    job_ids: Dict[str, str] = {}
    missing_entries: list[str] = []
    jobs_overview: list[dict] = []
    if use_all or keyword_targets:
        jobs_overview = list_jobs_overview(args.rest_url)

    if use_all:
        for job in jobs_overview:
            job_id = job.get("jid") or job.get("id")
            if not job_id:
                continue
            job_ids[job_id] = job_id

    if keyword_targets:
        patterns, unknown = resolve_job_name_patterns(keyword_targets)
        if unknown:
            options = ", ".join(sorted(JOB_NAME_PATTERNS.keys()))
            raise ValueError(f"unknown job keyword(s): {', '.join(unknown)} (options: {options})")

        for job in jobs_overview:
            name = str(job.get("name") or "")
            name_lower = name.lower()
            for pattern in patterns:
                if pattern.lower() in name_lower:
                    job_id = job.get("jid") or job.get("id")
                    if job_id:
                        job_ids[job_id] = job_id
                    break
        if not job_ids:
            missing_entries.extend(keyword_targets)

    for job_id in explicit_job_ids:
        job_ids[job_id] = job_id

    if not job_ids:
        if missing_entries:
            if args.output_json:
                print(
                    json.dumps(
                        {
                            "status": "ok",
                            "job_ids": [],
                            "results": {},
                            "missing_entries": missing_entries,
                        },
                        ensure_ascii=True,
                    )
                )
            else:
                print(f"no running jobs matched: {', '.join(missing_entries)}")
            return 0
        if args.output_json:
            print(json.dumps({"status": "ok", "job_ids": [], "results": {}}, ensure_ascii=True))
        else:
            print("no running jobs found")
        return 0

    results: Dict[str, str] = {}
    for job_id in job_ids.values():
        results[job_id] = cancel_job(args.rest_url, job_id)
    if args.output_json:
        payload = {"status": "ok", "job_ids": list(job_ids.values()), "results": results}
        if missing_entries:
            payload["missing_entries"] = missing_entries
        print(json.dumps(payload, ensure_ascii=True))
    else:
        canceled = [job_id for job_id, status in results.items() if status == "canceled"]
        missing = [job_id for job_id, status in results.items() if status == "not_found"]
        if canceled:
            label = "job canceled" if len(canceled) == 1 else "jobs canceled"
            print(f"{label}: {', '.join(canceled)}")
        if missing:
            print(f"jobs already finished: {', '.join(missing)}")
        if missing_entries:
            print(f"jobs not matched: {', '.join(missing_entries)}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=True))
        sys.exit(1)

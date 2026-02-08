#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.flink.common import (  # noqa: E402
    JOB_KEYWORDS,
    default_rest_url,
    resolve_jar_path,
    resolve_job_keywords,
    write_last_jobs,
)
from automation.ops.flink.list import list_jars  # noqa: E402
from automation.ops.flink.run import run_jar  # noqa: E402
from automation.ops.flink.upload import upload_jar  # noqa: E402

DEFAULT_JAR = os.getenv("FLINK_JAR_PATH")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload JAR and run Flink job by keyword")
    parser.add_argument("keywords", nargs="+", help="job keywords")
    parser.add_argument("--jar", default=DEFAULT_JAR, help="path to JAR file (defaults to latest build)")
    parser.add_argument("--rest-url", default=default_rest_url(), help="Flink REST base URL")
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def _resolve_entry_classes(keywords: List[str]) -> List[str]:
    entry_classes, unknown = resolve_job_keywords(keywords)
    if unknown:
        options = ", ".join(sorted(JOB_KEYWORDS.keys()))
        raise ValueError(f"unknown job keyword(s): {', '.join(unknown)} (options: {options})")
    return entry_classes


def _find_jar_id(rest_url: str, jar_name: str) -> str:
    jars = list_jars(rest_url)
    for jar in jars.get("files", []):
        if jar.get("name") == jar_name:
            jar_id = jar.get("id")
            if jar_id:
                return jar_id
    raise RuntimeError(f"jar id not found for {jar_name}")


def main() -> int:
    args = parse_args()
    entry_classes = _resolve_entry_classes(args.keywords)
    jar_path = resolve_jar_path(args.jar)

    upload_jar(args.rest_url, jar_path)
    jar_id = _find_jar_id(args.rest_url, jar_path.name)

    jobs: Dict[str, str] = {}
    for entry_class in entry_classes:
        resp = run_jar(args.rest_url, jar_id, entry_class)
        job_id = resp.get("jobid") or ""
        jobs[entry_class] = job_id
    if any(jobs.values()):
        write_last_jobs(jobs)

    if args.output_json:
        print(
            json.dumps(
                {"status": "ok", "jar_id": jar_id, "jobs": jobs},
                ensure_ascii=True,
            )
        )
    else:
        for entry_class, job_id in jobs.items():
            if job_id:
                print(f"job started: {entry_class} -> {job_id}")
            else:
                print(f"job started: {entry_class}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=True))
        sys.exit(1)

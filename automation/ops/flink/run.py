#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.flink.common import (  # noqa: E402
    default_rest_url,
    http_request,
    resolve_jar_path,
    write_last_jobs,
)
from automation.ops.flink.list import list_jars  # noqa: E402
from automation.ops.flink.upload import upload_jar  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Flink JAR")
    parser.add_argument("--rest-url", default=default_rest_url(), help="Flink REST base URL")
    parser.add_argument("--jar-id", default=None, help="JAR id in Flink")
    parser.add_argument("--jar", default=None, help="path to JAR file (defaults to latest build)")
    parser.add_argument("--entry-class", required=True, help="entry class name")
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def run_jar(rest_url: str, jar_id: str, entry_class: str) -> dict:
    url = urllib.parse.urljoin(rest_url, f"/jars/{jar_id}/run")
    payload = json.dumps({"entryClass": entry_class}).encode("utf-8")
    status, resp = http_request(
        url, method="POST", data=payload, headers={"Content-Type": "application/json"}, timeout=30
    )
    if status != 200:
        raise RuntimeError(f"flink run jar failed: HTTP {status}")
    return json.loads(resp.decode("utf-8"))


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
    jar_id = args.jar_id
    if not jar_id:
        jar_path = resolve_jar_path(args.jar)
        upload_jar(args.rest_url, jar_path)
        jar_id = _find_jar_id(args.rest_url, jar_path.name)

    payload = run_jar(args.rest_url, jar_id, args.entry_class)
    job_id = payload.get("jobid") or payload.get("jobId")
    if job_id:
        write_last_jobs({args.entry_class: job_id})
    if args.output_json:
        print(json.dumps({"status": "ok", "response": payload}, ensure_ascii=True))
    else:
        if job_id:
            print(f"job started: {job_id}")
        else:
            print("job started")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=True))
        sys.exit(1)

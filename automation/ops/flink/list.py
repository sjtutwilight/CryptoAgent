#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.flink.common import default_rest_url, http_request  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List Flink JARs")
    parser.add_argument("--rest-url", default=default_rest_url(), help="Flink REST base URL")
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def list_jars(rest_url: str) -> dict:
    url = urllib.parse.urljoin(rest_url, "/jars")
    status, resp = http_request(url, timeout=30)
    if status != 200:
        raise RuntimeError(f"flink list jars failed: HTTP {status}")
    return json.loads(resp.decode("utf-8"))


def main() -> int:
    args = parse_args()
    payload = list_jars(args.rest_url)
    if args.output_json:
        print(json.dumps({"status": "ok", "response": payload}, ensure_ascii=True))
    else:
        jars = payload.get("files", []) if isinstance(payload, dict) else []
        names = [jar.get("id") or jar.get("name") for jar in jars if isinstance(jar, dict)]
        if names:
            print("jars:")
            for name in names:
                print(f"- {name}")
        else:
            print("no jars found")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=True))
        sys.exit(1)

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
    multipart_body,
    resolve_jar_path,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload JAR to Flink")
    parser.add_argument("--rest-url", default=default_rest_url(), help="Flink REST base URL")
    parser.add_argument("--jar", default=None, help="path to JAR file (defaults to latest build)")
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def upload_jar(rest_url: str, jar_path: Path) -> dict:
    body, content_type = multipart_body("jarfile", jar_path)
    url = urllib.parse.urljoin(rest_url, "/jars/upload")
    status, resp = http_request(url, method="POST", data=body, headers={"Content-Type": content_type}, timeout=300)
    if status != 200:
        raise RuntimeError(f"flink upload failed: HTTP {status}")
    return json.loads(resp.decode("utf-8"))


def main() -> int:
    args = parse_args()
    jar_path = resolve_jar_path(args.jar)
    payload = upload_jar(args.rest_url, jar_path)
    if args.output_json:
        print(json.dumps({"status": "ok", "response": payload}, ensure_ascii=True))
    else:
        jar_id = payload.get("filename") or payload.get("jarid") or payload.get("id")
        if jar_id:
            print(f"uploaded jar: {jar_id}")
        else:
            print("uploaded jar")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=True))
        sys.exit(1)

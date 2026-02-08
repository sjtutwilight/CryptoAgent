#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from typing import Dict


def _parse_headers(items: list[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for item in items:
        if ":" not in item:
            raise ValueError(f"invalid header: {item}")
        key, value = item.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HTTP GET request")
    parser.add_argument("url", help="request URL")
    parser.add_argument("--header", action="append", default=[], help="header in key:value format")
    parser.add_argument("--timeout", type=int, default=10, help="timeout in seconds")
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = _parse_headers(args.header)
    req = urllib.request.Request(args.url, method="GET", headers=headers)
    with urllib.request.urlopen(req, timeout=args.timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        if args.output_json:
            print(json.dumps({"status": "ok", "status_code": resp.status, "body": body}, ensure_ascii=True))
        else:
            print(f"status: {resp.status}")
            print(body)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=True))
        sys.exit(1)

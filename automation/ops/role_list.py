#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.common import (  # noqa: E402
    default_api,
    default_container,
    docker_get_json,
    http_get_json,
    join_url,
    print_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List DataInjector roles")
    parser.add_argument("--api", help="DataInjector API base URL")
    parser.add_argument("--container", default=default_container(), help="DataInjector container name")
    parser.add_argument("--token", default=None, help="X-Worker-Token value")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api = args.api or default_api()
    if api:
        url = join_url(api, "/api/roles")
        resp = http_get_json(url, token=args.token)
    else:
        url = "http://localhost:8090/api/roles"
        resp = docker_get_json(args.container, url, token=args.token)

    print_json({"status": "ok", "response": resp})
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print_json({"status": "error", "detail": str(exc)})
        sys.exit(1)

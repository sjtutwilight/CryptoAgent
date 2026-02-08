#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.common import (  # noqa: E402
    default_api,
    default_container,
    docker_post_json,
    http_post_json,
    join_url,
    print_json,
)


def stop_roles_programmatic(
    role_ids: List[str],
    api: str = None,
    container: str = None,
    token: str = None,
) -> dict:
    """编程方式停止roles（供其他Python代码调用）
    
    Args:
        role_ids: 要停止的 role_id 列表
        api: DataInjector API URL，如不提供则使用 docker 方式
        container: Docker 容器名，默认为 datainjector-worker
        token: X-Worker-Token 认证令牌
        
    Returns:
        API响应结果
        
    Raises:
        ValueError: role_ids 为空或参数错误
        RuntimeError: API调用失败
    """
    if not role_ids:
        raise ValueError("role_ids cannot be empty")
    
    payload = {"role_ids": role_ids}
    
    api = api or default_api()
    if api:
        url = join_url(api, "/api/roles/stop")
        return http_post_json(url, payload, token=token)
    else:
        container = container or default_container()
        url = "http://localhost:8090/api/roles/stop"
        return docker_post_json(container, url, payload, token=token)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stop DataInjector roles")
    parser.add_argument("role_ids", nargs="+", help="role ids or 'all'")
    parser.add_argument("--api", help="DataInjector API base URL")
    parser.add_argument("--container", default=default_container(), help="DataInjector container name")
    parser.add_argument("--token", default=None, help="X-Worker-Token value")
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def _parse_role_ids(raw_values: List[str]) -> List[str]:
    role_ids: List[str] = []
    for raw in raw_values:
        parts = [item.strip() for item in raw.split(",") if item.strip()]
        role_ids.extend(parts)
    return role_ids


def main() -> int:
    args = parse_args()
    role_ids = _parse_role_ids(args.role_ids)
    if not role_ids:
        raise ValueError("provide role_ids or 'all'")
    if any(role_id.lower() in ("all", "al") for role_id in role_ids):
        payload = {"role_ids": []}
    else:
        payload = {"role_ids": role_ids}

    api = args.api or default_api()
    if api:
        url = join_url(api, "/api/roles/stop")
        resp = http_post_json(url, payload, token=args.token)
    else:
        url = "http://localhost:8090/api/roles/stop"
        resp = docker_post_json(args.container, url, payload, token=args.token)

    if args.output_json:
        print_json({"status": "ok", "response": resp})
    else:
        role_ids = payload.get("role_ids", [])
        if role_ids:
            print(f"roles stopped: {', '.join(role_ids)}")
        else:
            print("roles stopped: all")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print_json({"status": "error", "detail": str(exc)})
        sys.exit(1)

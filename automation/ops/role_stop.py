#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.common import (  # noqa: E402
    default_api,
    default_container,
    docker_post_json,
    http_post_json,
    join_url,
    print_json,
    read_json_input,
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
    parser.add_argument("--role-ids", default="", help="comma-separated role ids")
    parser.add_argument("--config", help="path to JSON payload file")
    parser.add_argument("--json", help="JSON payload string")
    parser.add_argument("--api", help="DataInjector API base URL")
    parser.add_argument("--container", default=default_container(), help="DataInjector container name")
    parser.add_argument("--token", default=None, help="X-Worker-Token value")
    return parser.parse_args()


def _parse_role_ids(raw: str) -> List[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def main() -> int:
    args = parse_args()
    payload = None
    if args.config or args.json:
        payload = read_json_input(args.config, args.json)
    else:
        role_ids = _parse_role_ids(args.role_ids)
        if not role_ids:
            raise ValueError("provide --role-ids or --config/--json")
        payload = {"role_ids": role_ids}

    if "role_ids" not in payload:
        raise ValueError("payload must include 'role_ids'")

    api = args.api or default_api()
    if api:
        url = join_url(api, "/api/roles/stop")
        resp = http_post_json(url, payload, token=args.token)
    else:
        url = "http://localhost:8090/api/roles/stop"
        resp = docker_post_json(args.container, url, payload, token=args.token)

    print_json({"status": "ok", "response": resp})
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print_json({"status": "error", "detail": str(exc)})
        sys.exit(1)

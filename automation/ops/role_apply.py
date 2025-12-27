#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.common import (  # noqa: E402
    default_api,
    default_container,
    docker_post_json,
    http_post_json,
    join_url,
    print_json,
)


def load_roles_from_config(config_path: Path) -> List[Dict]:
    """从配置文件加载所有roles（工具函数）"""
    return _load_roles(config_path)


def get_roles_by_ids(config_path: Path, role_ids: List[str]) -> List[Dict]:
    """根据role_ids从配置文件获取role配置（工具函数）
    
    Args:
        config_path: config.yaml 文件路径
        role_ids: 需要获取的 role_id 列表
        
    Returns:
        匹配的 role 配置列表
        
    Raises:
        ValueError: 如果某些 role_id 未找到
    """
    roles = _load_roles(config_path)
    role_map = {}
    for role in roles:
        if isinstance(role, dict) and role.get("role_id"):
            role_map[role["role_id"]] = role
    
    selected = []
    missing = []
    for role_id in role_ids:
        role = role_map.get(role_id)
        if role:
            selected.append(role)
        else:
            missing.append(role_id)
    
    if missing:
        raise ValueError(f"role_id not found: {', '.join(missing)}")
    
    return selected


def apply_roles_programmatic(
    role_ids: List[str],
    config_path: Path = None,
    api: str = None,
    container: str = None,
    token: str = None,
) -> Dict:
    """编程方式应用roles（供其他Python代码调用）
    
    Args:
        role_ids: 要应用的 role_id 列表
        config_path: config.yaml 路径，默认为 datainjector/worker/configs/config.yaml
        api: DataInjector API URL，如不提供则使用 docker 方式
        container: Docker 容器名，默认为 datainjector-worker
        token: X-Worker-Token 认证令牌
        
    Returns:
        API响应结果
        
    Raises:
        ValueError: role_id 未找到或参数错误
        RuntimeError: API调用失败
    """
    if not role_ids:
        raise ValueError("role_ids cannot be empty")
    
    if config_path is None:
        config_path = REPO_ROOT / "datainjector/worker/configs/config.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")
    
    selected = get_roles_by_ids(config_path, role_ids)
    payload = {"roles": selected}
    
    api = api or default_api()
    if api:
        url = join_url(api, "/api/roles/apply")
        return http_post_json(url, payload, token=token)
    else:
        container = container or default_container()
        url = "http://localhost:8090/api/roles/apply"
        return docker_post_json(container, url, payload, token=token)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply roles from config.yaml by role_id")
    parser.add_argument(
        "--config-yaml",
        default="datainjector/worker/configs/config.yaml",
        help="path to config.yaml",
    )
    parser.add_argument("--role-id", default="", help="single role id")
    parser.add_argument("--role-ids", default="", help="comma-separated role ids")
    parser.add_argument("--api", help="DataInjector API base URL")
    parser.add_argument("--container", default=default_container(), help="DataInjector container name")
    parser.add_argument("--token", default=None, help="X-Worker-Token value")
    return parser.parse_args()


def _parse_role_ids(args: argparse.Namespace) -> List[str]:
    role_ids: List[str] = []
    if args.role_id:
        role_ids.append(args.role_id.strip())
    if args.role_ids:
        role_ids.extend([item.strip() for item in args.role_ids.split(",") if item.strip()])
    return [role_id for role_id in role_ids if role_id]


def _load_roles(config_path: Path) -> List[Dict]:
    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    roles = data.get("roles")
    if not isinstance(roles, list):
        raise ValueError("config.yaml must include 'roles' list")
    return roles


def main() -> int:
    args = parse_args()
    role_ids = _parse_role_ids(args)
    if not role_ids:
        raise ValueError("provide --role-id or --role-ids")

    config_path = Path(args.config_yaml)
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")

    roles = _load_roles(config_path)
    role_map = {}
    for role in roles:
        if isinstance(role, dict) and role.get("role_id"):
            role_map[role["role_id"]] = role

    selected = []
    missing = []
    for role_id in role_ids:
        role = role_map.get(role_id)
        if role:
            selected.append(role)
        else:
            missing.append(role_id)

    if missing:
        raise ValueError(f"role_id not found: {', '.join(missing)}")

    payload = {"roles": selected}
    api = args.api or default_api()
    if api:
        url = join_url(api, "/api/roles/apply")
        resp = http_post_json(url, payload, token=args.token)
    else:
        url = "http://localhost:8090/api/roles/apply"
        resp = docker_post_json(args.container, url, payload, token=args.token)

    print_json({"status": "ok", "role_ids": role_ids, "response": resp})
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print_json({"status": "error", "detail": str(exc)})
        sys.exit(1)

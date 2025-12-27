from __future__ import annotations

from pathlib import Path

from automation.test.shared.repo_utils import repo_root


def resolve_host_path(path: str) -> Path:
    if Path(path).is_absolute():
        return Path(path)
    return repo_root() / path


def resolve_container_path(path: str, container_root: str = "/app") -> str:
    if path.startswith("/"):
        return path
    return f"{container_root}/{path}"

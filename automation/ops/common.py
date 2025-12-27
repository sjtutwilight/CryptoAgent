from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional


def print_json(payload: Dict[str, Any]) -> None:
    json.dump(payload, sys.stdout, ensure_ascii=True)
    sys.stdout.write("\n")


def read_json_input(path: Optional[str], json_str: Optional[str]) -> Dict[str, Any]:
    if path:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    if json_str:
        return json.loads(json_str)
    data = sys.stdin.read()
    if not data.strip():
        raise ValueError("missing JSON payload")
    return json.loads(data)


def _build_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Worker-Token"] = token
    return headers


def http_get_json(url: str, token: Optional[str] = None, timeout: int = 5) -> Dict[str, Any]:
    req = urllib.request.Request(url, method="GET", headers=_build_headers(token))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        return json.loads(body)


def http_post_json(
    url: str,
    payload: Dict[str, Any],
    token: Optional[str] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST", headers=_build_headers(token))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        return json.loads(body)


def docker_exec(container: str, cmd: str, input_data: Optional[bytes] = None) -> tuple[int, str, str]:
    full_cmd = ["docker", "exec", "-i", container, "sh", "-c", cmd]
    proc = subprocess.run(full_cmd, input=input_data, capture_output=True)
    stdout = proc.stdout.decode("utf-8", errors="ignore")
    stderr = proc.stderr.decode("utf-8", errors="ignore")
    return proc.returncode, stdout, stderr


def ensure_curl(container: str) -> None:
    check_cmd = "command -v curl >/dev/null 2>&1"
    code, _, _ = docker_exec(container, check_cmd)
    if code == 0:
        return
    install_cmds = [
        "apt-get update -qq && apt-get install -y -qq curl",
        "apk add --no-cache curl",
    ]
    last_err = ""
    for install_cmd in install_cmds:
        code, _, stderr = docker_exec(container, install_cmd)
        if code == 0:
            return
        last_err = stderr
    raise RuntimeError(f"install curl failed: {last_err}")


def _parse_json_output(output: str) -> Dict[str, Any]:
    text = output.strip()
    if not text:
        raise ValueError("empty response")
    lines = text.split("\n")
    for line in lines:
        line = line.strip()
        if line.startswith("{") or line.startswith("["):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return json.loads(text)


def docker_post_json(container: str, url: str, payload: Dict[str, Any], token: Optional[str] = None) -> Dict[str, Any]:
    ensure_curl(container)
    body = json.dumps(payload).encode("utf-8")
    header = f"-H 'X-Worker-Token: {token}'" if token else ""
    cmd = f"curl -sS -X POST {url} -H 'Content-Type: application/json' {header} --data-binary @-"
    code, stdout, stderr = docker_exec(container, cmd, input_data=body)
    if code != 0:
        raise RuntimeError(f"docker exec curl failed: {stderr}")
    return _parse_json_output(stdout)


def docker_get_json(container: str, url: str, token: Optional[str] = None) -> Dict[str, Any]:
    ensure_curl(container)
    header = f"-H 'X-Worker-Token: {token}'" if token else ""
    cmd = f"curl -sS -X GET {url} -H 'Content-Type: application/json' {header}"
    code, stdout, stderr = docker_exec(container, cmd)
    if code != 0:
        raise RuntimeError(f"docker exec curl failed: {stderr}")
    return _parse_json_output(stdout)


def join_url(base: str, path: str) -> str:
    return urllib.parse.urljoin(base.rstrip("/") + "/", path.lstrip("/"))


def default_container() -> str:
    return os.getenv("DATAINJECTOR_CONTAINER", "datainjector-worker")


def default_api() -> Optional[str]:
    return os.getenv("DATAINJECTOR_API")

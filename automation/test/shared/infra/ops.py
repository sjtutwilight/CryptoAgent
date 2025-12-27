"""基础设施操作：HTTP、Docker、ClickHouse 等"""
from __future__ import annotations

import json
import subprocess
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple


# ==================== HTTP 操作 ====================

def http_request(url: str, method: str = "GET", data: Optional[bytes] = None, 
                 headers: Optional[Dict[str, str]] = None, timeout: int = 10) -> Tuple[int, bytes]:
    """通用 HTTP 请求"""
    req = urllib.request.Request(url, data=data, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        return resp.status, body


def http_get_json(url: str, timeout: int = 5) -> Tuple[int, Dict[str, Any]]:
    """HTTP GET 并解析 JSON"""
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        return resp.status, json.loads(body)


def http_post_json(url: str, payload: Dict[str, Any], timeout: int = 10) -> Tuple[int, Dict[str, Any]]:
    """HTTP POST JSON 数据"""
    body = json.dumps(payload).encode("utf-8")
    status, resp = http_request(url, method="POST", data=body, 
                                 headers={"Content-Type": "application/json"}, timeout=timeout)
    return status, json.loads(resp.decode("utf-8"))


# ==================== Docker 操作 ====================

def docker_exec(container: str, cmd: str, input_data: Optional[bytes] = None) -> Tuple[int, str, str]:
    """在容器中执行命令"""
    full_cmd = ["docker", "exec", "-i", container, "sh", "-c", cmd]
    proc = subprocess.run(full_cmd, input=input_data, capture_output=True)
    stdout = proc.stdout.decode("utf-8", errors="ignore")
    stderr = proc.stderr.decode("utf-8", errors="ignore")
    return proc.returncode, stdout, stderr


def docker_exec_curl_post(container: str, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """在容器中使用 curl 发送 POST 请求"""
    body = json.dumps(payload).encode("utf-8")
    cmd = f"curl -sS -X POST {url} -H 'Content-Type: application/json' --data-binary @-"
    code, stdout, stderr = docker_exec(container, cmd, input_data=body)
    
    if code != 0:
        raise RuntimeError(f"docker exec curl failed: {stderr}")
    if not stdout:
        raise RuntimeError("docker exec returned empty response")
    
    # 尝试解析 JSON
    output = stdout.strip()
    lines = output.split("\n")
    for line in lines:
        line = line.strip()
        if line and (line.startswith("{") or line.startswith("[")):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"failed to parse JSON response: {e}, output: {output}")


def ensure_container_curl(container: str) -> None:
    """确保容器中安装了 curl"""
    check_cmd = "command -v curl >/dev/null 2>&1"
    code, _, _ = docker_exec(container, check_cmd)
    if code == 0:
        return
    
    install_cmd = "apt-get update -qq && apt-get install -y -qq curl"
    code, _, stderr = docker_exec(container, install_cmd)
    if code != 0:
        raise RuntimeError(f"install curl failed: {stderr}")


# ==================== ClickHouse 操作 ====================

def clickhouse_query(http_url: str, query: str, user: str = "", password: str = "") -> str:
    """执行 ClickHouse 查询"""
    params = {}
    if user:
        params["user"] = user
    if password:
        params["password"] = password
    if params:
        http_url = f"{http_url}?{urllib.parse.urlencode(params)}"
    
    data = query.encode("utf-8")
    req = urllib.request.Request(http_url, data=data, method="POST")
    req.add_header("Content-Type", "text/plain")
    with urllib.request.urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        return body


def clickhouse_count(http_url: str, table: str, user: str = "", password: str = "", 
                     where_clause: Optional[str] = None) -> int:
    """查询 ClickHouse 表行数"""
    query = f"SELECT count() FROM {table}"
    if where_clause:
        query = f"{query} WHERE {where_clause}"
    resp = clickhouse_query(http_url, query, user, password)
    return int(resp.strip())


def clickhouse_truncate(http_url: str, table: str, user: str = "", password: str = "") -> None:
    """清空 ClickHouse 表"""
    clickhouse_query(http_url, f"TRUNCATE TABLE {table}", user, password)


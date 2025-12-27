from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional


def run_cmd(cmd: List[str], timeout: int = 10) -> tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=True, timeout=timeout)
    stdout = proc.stdout.decode("utf-8", errors="ignore").strip()
    stderr = proc.stderr.decode("utf-8", errors="ignore").strip()
    return proc.returncode, stdout, stderr


def docker_inspect(container: str) -> Dict[str, Any]:
    code, out, err = run_cmd(["docker", "inspect", container])
    if code != 0:
        return {"name": container, "status": "not_found", "error": err or out}
    try:
        payload = json.loads(out)[0]
    except Exception as exc:
        return {"name": container, "status": "unknown", "error": f"inspect parse error: {exc}"}
    state = payload.get("State", {})
    return {
        "name": container,
        "status": state.get("Status"),
        "health": (state.get("Health") or {}).get("Status"),
        "started_at": state.get("StartedAt"),
        "finished_at": state.get("FinishedAt"),
        "exit_code": state.get("ExitCode"),
        "error": state.get("Error") or None,
    }


def docker_stats(container: str) -> Optional[Dict[str, str]]:
    code, out, _ = run_cmd(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}",
            container,
        ],
        timeout=5,
    )
    if code != 0 or not out:
        return None
    parts = out.split("|")
    if len(parts) != 3:
        return None
    return {
        "cpu": parts[0].strip(),
        "memory": parts[1].strip(),
        "memory_percent": parts[2].strip(),
    }


def docker_recent_errors(container: str, tail: int = 50) -> List[str]:
    code, out, _ = run_cmd(["docker", "logs", "--tail", str(tail), container], timeout=5)
    if code != 0:
        return []
    lines = out.splitlines()
    pattern = re.compile(r"error|exception|fatal|failed", re.IGNORECASE)
    return [line for line in lines if pattern.search(line)][-10:]


def http_get_json(url: str, timeout: int = 5) -> tuple[int, Dict[str, Any]]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        return resp.status, json.loads(body)


def http_post_plain(url: str, payload: str, timeout: int = 5) -> tuple[int, str]:
    req = urllib.request.Request(url, data=payload.encode("utf-8"), method="POST")
    req.add_header("Content-Type", "text/plain")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        return resp.status, body


def check_socket(host: str, port: int, timeout: int = 2) -> tuple[bool, Optional[str]]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        return True, None
    except OSError as exc:
        return False, str(exc)
    finally:
        sock.close()


def flink_overview() -> Dict[str, Any]:
    rest_url = os.getenv("FLINK_REST_URL")
    if not rest_url:
        port = os.getenv("FLINK_JOBMANAGER_PORT", "8081")
        rest_url = f"http://localhost:{port}"
    overview_url = urllib.parse.urljoin(rest_url, "/overview")
    try:
        status, payload = http_get_json(overview_url, timeout=5)
        return {
            "rest_url": rest_url,
            "overview": payload,
            "rest_ok": status == 200,
        }
    except Exception as exc:
        return {
            "rest_url": rest_url,
            "overview": None,
            "rest_ok": False,
            "error": str(exc),
        }


def clickhouse_check() -> Dict[str, Any]:
    port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    url = f"http://localhost:{port}"
    user = os.getenv("CLICKHOUSE_USER", "")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    params: Dict[str, str] = {}
    if user:
        params["user"] = user
    if password:
        params["password"] = password
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    try:
        status, body = http_post_plain(url, "SELECT 1", timeout=5)
        return {"ok": status == 200 and body.strip() == "1", "status": status, "body": body.strip()}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def kafka_check(topic: Optional[str] = None) -> Dict[str, Any]:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS_LOCAL") or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        bootstrap = "localhost:9092"
    host, _, port_str = bootstrap.partition(":")
    port = int(port_str or "9092")
    ok, err = check_socket(host, port, timeout=2)
    result: Dict[str, Any] = {"bootstrap": bootstrap, "reachable": ok}
    if err:
        result["error"] = err
    if topic:
        result["topic"] = topic
    return result


def observability_stack() -> Dict[str, Any]:
    loki_url = os.getenv("LOKI_URL", "http://localhost:3100")
    grafana_url = os.getenv("GRAFANA_URL", "http://localhost:3000")
    loki_ready = None
    loki_error = None
    try:
        req = urllib.request.Request(urllib.parse.urljoin(loki_url, "/ready"), method="GET")
        with urllib.request.urlopen(req, timeout=3) as resp:
            loki_ready = resp.read().decode("utf-8", errors="ignore").strip()
    except Exception as exc:
        loki_error = str(exc)
    grafana_ok = None
    grafana_error = None
    try:
        status, payload = http_get_json(urllib.parse.urljoin(grafana_url, "/api/health"), timeout=3)
        grafana_ok = status == 200
        grafana_error = None if grafana_ok else payload
    except Exception as exc:
        grafana_ok = False
        grafana_error = str(exc)
    return {
        "loki_url": loki_url,
        "loki_ready": loki_ready,
        "loki_error": loki_error,
        "grafana_url": grafana_url,
        "grafana_ok": grafana_ok,
        "grafana_error": grafana_error,
    }


def snapshot_flink() -> Dict[str, Any]:
    jobmanager = os.getenv("FLINK_JOBMANAGER_CONTAINER", "flink-jobmanager")
    taskmanager = os.getenv("FLINK_TASKMANAGER_CONTAINER", "flink-taskmanager")
    containers = []
    for name in [jobmanager, taskmanager]:
        info = docker_inspect(name)
        info["stats"] = docker_stats(name)
        info["recent_errors"] = docker_recent_errors(name)
        containers.append(info)
    return {
        "containers": containers,
        "rest": flink_overview(),
    }


def snapshot_docker(containers: Optional[List[str]] = None) -> Dict[str, Any]:
    targets = containers or []
    if not targets:
        code, out, _ = run_cmd(["docker", "ps", "--format", "{{.Names}}"])
        targets = out.splitlines() if code == 0 else []
    details = []
    for name in targets:
        info = docker_inspect(name)
        info["stats"] = docker_stats(name)
        info["recent_errors"] = docker_recent_errors(name)
        details.append(info)
    return {"containers": details}


def snapshot_clickhouse() -> Dict[str, Any]:
    return {"clickhouse": clickhouse_check()}


def snapshot_kafka(topic: Optional[str] = None) -> Dict[str, Any]:
    return {"kafka": kafka_check(topic)}


def snapshot_stack() -> Dict[str, Any]:
    return {"stack": observability_stack()}


def render_text(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=True, indent=2))

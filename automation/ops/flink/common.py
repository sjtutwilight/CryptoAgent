from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Optional, Tuple


def http_request(
    url: str,
    method: str = "GET",
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
) -> Tuple[int, bytes]:
    req = urllib.request.Request(url, data=data, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read()


def default_rest_url() -> str:
    return os.getenv("FLINK_REST_URL") or os.getenv("FLINK_REST") or "http://localhost:8081"


JOB_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "kline": ("com.twilight.aggregator.KlineSignalJob",),
    "balance": ("com.twilight.aggregator.AccountBalanceJob",),
    "token": ("com.twilight.aggregator.TokenMetricAggregatorJob",),
    "pnl": ("com.twilight.aggregator.PnLAggregatorJob",),
    "perp": (
        "com.twilight.aggregator.PerpExecutionMetricsJob",
        "com.twilight.aggregator.PerpContextMetricsJob",
        "com.twilight.aggregator.PerpPanelAggregatorJob",
    ),
}


def resolve_job_keywords(keywords: list[str]) -> tuple[list[str], list[str]]:
    entry_classes: list[str] = []
    unknown: list[str] = []
    for raw in keywords:
        key = raw.strip().lower()
        if not key:
            continue
        classes = JOB_KEYWORDS.get(key)
        if not classes:
            unknown.append(raw)
            continue
        entry_classes.extend(classes)
    return list(dict.fromkeys(entry_classes)), unknown


JOB_NAME_PATTERNS: Dict[str, Tuple[str, ...]] = {
    "kline": (
        "Kline Signal & Indicator Pipeline",
        "Multi-Indicator Kline Signal Generation Job",
    ),
    "balance": ("Account Balance Processing Job",),
    "token": ("Streamlined DeFi Token Aggregator",),
    "pnl": ("DeFi Account PnL Aggregator",),
    "perp": (
        "Perpetual Contract Execution Metrics Job",
        "Perpetual Contract Context Metrics Job",
        "Perpetual Contract Panel Aggregator Job",
    ),
}


def resolve_job_name_patterns(keywords: list[str]) -> tuple[list[str], list[str]]:
    patterns: list[str] = []
    unknown: list[str] = []
    for raw in keywords:
        key = raw.strip().lower()
        if not key:
            continue
        values = JOB_NAME_PATTERNS.get(key)
        if not values:
            unknown.append(raw)
            continue
        patterns.extend(values)
    return list(dict.fromkeys(patterns)), unknown


def list_jobs_overview(rest_url: str) -> list[dict]:
    url = urllib.parse.urljoin(rest_url, "/jobs/overview")
    status, resp = http_request(url, timeout=10)
    if status != 200:
        raise RuntimeError(f"flink jobs overview failed: HTTP {status}")
    data = json.loads(resp.decode("utf-8"))
    jobs = data.get("jobs")
    return jobs if isinstance(jobs, list) else []


def resolve_jar_path(raw_path: Optional[str]) -> Path:
    repo_root = Path(__file__).resolve().parents[3]
    if raw_path:
        jar_path = Path(raw_path)
        if not jar_path.is_absolute():
            jar_path = repo_root / jar_path
        return jar_path

    target_dir = repo_root / "process/aggregator/target"
    if not target_dir.exists():
        raise FileNotFoundError(f"jar directory not found: {target_dir}")

    candidates = []
    for jar in target_dir.glob("*.jar"):
        name = jar.name
        if name.endswith("-sources.jar") or name.endswith("-javadoc.jar") or name.startswith("original-"):
            continue
        candidates.append(jar)

    if not candidates:
        raise FileNotFoundError(f"no jar found in {target_dir}")

    return max(candidates, key=lambda p: p.stat().st_mtime)


def _last_jobs_path() -> Path:
    return Path(__file__).resolve().parents[3] / "runtime/ops/flink/last_jobs.json"


def write_last_jobs(jobs: Dict[str, str]) -> None:
    path = _last_jobs_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=True)


def read_last_jobs() -> Dict[str, str]:
    path = _last_jobs_path()
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return {str(k): str(v) for k, v in data.items() if v}
    return {}


def multipart_body(file_field: str, file_path: Path) -> Tuple[bytes, str]:
    boundary = f"----ops-{int(time.time() * 1000)}"
    file_bytes = file_path.read_bytes()
    filename = file_path.name
    header = (
        f"--{boundary}\r\n"
        f"Content-Disposition: form-data; name=\"{file_field}\"; filename=\"{filename}\"\r\n"
        "Content-Type: application/java-archive\r\n\r\n"
    )
    body = header.encode("utf-8") + file_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")
    content_type = f"multipart/form-data; boundary={boundary}"
    return body, content_type

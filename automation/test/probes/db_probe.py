from __future__ import annotations

import urllib.parse
import urllib.request
from typing import Optional

from ..shared.core.context import RunContext
from ..shared.core.result import ProbeResult, ProbeStatus


def _clickhouse_query(http_url: str, query: str, user: str = "", password: str = "") -> str:
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


def _clickhouse_count(http_url: str, table: str, user: str, password: str, where_clause: Optional[str]) -> int:
    query = f"SELECT count() FROM {table}"
    if where_clause:
        query = f"{query} WHERE {where_clause}"
    resp = _clickhouse_query(http_url, query, user, password)
    return int(resp.strip())


def result_exists(
    ctx: RunContext,
    table: str,
    min_rows: int = 1,
    where_clause: Optional[str] = None,
) -> ProbeResult:
    clickhouse_port = "8123"
    clickhouse_user = ""
    clickhouse_password = ""
    clickhouse_http = None
    try:
        import os

        clickhouse_port = os.getenv("CLICKHOUSE_HTTP_PORT", clickhouse_port)
        clickhouse_user = os.getenv("CLICKHOUSE_USER", "")
        clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    except Exception:
        pass

    if ctx.metadata:
        clickhouse_http = ctx.metadata.get("clickhouse_http") or clickhouse_http
        clickhouse_user = ctx.metadata.get("clickhouse_user") or clickhouse_user
        clickhouse_password = ctx.metadata.get("clickhouse_password") or clickhouse_password

    http_url = clickhouse_http or f"http://localhost:{clickhouse_port}"
    try:
        count = _clickhouse_count(http_url, table, clickhouse_user, clickhouse_password, where_clause)
    except Exception as exc:
        return ProbeResult(status=ProbeStatus.FAIL, detail=f"clickhouse query failed: {exc}")

    if count >= min_rows:
        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail=f"{table} rows >= {min_rows}",
            metrics={"count": count},
        )
    return ProbeResult(
        status=ProbeStatus.FAIL,
        detail=f"{table} rows < {min_rows}",
        metrics={"count": count},
    )

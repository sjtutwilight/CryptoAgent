"""
DataInjector Worker 诊断探针

提供细粒度的可观测性，用于诊断 worker 各个环节的状态
"""
from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..shared.core.context import RunContext
from ..shared.core.result import ProbeResult, ProbeStatus


def _extract_json_payload(line: str) -> Optional[Dict[str, Any]]:
    start = line.find("{")
    if start < 0:
        return None
    end = line.rfind("}")
    if end <= start:
        return None
    payload = line[start : end + 1].strip()
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def _normalize_entry(raw: str, data: Dict[str, Any]) -> Dict[str, Any]:
    task_id = data.get("task_id") or data.get("taskId")
    return {
        "ts": data.get("ts"),
        "level": data.get("level"),
        "event": data.get("event"),
        "role_id": data.get("role_id"),
        "run_id": data.get("run_id"),
        "task_id": task_id,
        "trace_id": data.get("trace_id"),
        "span_id": data.get("span_id"),
        "fields": data,
        "raw": raw,
    }


def _fetch_worker_logs(
    container: str,
    since_seconds: int,
    tail_lines: int,
) -> Tuple[List[Dict[str, Any]], List[str], Optional[str]]:
    cmd = [
        "docker",
        "logs",
        "--since",
        f"{since_seconds}s",
        "--tail",
        str(tail_lines),
        container,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    if proc.returncode != 0:
        return [], [], proc.stderr.strip() or "failed to get logs"

    logs = proc.stdout + proc.stderr
    lines = [line for line in logs.splitlines() if line.strip()]
    entries: List[Dict[str, Any]] = []
    for line in lines:
        data = _extract_json_payload(line)
        if not data:
            continue
        entries.append(_normalize_entry(line, data))

    return entries, lines, None


def _filter_entries(
    entries: Iterable[Dict[str, Any]],
    run_id: Optional[str] = None,
    role_id: Optional[str] = None,
    task_id: Optional[str] = None,
    events: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    event_set = set(events) if events else None
    filtered: List[Dict[str, Any]] = []
    for entry in entries:
        if run_id and entry.get("run_id") != run_id:
            continue
        if role_id and entry.get("role_id") != role_id:
            continue
        if task_id and entry.get("task_id") != task_id:
            continue
        if event_set and entry.get("event") not in event_set:
            continue
        filtered.append(entry)
    return filtered


def _summarize_events(entries: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        event = entry.get("event") or "unknown"
        ts = entry.get("ts")
        bucket = summary.setdefault(
            event,
            {
                "count": 0,
                "first_ts": ts,
                "last_ts": ts,
            },
        )
        bucket["count"] += 1
        if ts:
            if not bucket.get("first_ts") or ts < bucket["first_ts"]:
                bucket["first_ts"] = ts
            if not bucket.get("last_ts") or ts > bucket["last_ts"]:
                bucket["last_ts"] = ts
    return summary


def _compact_entries(entries: Iterable[Dict[str, Any]], limit: int = 15) -> List[Dict[str, Any]]:
    compacted: List[Dict[str, Any]] = []
    for entry in list(entries)[-limit:]:
        compacted.append(
            {
                "ts": entry.get("ts"),
                "level": entry.get("level"),
                "event": entry.get("event"),
                "role_id": entry.get("role_id"),
                "run_id": entry.get("run_id"),
                "task_id": entry.get("task_id"),
                "trace_id": entry.get("trace_id"),
            }
        )
    return compacted


def check_worker_health(ctx: RunContext, container: str = "datainjector-worker") -> ProbeResult:
    """检查 Worker 容器健康状态"""
    try:
        cmd = ["docker", "inspect", "--format", "{{.State.Status}}", container]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if proc.returncode != 0:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail=f"container not found: {container}",
            )
        
        status = proc.stdout.strip()
        if status != "running":
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail=f"container status: {status}",
            )
        
        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail="container running",
            payload={"container": container, "status": status}
        )
    except Exception as exc:
        return ProbeResult(status=ProbeStatus.FAIL, detail=f"health check failed: {exc}")


def check_kafka_consumer_lag(
    ctx: RunContext,
    topic: str,
    group_id: str,
    container: str = "datainjector-worker",
    kafka_broker: str = "kafka:29092"
) -> ProbeResult:
    """检查 Kafka 消费者 lag"""
    try:
        # 使用 kafka-consumer-groups 检查 lag
        cmd = [
            "docker", "exec", container, "sh", "-c",
            f"kafka-consumer-groups.sh --bootstrap-server {kafka_broker} "
            f"--group {group_id} --describe 2>/dev/null || echo 'NOT_FOUND'"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        output = proc.stdout.strip()
        
        if "NOT_FOUND" in output or not output:
            return ProbeResult(
                status=ProbeStatus.SKIP,
                detail=f"consumer group not found: {group_id}",
            )
        
        # 解析 lag 信息
        lines = output.split("\n")
        lag_info = []
        for line in lines:
            if topic in line:
                parts = line.split()
                if len(parts) >= 6:
                    lag_info.append({
                        "topic": parts[0],
                        "partition": parts[1],
                        "current_offset": parts[2],
                        "log_end_offset": parts[3],
                        "lag": parts[4]
                    })
        
        if not lag_info:
            return ProbeResult(
                status=ProbeStatus.SKIP,
                detail=f"no lag info for topic: {topic}",
            )
        
        total_lag = sum(int(info["lag"]) for info in lag_info if info["lag"].isdigit())
        
        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail=f"consumer lag: {total_lag}",
            metrics={"total_lag": total_lag, "partitions": len(lag_info)},
            payload={"lag_info": lag_info}
        )
    except Exception as exc:
        return ProbeResult(status=ProbeStatus.FAIL, detail=f"lag check failed: {exc}")


def check_worker_logs(
    ctx: RunContext,
    container: str = "datainjector-worker",
    since_seconds: int = 60,
    keywords: Optional[List[str]] = None,
    tail_lines: int = 2000,
) -> ProbeResult:
    """检查 Worker 日志中的错误和关键信息"""
    try:
        entries, lines, error = _fetch_worker_logs(container, since_seconds, tail_lines)
        if error:
            return ProbeResult(status=ProbeStatus.FAIL, detail=error)

        errors = [
            entry
            for entry in entries
            if (entry.get("level") == "ERROR")
            or (entry.get("event") or "").endswith(".error")
        ]
        warnings = [entry for entry in entries if entry.get("level") == "WARN"]

        keyword_hits: Dict[str, int] = {}
        if keywords:
            for keyword in keywords:
                hits = [line for line in lines if keyword in line]
                if hits:
                    keyword_hits[keyword] = len(hits)

        status = ProbeStatus.SUCCESS if not errors else ProbeStatus.FAIL

        return ProbeResult(
            status=status,
            detail=f"errors: {len(errors)}, warnings: {len(warnings)}",
            metrics={
                "error_count": len(errors),
                "warning_count": len(warnings),
                "keyword_hits": keyword_hits,
            },
            payload={
                "recent_errors": _compact_entries(errors, limit=10) if errors else [],
                "recent_warnings": _compact_entries(warnings, limit=5) if warnings else [],
            },
        )
    except Exception as exc:
        return ProbeResult(status=ProbeStatus.FAIL, detail=f"log check failed: {exc}")


def check_worker_events(
    ctx: RunContext,
    required_events: List[str],
    optional_events: Optional[List[str]] = None,
    run_id: Optional[str] = None,
    role_id: Optional[str] = None,
    task_id: Optional[str] = None,
    container: str = "datainjector-worker",
    since_seconds: int = 60,
    tail_lines: int = 2000,
) -> ProbeResult:
    """检查 worker 结构化事件是否满足预期"""
    entries, _, error = _fetch_worker_logs(container, since_seconds, tail_lines)
    if error:
        return ProbeResult(status=ProbeStatus.FAIL, detail=error)

    filtered = _filter_entries(entries, run_id=run_id, role_id=role_id, task_id=task_id)
    summary = _summarize_events(filtered)

    missing = [evt for evt in required_events if summary.get(evt, {}).get("count", 0) == 0]
    status = ProbeStatus.FAIL if missing else ProbeStatus.SUCCESS

    detail = "all required events observed"
    if missing:
        detail = f"missing events: {', '.join(missing)}"

    optional_hits = {}
    if optional_events:
        for evt in optional_events:
            optional_hits[evt] = summary.get(evt, {}).get("count", 0)

    return ProbeResult(
        status=status,
        detail=detail,
        metrics={
            "total_events": len(filtered),
            "missing_count": len(missing),
            "optional_hits": optional_hits,
        },
        payload={
            "missing_events": missing,
            "event_summary": summary,
            "filters": {"run_id": run_id, "role_id": role_id, "task_id": task_id},
            "recent_events": _compact_entries(filtered, limit=12),
        },
    )


def check_worker_errors(
    ctx: RunContext,
    run_id: Optional[str] = None,
    role_id: Optional[str] = None,
    task_id: Optional[str] = None,
    container: str = "datainjector-worker",
    since_seconds: int = 60,
    tail_lines: int = 2000,
    error_events: Optional[List[str]] = None,
) -> ProbeResult:
    """检查 worker 结构化错误事件"""
    entries, _, error = _fetch_worker_logs(container, since_seconds, tail_lines)
    if error:
        return ProbeResult(status=ProbeStatus.FAIL, detail=error)

    filtered = _filter_entries(entries, run_id=run_id, role_id=role_id, task_id=task_id)
    if not filtered:
        return ProbeResult(status=ProbeStatus.SKIP, detail="no structured logs for filters")

    error_event_set = set(error_events or [])
    error_entries = []
    for entry in filtered:
        event = entry.get("event") or ""
        if entry.get("level") == "ERROR":
            error_entries.append(entry)
            continue
        if event.endswith(".error") or event in error_event_set:
            error_entries.append(entry)

    status = ProbeStatus.FAIL if error_entries else ProbeStatus.SUCCESS
    detail = f"errors: {len(error_entries)}"

    return ProbeResult(
        status=status,
        detail=detail,
        metrics={"error_count": len(error_entries)},
        payload={"recent_errors": _compact_entries(error_entries, limit=10)},
    )


def check_role_status(
    ctx: RunContext,
    role_id: str,
    container: str = "datainjector-worker"
) -> ProbeResult:
    """检查 role 是否正在运行"""
    try:
        # 通过 API 查询 role 状态
        cmd = [
            "docker", "exec", container, "sh", "-c",
            "curl -sS http://localhost:8090/api/roles 2>/dev/null"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        
        if proc.returncode != 0:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail="failed to query roles API",
            )
        
        try:
            raw = proc.stdout.strip()
            data = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail="invalid API response",
                payload={"raw": proc.stdout[:1000]},
            )

        roles_list = []
        if isinstance(data, list):
            roles_list = data
        elif isinstance(data, dict):
            if isinstance(data.get("roles"), list):
                roles_list = data["roles"]
            elif isinstance(data.get("data"), list):
                roles_list = data["data"]
        else:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail="unexpected API response type",
                payload={"raw": proc.stdout[:1000]},
            )

        target_role = None
        for role in roles_list:
            if isinstance(role, str) and role == role_id:
                target_role = {"role_id": role, "status": "running"}
                break
            if isinstance(role, dict):
                if role.get("role_id") == role_id or role.get("roleId") == role_id or role.get("id") == role_id:
                    target_role = role
                    break

        if not target_role:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail=f"role not found: {role_id}",
                payload={"role_count": len(roles_list)},
            )

        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail=f"role status: {target_role.get('status', 'unknown')}",
            payload=target_role
        )
    except Exception as exc:
        return ProbeResult(status=ProbeStatus.FAIL, detail=f"role status check failed: {exc}")


def check_file_permissions(
    ctx: RunContext,
    output_dir: str,
    container: str = "datainjector-worker"
) -> ProbeResult:
    """检查文件写入权限"""
    try:
        # 检查目录是否存在
        cmd = ["docker", "exec", container, "sh", "-c", f"ls -ld {output_dir} 2>/dev/null"]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        
        if proc.returncode != 0:
            # 目录不存在，尝试创建
            create_cmd = ["docker", "exec", container, "sh", "-c", f"mkdir -p {output_dir}"]
            create_proc = subprocess.run(create_cmd, capture_output=True, text=True, timeout=5)
            if create_proc.returncode != 0:
                return ProbeResult(
                    status=ProbeStatus.FAIL,
                    detail=f"cannot create directory: {output_dir}",
                )
        
        # 检查写入权限
        test_file = f"{output_dir}/.test_write_{int(time.time())}"
        write_cmd = ["docker", "exec", container, "sh", "-c", f"touch {test_file} && rm {test_file}"]
        write_proc = subprocess.run(write_cmd, capture_output=True, text=True, timeout=5)
        
        if write_proc.returncode != 0:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail=f"no write permission: {output_dir}",
                payload={"error": write_proc.stderr}
            )
        
        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail=f"directory writable: {output_dir}",
        )
    except Exception as exc:
        return ProbeResult(status=ProbeStatus.FAIL, detail=f"permission check failed: {exc}")


def check_api_connectivity(
    ctx: RunContext,
    endpoint: str,
    container: str = "datainjector-worker"
) -> ProbeResult:
    """检查外部 API 连通性"""
    try:
        cmd = [
            "docker", "exec", container, "sh", "-c",
            f"curl -sS -o /dev/null -w '%{{http_code}}' --max-time 10 {endpoint} 2>/dev/null"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        
        if proc.returncode != 0:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail=f"cannot reach {endpoint}",
            )
        
        http_code = proc.stdout.strip()
        
        if http_code.startswith("2") or http_code.startswith("3"):
            status = ProbeStatus.SUCCESS
            detail = f"API reachable: {http_code}"
        else:
            status = ProbeStatus.FAIL
            detail = f"API returned: {http_code}"
        
        return ProbeResult(
            status=status,
            detail=detail,
            metrics={"http_code": http_code}
        )
    except Exception as exc:
        return ProbeResult(status=ProbeStatus.FAIL, detail=f"connectivity check failed: {exc}")


DEFAULT_FAULT_RULES: Dict[str, Any] = {
    "required_events": [
        "ws.reconnect.start",
        "ws.reconnect.success",
        "caller.response",
        "pipeline.finish",
    ],
    "required_backfill_events": [
        "integrity.backfill.trigger",
        "integrity.backfill.success",
    ],
    "fail_events": [
        "handler.error",
        "sink.error",
        "pipeline.error",
        "caller.error",
        "integrity.backfill.exhausted",
        "integrity.backfill.enqueue.error",
    ],
    "text_fallback_patterns": {
        "integrity.backfill.trigger": r"role\s+{role}\s+worker-\d+:.*backfill.*",
        "integrity.backfill.success": r"role\s+{role}\s+worker-\d+:.*backfill.*succeeded",
        "integrity.backfill.exhausted": r"role\s+{role}\s+worker-\d+:.*backfill.*exhausted",
    },
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _run_dir_from_ctx(ctx: RunContext) -> Path:
    base_dir = str((ctx.metadata or {}).get("run_base_dir") or "automation/test/runs")
    root = _project_root()
    base_path = Path(base_dir)
    if not base_path.is_absolute():
        base_path = root / base_path
    return base_path / ctx.run_id


def _deep_merge_dict(base: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not override:
        return dict(base)
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def _collect_structured_hits(entries: List[Dict[str, Any]], role_id: str, event: str) -> List[Dict[str, Any]]:
    return [
        item for item in entries
        if item.get("role_id") == role_id and item.get("event") == event
    ]


def _collect_text_hits(lines: List[str], role_id: str, pattern_template: str) -> List[Dict[str, Any]]:
    pattern = pattern_template.format(role=re.escape(role_id))
    regex = re.compile(pattern)
    hits: List[Dict[str, Any]] = []
    for raw in lines:
        if regex.search(raw):
            hits.append({
                "ts": None,
                "level": "INFO",
                "event": "text.fallback",
                "role_id": role_id,
                "raw": raw,
            })
    return hits


def _build_evidence(items: List[Dict[str, Any]], source: str, event: str, limit: int = 20) -> List[Dict[str, Any]]:
    evidences: List[Dict[str, Any]] = []
    for entry in items[-limit:]:
        evidences.append({
            "source": source,
            "event": event,
            "role_id": entry.get("role_id"),
            "ts": entry.get("ts"),
            "level": entry.get("level"),
            "raw": entry.get("raw"),
        })
    return evidences


def evaluate_fault_regression(
    entries: List[Dict[str, Any]],
    lines: List[str],
    role_ids: List[str],
    rules: Optional[Dict[str, Any]] = None,
    expect_backfill: bool = False,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    effective_rules = _deep_merge_dict(DEFAULT_FAULT_RULES, rules)
    required_events = list(effective_rules.get("required_events") or [])
    if expect_backfill:
        required_events.extend(list(effective_rules.get("required_backfill_events") or []))
    # 保持顺序并去重
    required_events = list(dict.fromkeys(required_events))
    fail_events = list(effective_rules.get("fail_events") or [])
    text_fallback_patterns = dict(effective_rules.get("text_fallback_patterns") or {})

    role_results: Dict[str, Any] = {}
    evidence_rows: List[Dict[str, Any]] = []
    failed_roles: List[str] = []

    for role_id in role_ids:
        missing_events: List[str] = []
        fallback_hits: Dict[str, int] = {}
        event_counts: Dict[str, int] = {}
        failed_event_hits: List[Dict[str, Any]] = []

        for event_name in required_events:
            hits = _collect_structured_hits(entries, role_id, event_name)
            if hits:
                event_counts[event_name] = len(hits)
                evidence_rows.extend(_build_evidence(hits, source="structured", event=event_name))
                continue

            pattern = text_fallback_patterns.get(event_name)
            if pattern:
                text_hits = _collect_text_hits(lines, role_id, pattern)
                if text_hits:
                    fallback_hits[event_name] = len(text_hits)
                    event_counts[event_name] = len(text_hits)
                    evidence_rows.extend(_build_evidence(text_hits, source="text_fallback", event=event_name))
                    continue

            missing_events.append(event_name)
            event_counts[event_name] = 0

        for event_name in fail_events:
            fail_hits = _collect_structured_hits(entries, role_id, event_name)
            if not fail_hits:
                continue
            failed_event_hits.append({"event": event_name, "count": len(fail_hits)})
            evidence_rows.extend(_build_evidence(fail_hits, source="structured", event=event_name))

        status = "PASS"
        if missing_events or failed_event_hits:
            status = "FAIL"
            failed_roles.append(role_id)

        role_results[role_id] = {
            "status": status,
            "missing_events": missing_events,
            "failed_events": failed_event_hits,
            "event_counts": event_counts,
            "fallback_hits": fallback_hits,
        }

    summary = {
        "overall_status": "FAIL" if failed_roles else "PASS",
        "failed_roles": failed_roles,
        "role_results": role_results,
        "effective_rules": {
            "required_events": required_events,
            "fail_events": fail_events,
            "expect_backfill": expect_backfill,
        },
    }
    return summary, evidence_rows


def check_fault_regression(
    ctx: RunContext,
    role_ids: List[str],
    container: str = "datainjector-worker",
    since_seconds: int = 120,
    tail_lines: int = 5000,
    rules: Optional[Dict[str, Any]] = None,
    expect_backfill: bool = False,
) -> ProbeResult:
    if not role_ids:
        return ProbeResult(status=ProbeStatus.FAIL, detail="role_ids is empty")

    entries, lines, error = _fetch_worker_logs(container, since_seconds, tail_lines)
    if error:
        return ProbeResult(status=ProbeStatus.FAIL, detail=error)

    summary, evidence = evaluate_fault_regression(
        entries=entries,
        lines=lines,
        role_ids=role_ids,
        rules=rules,
        expect_backfill=expect_backfill,
    )
    failed_roles = summary.get("failed_roles") or []
    status = ProbeStatus.FAIL if failed_roles else ProbeStatus.SUCCESS
    detail = "all roles passed"
    if failed_roles:
        detail = f"failed roles: {', '.join(failed_roles)}"

    return ProbeResult(
        status=status,
        detail=detail,
        metrics={
            "role_count": len(role_ids),
            "failed_role_count": len(failed_roles),
            "event_count": len(entries),
        },
        payload={
            "summary": summary,
            "evidence": evidence,
        },
    )


def write_fault_regression_artifacts(
    ctx: RunContext,
    summary: Dict[str, Any],
    evidence: List[Dict[str, Any]],
) -> ProbeResult:
    run_dir = _run_dir_from_ctx(ctx)
    run_dir.mkdir(parents=True, exist_ok=True)

    summary_json_path = run_dir / "fault_regression_summary.json"
    summary_txt_path = run_dir / "summary.txt"
    evidence_path = run_dir / "evidence.jsonl"

    with summary_json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=True, indent=2)
        f.write("\n")

    with evidence_path.open("w", encoding="utf-8") as f:
        for row in evidence:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

    lines = [
        f"run_id: {ctx.run_id}",
        f"scenario: {ctx.scenario}",
        f"overall_status: {summary.get('overall_status', 'UNKNOWN')}",
        f"failed_roles: {', '.join(summary.get('failed_roles') or []) or 'none'}",
        "",
        "role_results:",
    ]
    role_results = summary.get("role_results") or {}
    for role_id in sorted(role_results.keys()):
        item = role_results[role_id] or {}
        missing = ", ".join(item.get("missing_events") or []) or "none"
        failed = ", ".join([f"{x.get('event')}({x.get('count')})" for x in (item.get("failed_events") or [])]) or "none"
        lines.append(f"- {role_id}: {item.get('status', 'UNKNOWN')}")
        lines.append(f"  missing_events: {missing}")
        lines.append(f"  failed_events: {failed}")

    with summary_txt_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    return ProbeResult(
        status=ProbeStatus.SUCCESS,
        detail="fault regression artifacts written",
        payload={
            "run_dir": str(run_dir),
            "summary_json": str(summary_json_path),
            "summary_txt": str(summary_txt_path),
            "evidence_jsonl": str(evidence_path),
        },
    )

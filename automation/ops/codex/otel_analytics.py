#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


# 仓库根目录用于统一定位 runtime/data 落盘路径
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = REPO_ROOT / "runtime" / "data" / "codex_otel"


@dataclass
class Suggestion:
    """优化建议实体，承载闭环所需的核心字段。"""

    id: str
    type: str
    title: str
    action: str
    expected_benefit: str
    evidence: Dict[str, Any]
    status: str
    created_at: str
    updated_at: str


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time_seconds(value: Any) -> float:
    """将多种时间格式统一为秒级时间戳，无法解析时返回当前时间。"""
    if value is None:
        return time.time()
    if isinstance(value, (int, float)):
        # OTel 常见 nano 时间戳
        if value > 1e16:
            return float(value) / 1e9
        # 毫秒时间戳
        if value > 1e12:
            return float(value) / 1e3
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return time.time()
        if raw.isdigit():
            return parse_time_seconds(int(raw))
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return time.time()
    return time.time()


def safe_json_loads(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def load_jsonl(path: Path) -> List[Any]:
    if not path.exists():
        raise FileNotFoundError(f"输入文件不存在: {path}")
    rows: List[Any] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(safe_json_loads(line))
    return rows


def flatten_otlp_payload(row: Any) -> Iterable[Dict[str, Any]]:
    """兼容 OTel 原生 payload 与扁平 JSONL 两种输入形态。"""
    if isinstance(row, dict) and "resourceLogs" in row:
        for resource_log in row.get("resourceLogs", []):
            resource_attrs = normalize_attr_kv(resource_log.get("resource", {}).get("attributes", []))
            for scope_log in resource_log.get("scopeLogs", []):
                scope_attrs = normalize_attr_kv(scope_log.get("scope", {}).get("attributes", []))
                for record in scope_log.get("logRecords", []):
                    event_attrs = normalize_attr_kv(record.get("attributes", []))
                    body = record.get("body")
                    body_value = parse_otlp_any_value(body)
                    if isinstance(body_value, str):
                        body_value = safe_json_loads(body_value)
                    payload = {
                        "timestamp": record.get("timeUnixNano"),
                        "severity_text": record.get("severityText"),
                        "attributes": {**resource_attrs, **scope_attrs, **event_attrs},
                        "body": body_value,
                    }
                    yield payload
        return

    if isinstance(row, dict):
        yield row


def parse_otlp_any_value(value: Any) -> Any:
    if isinstance(value, dict):
        for key in [
            "stringValue",
            "intValue",
            "doubleValue",
            "boolValue",
            "bytesValue",
            "arrayValue",
            "kvlistValue",
        ]:
            if key in value:
                return value[key]
    return value


def normalize_attr_kv(items: Any) -> Dict[str, Any]:
    """将 OTel attribute 列表标准化为 dict。"""
    result: Dict[str, Any] = {}
    if isinstance(items, dict):
        return items
    if not isinstance(items, list):
        return result
    for item in items:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not key:
            continue
        result[str(key)] = parse_otlp_any_value(item.get("value"))
    return result


def normalize_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    """统一事件字段，保证后续指标计算口径一致。"""
    attrs = raw.get("attributes", {}) if isinstance(raw.get("attributes"), dict) else normalize_attr_kv(raw.get("attributes", []))
    body = raw.get("body", {})
    if isinstance(body, str):
        body = safe_json_loads(body)
    if not isinstance(body, dict):
        body = {"message": str(body)}

    event_name = (
        body.get("event")
        or body.get("event_name")
        or attrs.get("event.name")
        or attrs.get("event")
        or raw.get("event")
        or "unknown"
    )
    session_id = (
        body.get("session_id")
        or attrs.get("session_id")
        or attrs.get("codex.session_id")
        or raw.get("session_id")
        or "unknown-session"
    )
    tool_name = (
        body.get("tool_name")
        or body.get("tool")
        or attrs.get("tool.name")
        or attrs.get("tool")
        or "unknown-tool"
    )
    command = (
        body.get("command")
        or attrs.get("command")
        or body.get("action")
        or attrs.get("action")
        or ""
    )
    outcome = (
        body.get("outcome")
        or attrs.get("outcome")
        or raw.get("status")
        or ""
    )

    timestamp = parse_time_seconds(raw.get("timestamp") or raw.get("ts") or raw.get("timeUnixNano") or raw.get("time_unix_nano"))
    if "timestamp" not in raw and "timeUnixNano" in raw:
        timestamp = parse_time_seconds(raw.get("timeUnixNano"))

    category = classify_event(str(event_name), str(command))

    return {
        "event_name": str(event_name),
        "session_id": str(session_id),
        "tool_name": str(tool_name),
        "command": str(command),
        "outcome": str(outcome),
        "category": category,
        "timestamp": timestamp,
        "raw": raw,
    }


def classify_event(event_name: str, command: str) -> str:
    name = event_name.lower()
    cmd = command.lower()

    if "tool_error" in name or "error" == name:
        return "tool_error"
    if "tool_result" in name or ("tool" in name and "result" in name):
        return "tool_result"
    if "tool_decision" in name:
        return "tool_decision"
    if "session_start" in name:
        return "session_start"
    if "session_end" in name:
        return "session_end"
    if "patch_apply" in name or "apply_patch" in name or "write" in name or "edit" in name:
        return "edit"
    if "test" in name or "verify" in name or "pytest" in cmd or "go test" in cmd:
        return "test"
    if any(token in name for token in ["read", "open", "search", "find", "list"]):
        return "read"
    if any(token in cmd for token in ["rg ", "cat ", "sed ", "ls "]):
        return "read"
    return "other"


def average(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def compute_snapshot(events: List[Dict[str, Any]], window_hours: int) -> Dict[str, Any]:
    """计算决策链核心指标，并输出可追溯证据。"""
    now_ts = time.time()
    window_start_ts = now_ts - window_hours * 3600
    scoped = [e for e in events if e["timestamp"] >= window_start_ts]
    scoped.sort(key=lambda x: x["timestamp"])

    read_count = 0
    edit_count = 0
    test_count = 0
    tool_error_count = 0
    tool_result_success = 0
    tool_result_total = 0

    session_start_ts: Dict[str, float] = {}
    session_last_ts: Dict[str, float] = {}
    session_first_output_ts: Dict[str, float] = {}
    per_session_steps: Dict[str, Counter[str]] = defaultdict(Counter)
    tool_error_hotspots: Counter[str] = Counter()

    for event in scoped:
        sid = event["session_id"]
        cat = event["category"]
        session_last_ts[sid] = event["timestamp"]

        # 统计会话起点
        if cat == "session_start" and sid not in session_start_ts:
            session_start_ts[sid] = event["timestamp"]

        # 统计阶段行为计数
        if cat == "read":
            read_count += 1
        elif cat == "edit":
            edit_count += 1
        elif cat == "test":
            test_count += 1

        # 统计工具执行质量
        if cat == "tool_error":
            tool_error_count += 1
            tool_error_hotspots[event["tool_name"]] += 1
        if cat == "tool_result":
            tool_result_total += 1
            if event["outcome"].lower() in {"ok", "success", "succeeded"}:
                tool_result_success += 1

        # 记录可视为“有效产出”的最早时间
        if sid in session_start_ts and sid not in session_first_output_ts and cat in {"edit", "tool_result"}:
            session_first_output_ts[sid] = event["timestamp"]

        # 用“事件名+工具名+命令”近似描述步骤
        step_key = f"{event['event_name']}|{event['tool_name']}|{event['command']}"
        per_session_steps[sid][step_key] += 1

    repeat_step_count = 0
    repeated_examples: List[Dict[str, Any]] = []
    for sid, counter in per_session_steps.items():
        for step, count in counter.items():
            if count > 1:
                repeat_step_count += count - 1
                if len(repeated_examples) < 5:
                    repeated_examples.append({"session_id": sid, "step": step, "count": count})

    first_latency_values: List[float] = []
    no_output_values: List[float] = []
    for sid, start_ts in session_start_ts.items():
        if sid in session_first_output_ts:
            first_latency_values.append(max(0.0, session_first_output_ts[sid] - start_ts))
        else:
            last_seen = session_last_ts.get(sid, now_ts)
            no_output_values.append(max(0.0, last_seen - start_ts))

    denominator = max(1, edit_count + test_count)
    low_value_read_ratio = read_count / denominator

    tool_success_denominator = max(1, tool_result_total + tool_error_count)
    tool_success_rate = tool_result_success / tool_success_denominator

    snapshot = {
        "generated_at": now_iso(),
        "window_hours": window_hours,
        "event_total": len(scoped),
        "session_total": len(session_start_ts),
        "metrics": {
            "codex_total_sessions": float(len(session_start_ts)),
            "codex_repeat_step_count": float(repeat_step_count),
            "codex_tool_error_total": float(tool_error_count),
            "codex_tool_success_rate": float(tool_success_rate),
            "codex_low_value_read_ratio": float(low_value_read_ratio),
            "codex_first_effective_output_latency_seconds": float(average(first_latency_values)),
            "codex_session_no_output_duration_seconds": float(max(no_output_values) if no_output_values else 0.0),
        },
        "evidence": {
            "read_count": read_count,
            "edit_count": edit_count,
            "test_count": test_count,
            "tool_result_total": tool_result_total,
            "tool_result_success": tool_result_success,
            "repeated_examples": repeated_examples,
            "tool_error_hotspots": [
                {"tool": tool, "count": count} for tool, count in tool_error_hotspots.most_common(5)
            ],
        },
    }
    return snapshot


def build_suggestions(snapshot: Dict[str, Any]) -> List[Suggestion]:
    """依据阈值生成候选建议，支持脚本化/文档化/流程优化三类。"""
    metrics = snapshot["metrics"]
    evidence = snapshot["evidence"]
    created_at = snapshot["generated_at"]
    suggestions: List[Suggestion] = []

    if metrics.get("codex_repeat_step_count", 0.0) >= 5:
        suggestions.append(
            Suggestion(
                id=f"SUGG-{int(time.time())}-SCRIPT",
                type="script",
                title="高频重复步骤可脚本化",
                action="将重复命令链沉淀为可复用脚本入口，并在工具说明中标记优先使用。",
                expected_benefit="减少重复手工步骤与上下文切换，缩短会话时长。",
                evidence={
                    "repeat_step_count": metrics.get("codex_repeat_step_count", 0.0),
                    "examples": evidence.get("repeated_examples", []),
                },
                status="proposed",
                created_at=created_at,
                updated_at=created_at,
            )
        )

    if metrics.get("codex_low_value_read_ratio", 0.0) >= 3.0:
        suggestions.append(
            Suggestion(
                id=f"SUGG-{int(time.time())}-DOC",
                type="doc",
                title="低价值读取比例偏高",
                action="补充模块索引文档与排障入口，减少无效读取。",
                expected_benefit="提升首个有效改动产出速度，降低无产出会话比例。",
                evidence={
                    "low_value_read_ratio": metrics.get("codex_low_value_read_ratio", 0.0),
                    "read_count": evidence.get("read_count", 0),
                    "edit_count": evidence.get("edit_count", 0),
                    "test_count": evidence.get("test_count", 0),
                },
                status="proposed",
                created_at=created_at,
                updated_at=created_at,
            )
        )

    if metrics.get("codex_tool_error_total", 0.0) >= 10:
        suggestions.append(
            Suggestion(
                id=f"SUGG-{int(time.time())}-PROC",
                type="process",
                title="工具失败热点需要流程治理",
                action="建立失败热点工具的前置检查清单与回退策略。",
                expected_benefit="降低 tool_error 峰值并减少中断恢复成本。",
                evidence={
                    "tool_error_total": metrics.get("codex_tool_error_total", 0.0),
                    "tool_error_hotspots": evidence.get("tool_error_hotspots", []),
                },
                status="proposed",
                created_at=created_at,
                updated_at=created_at,
            )
        )

    return suggestions


def ensure_dirs(base_dir: Path) -> Dict[str, Path]:
    # 统一管理结果落盘目录，便于后续检索与审计
    paths = {
        "base": base_dir,
        "snapshots": base_dir / "snapshots",
        "reports": base_dir / "reports",
        "suggestions": base_dir / "suggestions",
        "audit": base_dir / "audit",
        "postmortem": base_dir / "postmortem",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def write_json_retry(path: Path, payload: Any, retries: int = 3) -> None:
    """带重试的原子写入，避免中断时产生半文件。"""
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    for attempt in range(1, retries + 1):
        try:
            tmp_path = path.with_suffix(path.suffix + ".tmp")
            tmp_path.write_text(text, encoding="utf-8")
            tmp_path.replace(path)
            return
        except OSError:
            if attempt == retries:
                raise
            time.sleep(min(0.3 * attempt, 1.0))


def load_suggestions(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        return []
    payload = json.loads(content)
    if isinstance(payload, list):
        return payload
    return []


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def generate_report_markdown(snapshot: Dict[str, Any], suggestions: List[Suggestion]) -> str:
    metrics = snapshot["metrics"]
    lines = [
        "# Codex OTel 每日分析报告",
        "",
        f"- 生成时间: {snapshot['generated_at']}",
        f"- 窗口: 近 {snapshot['window_hours']} 小时",
        f"- 事件总量: {snapshot['event_total']}",
        f"- 会话总量: {int(metrics.get('codex_total_sessions', 0))}",
        "",
        "## 核心指标",
        "",
        f"- 重复步骤次数: {metrics.get('codex_repeat_step_count', 0):.0f}",
        f"- 低价值读取比: {metrics.get('codex_low_value_read_ratio', 0):.2f}",
        f"- tool_error 总数: {metrics.get('codex_tool_error_total', 0):.0f}",
        f"- 工具成功率: {metrics.get('codex_tool_success_rate', 0):.2%}",
        f"- 首个有效产出时延(秒): {metrics.get('codex_first_effective_output_latency_seconds', 0):.2f}",
        "",
        "## 建议候选",
        "",
    ]
    if not suggestions:
        lines.append("- 无新增建议候选。")
    else:
        for item in suggestions:
            lines.extend(
                [
                    f"- [{item.type}] {item.title}",
                    f"  - 建议动作: {item.action}",
                    f"  - 预期收益: {item.expected_benefit}",
                    f"  - 证据: `{json.dumps(item.evidence, ensure_ascii=False)}`",
                ]
            )
    return "\n".join(lines) + "\n"


def persist_snapshot_and_reports(snapshot: Dict[str, Any], suggestions: List[Suggestion], base_dir: Path) -> Dict[str, Path]:
    paths = ensure_dirs(base_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    snapshot_path = paths["snapshots"] / f"snapshot-{ts}.json"
    latest_path = paths["snapshots"] / "latest.json"
    report_path = paths["reports"] / f"report-{ts}.md"
    suggestion_path = paths["suggestions"] / "suggestions.json"

    # 核心分析产物写入并保留 latest 快捷入口
    write_json_retry(snapshot_path, snapshot)
    write_json_retry(latest_path, snapshot)
    report_path.write_text(generate_report_markdown(snapshot, suggestions), encoding="utf-8")

    existing = load_suggestions(suggestion_path)
    for item in suggestions:
        existing.append(item.__dict__)
    write_json_retry(suggestion_path, existing)

    return {
        "snapshot": snapshot_path,
        "latest": latest_path,
        "report": report_path,
        "suggestions": suggestion_path,
    }


def review_transition(
    base_dir: Path,
    suggestion_id: str,
    target_status: str,
    actor: str,
    reason: str,
) -> Dict[str, Any]:
    paths = ensure_dirs(base_dir)
    suggestion_path = paths["suggestions"] / "suggestions.json"
    items = load_suggestions(suggestion_path)

    # 状态机约束，防止越级操作
    allowed = {
        "proposed": {"approved", "rejected"},
        "approved": {"implemented", "rejected"},
        "implemented": set(),
        "rejected": set(),
    }

    matched = None
    for item in items:
        if item.get("id") == suggestion_id:
            matched = item
            break
    if not matched:
        raise ValueError(f"建议不存在: {suggestion_id}")

    current = matched.get("status", "proposed")
    if target_status not in allowed.get(current, set()):
        raise ValueError(f"非法状态流转: {current} -> {target_status}")

    matched["status"] = target_status
    matched["updated_at"] = now_iso()
    matched.setdefault("review_history", []).append(
        {
            "actor": actor,
            "reason": reason,
            "from": current,
            "to": target_status,
            "ts": now_iso(),
        }
    )

    write_json_retry(suggestion_path, items)
    append_jsonl(
        paths["audit"] / "review_audit.jsonl",
        {
            "event": "suggestion_review",
            "suggestion_id": suggestion_id,
            "from": current,
            "to": target_status,
            "actor": actor,
            "reason": reason,
            "ts": now_iso(),
        },
    )

    return matched


def compare_regression(before: Dict[str, Any], after: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """输出 improved/neutral/regressed，并给出逐指标差异。"""
    b = before.get("metrics", {})
    a = after.get("metrics", {})

    # 高值越好与低值越好指标分别处理
    higher_better = ["codex_tool_success_rate"]
    lower_better = ["codex_low_value_read_ratio", "codex_first_effective_output_latency_seconds", "codex_tool_error_total"]

    improved = 0
    regressed = 0
    detail: Dict[str, Any] = {}

    for key in higher_better:
        bv, av = float(b.get(key, 0.0)), float(a.get(key, 0.0))
        detail[key] = {"before": bv, "after": av, "delta": av - bv}
        if av > bv:
            improved += 1
        elif av < bv:
            regressed += 1

    for key in lower_better:
        bv, av = float(b.get(key, 0.0)), float(a.get(key, 0.0))
        detail[key] = {"before": bv, "after": av, "delta": av - bv}
        if av < bv:
            improved += 1
        elif av > bv:
            regressed += 1

    if regressed > improved:
        return "regressed", detail
    if improved > regressed:
        return "improved", detail
    return "neutral", detail


def build_postmortem_template(suggestion: Dict[str, Any], regression_detail: Dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Codex 优化回归复盘",
            "",
            f"- 建议ID: {suggestion.get('id')}",
            f"- 建议标题: {suggestion.get('title')}",
            f"- 生成时间: {now_iso()}",
            "",
            "## 现象",
            "",
            "- 描述回归表现与影响范围。",
            "",
            "## 指标对比",
            "",
            "```json",
            json.dumps(regression_detail, ensure_ascii=False, indent=2),
            "```",
            "",
            "## 可能原因",
            "",
            "- 变更执行与假设不一致",
            "- 指标口径偏差",
            "- 新增约束导致副作用",
            "",
            "## 修正动作",
            "",
            "1. 回滚或修正相关脚本/文档。",
            "2. 补充验证用例，避免再次回归。",
            "3. 更新运行手册和阈值配置。",
        ]
    ) + "\n"


def run_regression(
    base_dir: Path,
    suggestion_id: str,
    before_path: Path,
    after_path: Path,
    actor: str,
    reason: str,
) -> Dict[str, Any]:
    paths = ensure_dirs(base_dir)
    suggestion_path = paths["suggestions"] / "suggestions.json"
    items = load_suggestions(suggestion_path)

    matched = None
    for item in items:
        if item.get("id") == suggestion_id:
            matched = item
            break
    if not matched:
        raise ValueError(f"建议不存在: {suggestion_id}")

    before = json.loads(before_path.read_text(encoding="utf-8"))
    after = json.loads(after_path.read_text(encoding="utf-8"))
    result, detail = compare_regression(before, after)

    matched["regression_result"] = result
    matched["regression_detail"] = detail
    matched["updated_at"] = now_iso()

    postmortem_path = None
    if result == "regressed":
        postmortem_path = paths["postmortem"] / f"postmortem-{suggestion_id}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.md"
        postmortem_path.write_text(build_postmortem_template(matched, detail), encoding="utf-8")

    write_json_retry(suggestion_path, items)
    append_jsonl(
        paths["audit"] / "review_audit.jsonl",
        {
            "event": "suggestion_regression",
            "suggestion_id": suggestion_id,
            "result": result,
            "actor": actor,
            "reason": reason,
            "postmortem": str(postmortem_path) if postmortem_path else "",
            "ts": now_iso(),
        },
    )

    payload = {
        "suggestion_id": suggestion_id,
        "result": result,
        "detail": detail,
    }
    if postmortem_path:
        payload["postmortem"] = str(postmortem_path)
    return payload


def run_analyze(input_path: Path, base_dir: Path, window_hours: int) -> Dict[str, Any]:
    raw_rows = load_jsonl(input_path)
    normalized: List[Dict[str, Any]] = []
    for row in raw_rows:
        for event in flatten_otlp_payload(row):
            normalized.append(normalize_event(event))

    snapshot = compute_snapshot(normalized, window_hours=window_hours)
    suggestions = build_suggestions(snapshot)
    persisted = persist_snapshot_and_reports(snapshot, suggestions, base_dir)

    return {
        "snapshot": snapshot,
        "suggestions_created": [item.__dict__ for item in suggestions],
        "paths": {k: str(v) for k, v in persisted.items()},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Codex OTel 分析与治理工具")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="结果落盘目录")

    subparsers = parser.add_subparsers(dest="command", required=True)

    analyze = subparsers.add_parser("analyze", help="分析 OTel 事件并产出快照与建议")
    analyze.add_argument("--input", required=True, help="输入 JSONL 文件路径")
    analyze.add_argument("--window-hours", type=int, default=24, help="统计窗口（小时）")

    review = subparsers.add_parser("review", help="审核建议状态流转")
    review.add_argument("--id", required=True, help="建议 ID")
    review.add_argument("--to", required=True, choices=["approved", "rejected", "implemented"], help="目标状态")
    review.add_argument("--actor", required=True, help="审核人")
    review.add_argument("--reason", required=True, help="审核原因")

    regress = subparsers.add_parser("regress", help="执行实施后回归评估")
    regress.add_argument("--id", required=True, help="建议 ID")
    regress.add_argument("--before", required=True, help="实施前快照")
    regress.add_argument("--after", required=True, help="实施后快照")
    regress.add_argument("--actor", required=True, help="评估人")
    regress.add_argument("--reason", required=True, help="评估说明")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = Path(args.data_dir).resolve()

    if args.command == "analyze":
        result = run_analyze(Path(args.input).resolve(), base_dir, args.window_hours)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.command == "review":
        result = review_transition(base_dir, args.id, args.to, args.actor, args.reason)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.command == "regress":
        result = run_regression(
            base_dir=base_dir,
            suggestion_id=args.id,
            before_path=Path(args.before).resolve(),
            after_path=Path(args.after).resolve(),
            actor=args.actor,
            reason=args.reason,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    raise ValueError(f"未知命令: {args.command}")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)

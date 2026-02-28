#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.codex.otel_analytics import (
    review_transition,
    run_analyze,
    run_regression,
    write_json_retry,
)


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def build_mock_events(path: Path) -> None:
    """构造一组可复现的 mock 事件，覆盖分析-建议-回归主链路。"""
    base_ts = int(datetime.now(timezone.utc).timestamp())
    events = [
        {"timestamp": base_ts - 100, "event": "session_start", "session_id": "drill-1", "tool_name": "init"},
    ]
    # 连续重复读取，故意触发“脚本化候选”和“低价值读取”阈值
    for idx in range(8):
        events.append(
            {
                "timestamp": base_ts - 95 + idx,
                "event": "open",
                "session_id": "drill-1",
                "tool_name": "open",
                "command": "cat a",
            }
        )
    events.extend(
        [
            {"timestamp": base_ts - 84, "event": "tool_error", "session_id": "drill-1", "tool_name": "apply_patch", "outcome": "error"},
            {"timestamp": base_ts - 80, "event": "patch_apply", "session_id": "drill-1", "tool_name": "apply_patch", "outcome": "ok"},
            {"timestamp": base_ts - 70, "event": "session_end", "session_id": "drill-1", "tool_name": "done"},
        ]
    )
    text = "\n".join(json.dumps(item, ensure_ascii=False) for item in events) + "\n"
    path.write_text(text, encoding="utf-8")


def main() -> int:
    drill_dir = REPO_ROOT / "runtime" / "data" / "codex_otel" / "drills" / now_utc()
    drill_dir.mkdir(parents=True, exist_ok=True)

    input_path = drill_dir / "events.jsonl"
    build_mock_events(input_path)

    # 1) 触发分析与建议生成
    analyze_result = run_analyze(input_path=input_path, base_dir=REPO_ROOT / "runtime" / "data" / "codex_otel", window_hours=24)
    suggestions = analyze_result.get("suggestions_created", [])

    # 2) 模拟审核通过一个建议（如存在）
    reviewed = None
    regression = None
    if suggestions:
        first_id = suggestions[0]["id"]
        reviewed = review_transition(
            base_dir=REPO_ROOT / "runtime" / "data" / "codex_otel",
            suggestion_id=first_id,
            target_status="approved",
            actor="drill",
            reason="演练自动审核",
        )
        reviewed = review_transition(
            base_dir=REPO_ROOT / "runtime" / "data" / "codex_otel",
            suggestion_id=first_id,
            target_status="implemented",
            actor="drill",
            reason="演练自动实施",
        )

        # 3) 构造 before/after 触发回归评估
        before_path = drill_dir / "before.json"
        after_path = drill_dir / "after.json"
        write_json_retry(
            before_path,
            {
                "metrics": {
                    "codex_tool_success_rate": 0.90,
                    "codex_low_value_read_ratio": 1.20,
                    "codex_first_effective_output_latency_seconds": 8.0,
                    "codex_tool_error_total": 2.0,
                }
            },
        )
        write_json_retry(
            after_path,
            {
                "metrics": {
                    "codex_tool_success_rate": 0.95,
                    "codex_low_value_read_ratio": 0.90,
                    "codex_first_effective_output_latency_seconds": 5.0,
                    "codex_tool_error_total": 1.0,
                }
            },
        )
        regression = run_regression(
            base_dir=REPO_ROOT / "runtime" / "data" / "codex_otel",
            suggestion_id=first_id,
            before_path=before_path,
            after_path=after_path,
            actor="drill",
            reason="演练评估",
        )

    summary = {
        "drill_dir": str(drill_dir),
        "input": str(input_path),
        "analyze": analyze_result,
        "reviewed": reviewed,
        "regression": regression,
    }
    summary_path = drill_dir / "summary.json"
    write_json_retry(summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

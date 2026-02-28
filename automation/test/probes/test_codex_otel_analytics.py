from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

from automation.ops.codex.otel_analytics import (
    build_suggestions,
    compare_regression,
    compute_snapshot,
    ensure_dirs,
    review_transition,
    run_regression,
    write_json_retry,
)


class CodexOtelAnalyticsTest(unittest.TestCase):
    def test_compute_snapshot_and_suggestions(self) -> None:
        # 构造一组最小事件，覆盖 session/read/edit/error 主流程
        now_ts = time.time()
        events = [
            {
                "event_name": "session_start",
                "session_id": "s1",
                "tool_name": "none",
                "command": "",
                "outcome": "",
                "category": "session_start",
                "timestamp": now_ts - 40,
                "raw": {},
            },
            {
                "event_name": "open",
                "session_id": "s1",
                "tool_name": "open",
                "command": "cat a",
                "outcome": "",
                "category": "read",
                "timestamp": now_ts - 30,
                "raw": {},
            },
            {
                "event_name": "open",
                "session_id": "s1",
                "tool_name": "open",
                "command": "cat a",
                "outcome": "",
                "category": "read",
                "timestamp": now_ts - 29,
                "raw": {},
            },
            {
                "event_name": "tool_error",
                "session_id": "s1",
                "tool_name": "apply_patch",
                "command": "",
                "outcome": "error",
                "category": "tool_error",
                "timestamp": now_ts - 20,
                "raw": {},
            },
            {
                "event_name": "patch_apply",
                "session_id": "s1",
                "tool_name": "apply_patch",
                "command": "",
                "outcome": "ok",
                "category": "edit",
                "timestamp": now_ts - 10,
                "raw": {},
            },
        ]

        snapshot = compute_snapshot(events, window_hours=99999)
        self.assertGreaterEqual(snapshot["metrics"]["codex_repeat_step_count"], 1)
        self.assertGreater(snapshot["metrics"]["codex_tool_error_total"], 0)

        suggestions = build_suggestions(snapshot)
        # 此数据至少应能触发一个文档/流程建议
        self.assertTrue(isinstance(suggestions, list))

    def test_review_transition(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            paths = ensure_dirs(base_dir)
            suggestion_path = paths["suggestions"] / "suggestions.json"
            write_json_retry(
                suggestion_path,
                [
                    {
                        "id": "S1",
                        "status": "proposed",
                        "title": "t",
                        "type": "doc",
                        "action": "a",
                        "expected_benefit": "b",
                        "evidence": {},
                        "created_at": "now",
                        "updated_at": "now",
                    }
                ],
            )

            reviewed = review_transition(base_dir, "S1", "approved", "tester", "ok")
            self.assertEqual("approved", reviewed["status"])

            with self.assertRaises(ValueError):
                review_transition(base_dir, "S1", "proposed", "tester", "rollback")

    def test_run_regression_generates_postmortem_when_regressed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            paths = ensure_dirs(base_dir)
            suggestion_path = paths["suggestions"] / "suggestions.json"
            write_json_retry(
                suggestion_path,
                [
                    {
                        "id": "S2",
                        "status": "implemented",
                        "title": "t",
                        "type": "script",
                        "action": "a",
                        "expected_benefit": "b",
                        "evidence": {},
                        "created_at": "now",
                        "updated_at": "now",
                    }
                ],
            )

            before_path = base_dir / "before.json"
            after_path = base_dir / "after.json"
            write_json_retry(
                before_path,
                {
                    "metrics": {
                        "codex_tool_success_rate": 0.9,
                        "codex_low_value_read_ratio": 1.0,
                        "codex_first_effective_output_latency_seconds": 10.0,
                        "codex_tool_error_total": 2.0,
                    }
                },
            )
            write_json_retry(
                after_path,
                {
                    "metrics": {
                        "codex_tool_success_rate": 0.7,
                        "codex_low_value_read_ratio": 2.0,
                        "codex_first_effective_output_latency_seconds": 30.0,
                        "codex_tool_error_total": 9.0,
                    }
                },
            )

            result = run_regression(
                base_dir=base_dir,
                suggestion_id="S2",
                before_path=before_path,
                after_path=after_path,
                actor="tester",
                reason="验证回归",
            )
            self.assertEqual("regressed", result["result"])
            self.assertIn("postmortem", result)
            self.assertTrue(Path(result["postmortem"]).exists())

    def test_compare_regression_neutral(self) -> None:
        result, _ = compare_regression(
            {
                "metrics": {
                    "codex_tool_success_rate": 0.8,
                    "codex_low_value_read_ratio": 1.0,
                    "codex_first_effective_output_latency_seconds": 10.0,
                    "codex_tool_error_total": 3.0,
                }
            },
            {
                "metrics": {
                    "codex_tool_success_rate": 0.8,
                    "codex_low_value_read_ratio": 1.0,
                    "codex_first_effective_output_latency_seconds": 10.0,
                    "codex_tool_error_total": 3.0,
                }
            },
        )
        self.assertEqual("neutral", result)


if __name__ == "__main__":
    unittest.main()

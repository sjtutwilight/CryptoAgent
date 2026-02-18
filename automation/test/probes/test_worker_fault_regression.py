from __future__ import annotations

import unittest

from automation.test.probes.worker_probe import evaluate_fault_regression


class FaultRegressionProbeTest(unittest.TestCase):
    def test_pass_with_required_structured_events(self) -> None:
        role_id = "role-a"
        entries = [
            {"role_id": role_id, "event": "ws.reconnect.start", "ts": "t1", "level": "INFO", "raw": "x"},
            {"role_id": role_id, "event": "ws.reconnect.success", "ts": "t2", "level": "INFO", "raw": "x"},
            {"role_id": role_id, "event": "caller.response", "ts": "t3", "level": "INFO", "raw": "x"},
            {"role_id": role_id, "event": "pipeline.finish", "ts": "t4", "level": "INFO", "raw": "x"},
        ]

        summary, _ = evaluate_fault_regression(entries, [], [role_id], rules=None, expect_backfill=False)

        self.assertEqual("PASS", summary["role_results"][role_id]["status"])
        self.assertEqual([], summary["failed_roles"])

    def test_fail_on_missing_and_fail_event(self) -> None:
        role_id = "role-b"
        entries = [
            {"role_id": role_id, "event": "ws.reconnect.start", "ts": "t1", "level": "INFO", "raw": "x"},
            {"role_id": role_id, "event": "handler.error", "ts": "t2", "level": "ERROR", "raw": "x"},
        ]

        summary, _ = evaluate_fault_regression(entries, [], [role_id], rules=None, expect_backfill=False)

        result = summary["role_results"][role_id]
        self.assertEqual("FAIL", result["status"])
        self.assertIn("pipeline.finish", result["missing_events"])
        self.assertTrue(any(item["event"] == "handler.error" for item in result["failed_events"]))

    def test_text_fallback_for_backfill_event(self) -> None:
        role_id = "role-c"
        entries = [
            {"role_id": role_id, "event": "ws.reconnect.start", "ts": "t1", "level": "INFO", "raw": "x"},
            {"role_id": role_id, "event": "ws.reconnect.success", "ts": "t2", "level": "INFO", "raw": "x"},
            {"role_id": role_id, "event": "caller.response", "ts": "t3", "level": "INFO", "raw": "x"},
            {"role_id": role_id, "event": "pipeline.finish", "ts": "t4", "level": "INFO", "raw": "x"},
        ]
        lines = [
            "2026/01/01 role role-c worker-0: range backfill [1, 10] succeeded via rest",
            "2026/01/01 role role-c worker-0: trigger backfill range",
        ]

        summary, _ = evaluate_fault_regression(entries, lines, [role_id], rules=None, expect_backfill=True)

        result = summary["role_results"][role_id]
        self.assertEqual("PASS", result["status"])
        self.assertGreaterEqual(result["fallback_hits"].get("integrity.backfill.success", 0), 1)


if __name__ == "__main__":
    unittest.main()

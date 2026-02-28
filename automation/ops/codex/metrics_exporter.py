#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Dict, List


def load_snapshot(snapshot_path: Path) -> Dict[str, float]:
    """读取最新快照并提取 Prometheus 指标。"""
    if not snapshot_path.exists():
        return {}
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    return payload.get("metrics", {}) if isinstance(payload, dict) else {}


def load_suggestion_stats(suggestion_path: Path) -> Dict[str, int]:
    # 建议状态统计用于观测闭环进度
    if not suggestion_path.exists():
        return {}
    payload = json.loads(suggestion_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return {}

    counts: Dict[str, int] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1
    return counts


def render_metrics(snapshot_metrics: Dict[str, float], suggestion_stats: Dict[str, int]) -> str:
    lines: List[str] = []

    lines.append("# HELP codex_analysis_last_run_timestamp 最近一次分析的 Unix 时间")
    lines.append("# TYPE codex_analysis_last_run_timestamp gauge")
    lines.append(f"codex_analysis_last_run_timestamp {time.time():.0f}")

    for key in [
        "codex_total_sessions",
        "codex_repeat_step_count",
        "codex_tool_error_total",
        "codex_tool_success_rate",
        "codex_low_value_read_ratio",
        "codex_first_effective_output_latency_seconds",
        "codex_session_no_output_duration_seconds",
    ]:
        value = float(snapshot_metrics.get(key, 0.0))
        lines.append(f"# TYPE {key} gauge")
        lines.append(f"{key} {value}")

    lines.append("# HELP codex_suggestion_total 建议状态数量")
    lines.append("# TYPE codex_suggestion_total gauge")
    for status, count in sorted(suggestion_stats.items()):
        lines.append(f'codex_suggestion_total{{status="{status}"}} {count}')

    return "\n".join(lines) + "\n"


def build_handler(snapshot_path: Path, suggestion_path: Path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path not in {"/metrics", "/"}:
                self.send_response(404)
                self.end_headers()
                return

            snapshot_metrics = load_snapshot(snapshot_path)
            suggestion_stats = load_suggestion_stats(suggestion_path)
            body = render_metrics(snapshot_metrics, suggestion_stats).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            # 导出器保持静默，避免刷屏
            return

    return Handler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Codex OTel 聚合指标导出器")
    parser.add_argument("--snapshot", required=True, help="latest snapshot JSON 路径")
    parser.add_argument("--suggestions", default="/runtime/data/codex_otel/suggestions/suggestions.json", help="建议状态文件路径")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址")
    parser.add_argument("--port", type=int, default=9470, help="监听端口")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot_path = Path(args.snapshot).resolve()
    suggestion_path = Path(args.suggestions).resolve()
    server = HTTPServer((args.host, args.port), build_handler(snapshot_path, suggestion_path))
    print(f"codex metrics exporter listening on {args.host}:{args.port}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

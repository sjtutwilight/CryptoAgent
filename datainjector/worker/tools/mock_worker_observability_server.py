#!/usr/bin/env python3
import json
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

START = time.time()


def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def elapsed() -> int:
    return int(time.time() - START)


def metric_lines() -> str:
    t = max(elapsed(), 1)
    caller = t * 30
    pipeline = t * 24
    sink = t * 18
    drops = t * 60
    reconnects = t * 4
    dedup = t * 12
    result_success = t * 14
    result_fail = t * 2
    result_timeout = t
    pending_count = t * 6
    pending_sum = float(t * 240)
    enqueue_count = t * 12
    enqueue_sum = float(t * 3)
    backlog = max(0, 120 - t // 2)
    inflight = 1 if (t % 20) < 10 else 0

    return "\n".join([
        "# HELP worker_integrity_backfill_sessions_inflight mock gauge",
        "# TYPE worker_integrity_backfill_sessions_inflight gauge",
        f'worker_integrity_backfill_sessions_inflight{{role_id="role-a",stream_key="btc-usdt",backfill_type="range"}} {inflight}',
        "# HELP worker_integrity_backfill_schedule_dedup_total mock counter",
        "# TYPE worker_integrity_backfill_schedule_dedup_total counter",
        f'worker_integrity_backfill_schedule_dedup_total{{role_id="role-a",stream_key="btc-usdt",backfill_type="range"}} {dedup}',
        "# HELP worker_integrity_backfill_result_total mock counter",
        "# TYPE worker_integrity_backfill_result_total counter",
        f'worker_integrity_backfill_result_total{{role_id="role-a",backfill_type="range",status="success",error_class="none"}} {result_success}',
        f'worker_integrity_backfill_result_total{{role_id="role-a",backfill_type="range",status="fail",error_class="pipeline_error"}} {result_fail}',
        f'worker_integrity_backfill_result_total{{role_id="role-a",backfill_type="range",status="timeout",error_class="timeout"}} {result_timeout}',
        "# HELP worker_integrity_backfill_pending_duration_seconds mock histogram",
        "# TYPE worker_integrity_backfill_pending_duration_seconds histogram",
        f'worker_integrity_backfill_pending_duration_seconds_bucket{{role_id="role-a",stream_key="btc-usdt",backfill_type="range",status="success",le="1"}} {pending_count // 3}',
        f'worker_integrity_backfill_pending_duration_seconds_bucket{{role_id="role-a",stream_key="btc-usdt",backfill_type="range",status="success",le="5"}} {pending_count // 2}',
        f'worker_integrity_backfill_pending_duration_seconds_bucket{{role_id="role-a",stream_key="btc-usdt",backfill_type="range",status="success",le="10"}} {pending_count}',
        f'worker_integrity_backfill_pending_duration_seconds_bucket{{role_id="role-a",stream_key="btc-usdt",backfill_type="range",status="success",le="30"}} {pending_count}',
        f'worker_integrity_backfill_pending_duration_seconds_bucket{{role_id="role-a",stream_key="btc-usdt",backfill_type="range",status="success",le="60"}} {pending_count}',
        f'worker_integrity_backfill_pending_duration_seconds_bucket{{role_id="role-a",stream_key="btc-usdt",backfill_type="range",status="success",le="+Inf"}} {pending_count}',
        f'worker_integrity_backfill_pending_duration_seconds_sum{{role_id="role-a",stream_key="btc-usdt",backfill_type="range",status="success"}} {pending_sum}',
        f'worker_integrity_backfill_pending_duration_seconds_count{{role_id="role-a",stream_key="btc-usdt",backfill_type="range",status="success"}} {pending_count}',
        "# HELP worker_integrity_backfill_enqueue_latency_seconds mock histogram",
        "# TYPE worker_integrity_backfill_enqueue_latency_seconds histogram",
        f'worker_integrity_backfill_enqueue_latency_seconds_bucket{{role_id="role-a",backfill_type="range",status="success",le="0.1"}} {enqueue_count // 3}',
        f'worker_integrity_backfill_enqueue_latency_seconds_bucket{{role_id="role-a",backfill_type="range",status="success",le="0.5"}} {enqueue_count // 2}',
        f'worker_integrity_backfill_enqueue_latency_seconds_bucket{{role_id="role-a",backfill_type="range",status="success",le="1"}} {enqueue_count}',
        f'worker_integrity_backfill_enqueue_latency_seconds_bucket{{role_id="role-a",backfill_type="range",status="success",le="2"}} {enqueue_count}',
        f'worker_integrity_backfill_enqueue_latency_seconds_bucket{{role_id="role-a",backfill_type="range",status="success",le="+Inf"}} {enqueue_count}',
        f'worker_integrity_backfill_enqueue_latency_seconds_sum{{role_id="role-a",backfill_type="range",status="success"}} {enqueue_sum}',
        f'worker_integrity_backfill_enqueue_latency_seconds_count{{role_id="role-a",backfill_type="range",status="success"}} {enqueue_count}',
        "# HELP worker_integrity_backfill_compensation_backlog mock gauge",
        "# TYPE worker_integrity_backfill_compensation_backlog gauge",
        f'worker_integrity_backfill_compensation_backlog{{role_id="role-a"}} {backlog}',
        "# HELP worker_task_stage_total mock counter",
        "# TYPE worker_task_stage_total counter",
        f'worker_task_stage_total{{role_id="role-a",stage="caller_accepted",result="event"}} {caller}',
        f'worker_task_stage_total{{role_id="role-a",stage="pipeline_succeeded",result="event"}} {pipeline}',
        f'worker_task_stage_total{{role_id="role-a",stage="final_succeeded",result="success"}} {sink}',
        "# HELP worker_websocket_drops_total mock counter",
        "# TYPE worker_websocket_drops_total counter",
        f'worker_websocket_drops_total{{role_id="role-a",layer="caller_buffer",reason="max_messages_drop_oldest"}} {drops}',
        "# HELP worker_websocket_reconnects_total mock counter",
        "# TYPE worker_websocket_reconnects_total counter",
        f'worker_websocket_reconnects_total{{role_id="role-a",endpoint="wss://mock"}} {reconnects}',
        "# HELP worker_websocket_errors_total mock counter",
        "# TYPE worker_websocket_errors_total counter",
        f'worker_websocket_errors_total{{role_id="role-a",endpoint="wss://mock",error_type="policy_1008"}} {t}',
        ""
    ])


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        data = metric_lines().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        return


def log_loop():
    idx = 0
    while True:
        idx += 1
        payload = {
            "ts": now_ts(),
            "level": "INFO" if idx % 5 else "ERROR",
            "event": "integrity.backfill.result" if idx % 2 else "pipeline.finish",
            "message": "mock worker observability event",
            "service": "datainjector-worker",
            "role_id": "role-a",
            "error_class": "timeout" if idx % 5 == 0 else "none",
            "backfill_type": "range",
            "session_key": "role-a|btc-usdt|range",
            "cmd_id": f"cmd-{idx:04d}",
        }
        print(json.dumps(payload, ensure_ascii=True), flush=True)
        time.sleep(1)


if __name__ == "__main__":
    threading.Thread(target=log_loop, daemon=True).start()
    server = HTTPServer(("0.0.0.0", 9100), MetricsHandler)
    server.serve_forever()

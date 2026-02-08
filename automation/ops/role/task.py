#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.ops.common import print_json  # noqa: E402


DEFAULT_TOPIC = os.getenv("KAFKA_TASK_TOPIC", "batch.tasks")
DEFAULT_CONTAINER = os.getenv("KAFKA_CONTAINER_NAME", "crypto-kafka")
DEFAULT_LOCAL_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_LOCAL") or os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
DEFAULT_CONTAINER_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def _build_task_payload(role_id: str, run_id: str, now_ms: int) -> Dict:
    if role_id == "binance-spot-link-kline-batch":
        start_ms = now_ms - 60 * 60 * 1000
        task_id = f"{role_id}-{run_id}"
        return {
            "task_id": task_id,
            "taskId": task_id,
            "run_id": run_id,
            "metadata": {"symbol": "LINKUSDT", "interval": "1m", "run_id": run_id},
            "query": {
                "symbol": "LINKUSDT",
                "interval": "1m",
                "limit": 1000,
                "startTime": start_ms,
                "endTime": now_ms,
            },
        }
    raise ValueError(f"no task template for role_id: {role_id}")


def _write_with_kcat(bootstrap: str, topic: str, message: str) -> Optional[str]:
    if shutil.which("kcat") is None:
        return None
    cmd = ["kcat", "-P", "-b", bootstrap, "-t", topic]
    proc = subprocess.run(cmd, input=(message + "\n").encode("utf-8"), capture_output=True)
    output = (proc.stdout + proc.stderr).decode("utf-8", errors="ignore")
    if proc.returncode != 0:
        raise RuntimeError(f"kcat produce failed: {output}")
    return output


def _write_with_kafka_console(container: str, bootstrap: str, topic: str, message: str) -> str:
    cmd = [
        "docker",
        "exec",
        "-i",
        container,
        "kafka-console-producer",
        "--bootstrap-server",
        bootstrap,
        "--topic",
        topic,
    ]
    proc = subprocess.run(cmd, input=(message + "\n").encode("utf-8"), capture_output=True)
    output = (proc.stdout + proc.stderr).decode("utf-8", errors="ignore")
    if proc.returncode != 0:
        raise RuntimeError(f"kafka-console-producer failed: {output}")
    return output


def send_task_programmatic(
    role_id: str,
    run_id: Optional[str] = None,
    topic: str = DEFAULT_TOPIC,
    local_bootstrap: str = DEFAULT_LOCAL_BOOTSTRAP,
    container: str = DEFAULT_CONTAINER,
    container_bootstrap: str = DEFAULT_CONTAINER_BOOTSTRAP,
) -> Tuple[Dict, str]:
    if not role_id:
        raise ValueError("role_id is required")
    run_id = run_id or time.strftime("%Y%m%d-%H%M%S")
    now_ms = int(time.time() * 1000)
    payload = _build_task_payload(role_id, run_id, now_ms)
    message = json.dumps(payload, ensure_ascii=True)

    output = _write_with_kcat(local_bootstrap, topic, message)
    if output is not None:
        return payload, f"kcat({local_bootstrap})"

    _write_with_kafka_console(container, container_bootstrap, topic, message)
    return payload, f"kafka-console-producer({container_bootstrap})"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send role task to Kafka")
    parser.add_argument("role_id", help="role_id to send task for")
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload, method = send_task_programmatic(args.role_id)
    task_id = payload.get("task_id") or payload.get("taskId")
    if args.output_json:
        print_json(
            {
                "status": "ok",
                "topic": DEFAULT_TOPIC,
                "method": method,
                "task_id": task_id,
                "payload": payload,
            }
        )
    else:
        print(f"task sent: {task_id} via {method}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print_json({"status": "error", "detail": str(exc)})
        sys.exit(1)

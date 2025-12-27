from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from typing import List, Optional

from automation.test.probes import infra_probe
from ..shared.core.context import RunContext
from ..shared.core.result import ProbeResult, ProbeStatus


def _read_with_kcat(bootstrap: str, topic: str, max_messages: int) -> Optional[str]:
    if shutil.which("kcat") is None:
        return None
    cmd = [
        "kcat",
        "-C",
        "-b",
        bootstrap,
        "-t",
        topic,
        "-o",
        "end",
        "-c",
        str(max_messages),
        "-e",
        "-q",
    ]
    proc = subprocess.run(cmd, capture_output=True)
    output = (proc.stdout + proc.stderr).decode("utf-8", errors="ignore")
    return output


def _read_with_kafka_console(container: str, topic: str, group_id: str, max_messages: int, timeout_ms: int) -> Optional[str]:
    cmd = [
        "docker",
        "exec",
        container,
        "kafka-console-consumer",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        topic,
        "--group",
        group_id,
        "--max-messages",
        str(max_messages),
        "--timeout-ms",
        str(timeout_ms),
        "--consumer-property",
        "auto.offset.reset=latest",
    ]
    proc = subprocess.run(cmd, capture_output=True)
    output = (proc.stdout + proc.stderr).decode("utf-8", errors="ignore")
    return output


def _write_with_kcat(bootstrap: str, topic: str, message: str) -> Optional[str]:
    if shutil.which("kcat") is None:
        return None
    cmd = ["kcat", "-P", "-b", bootstrap, "-t", topic]
    proc = subprocess.run(cmd, input=(message + "\n").encode("utf-8"), capture_output=True)
    output = (proc.stdout + proc.stderr).decode("utf-8", errors="ignore")
    if proc.returncode != 0:
        raise RuntimeError(f"kcat produce failed: {output}")
    return output


def _write_with_kafka_console(container: str, bootstrap: str, topic: str, message: str) -> Optional[str]:
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


def _has_producer_errors(output: str) -> bool:
    text = output.lower()
    return any(
        marker in text
        for marker in (
            "could not be established",
            "disconnected",
            "connection refused",
            "failed to update metadata",
            "error",
        )
    )


def has_message_with_run_id(ctx: RunContext, topic: str) -> ProbeResult:
    run_id = ctx.run_id
    metadata = ctx.metadata or {}
    timeout_sec = int(metadata.get("kafka_wait_timeout", 60))
    interval_sec = int(metadata.get("kafka_wait_interval", 5))
    max_messages = int(metadata.get("kafka_max_messages", 50))
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS_LOCAL") or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    container = os.getenv("KAFKA_CONTAINER_NAME", "crypto-kafka")

    deadline = time.time() + timeout_sec
    last_output = ""
    while time.time() < deadline:
        output = _read_with_kcat(bootstrap, topic, max_messages)
        if output is None:
            group_id = f"e2e-{run_id}"
            output = _read_with_kafka_console(container, topic, group_id, max_messages, int(interval_sec * 1000))
        last_output = output or ""
        if run_id in last_output:
            return ProbeResult(
                status=ProbeStatus.SUCCESS,
                detail=f"run_id found in {topic}",
                metrics={"checked_messages": max_messages},
            )
        time.sleep(interval_sec)

    return ProbeResult(
        status=ProbeStatus.FAIL,
        detail=f"run_id not found in {topic}",
        metrics={"checked_messages": max_messages, "timeout_sec": timeout_sec},
        payload={"last_output": last_output[:1000]},
    )


def send_json_message(ctx: RunContext, topic: str, payload: dict) -> ProbeResult:
    local_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS_LOCAL") or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    container_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    container = os.getenv("KAFKA_CONTAINER_NAME", "crypto-kafka")
    message = json.dumps(payload, ensure_ascii=True)

    try:
        output = _write_with_kcat(local_bootstrap, topic, message)
        method = "kcat" if output is not None else None
        kcat_error = None
    except Exception as exc:
        output = None
        method = None
        kcat_error = str(exc)

    if method is None:
        try:
            output = _write_with_kafka_console(container, container_bootstrap, topic, message)
            method = f"kafka-console-producer({container_bootstrap})"
        except Exception as exc:
            detail = f"produce failed: {exc}"
            if kcat_error:
                detail = f"{detail}; kcat error: {kcat_error}"
            return ProbeResult(status=ProbeStatus.FAIL, detail=detail)

    output_text = output or ""
    if output_text and _has_producer_errors(output_text):
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail="producer reported connection error",
            metrics={"topic": topic},
            payload={"message": message, "output": output_text[:1000]},
        )

    return ProbeResult(
        status=ProbeStatus.SUCCESS,
        detail=f"message sent via {method}",
        metrics={"topic": topic},
        payload={"message": message, "output": output_text[:1000]},
    )


def topic_check(_: RunContext, topic: str) -> ProbeResult:
    payload = infra_probe.snapshot_kafka(topic)
    reachable = payload.get("kafka", {}).get("reachable") is True
    status = ProbeStatus.SUCCESS if reachable else ProbeStatus.FAIL
    detail = "kafka reachable" if reachable else "kafka not reachable"
    return ProbeResult(status=status, detail=detail, payload=payload)


def topic_watch(_: RunContext, __: str) -> ProbeResult:
    # TODO: implement Kafka topic watch probe.
    return ProbeResult(status=ProbeStatus.SKIP, detail="TODO: kafka_probe.topic_watch")

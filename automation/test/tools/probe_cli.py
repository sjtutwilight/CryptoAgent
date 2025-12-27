#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.test.probes import infra_probe, kafka_probe  # type: ignore  # noqa: E402
from automation.test.shared.config_loader import load_env_file  # type: ignore  # noqa: E402
from automation.test.shared.core.context import RunContext  # type: ignore  # noqa: E402
from automation.test.shared.core.result import ProbeResult  # type: ignore  # noqa: E402


def load_infra_env() -> None:
    default_env = REPO_ROOT / "config/infrastructure/env/docker.env"
    env_path = Path(os.getenv("INFRA_ENV_FILE", str(default_env)))
    if not env_path.exists():
        return
    for key, value in load_env_file(env_path).items():
        os.environ[key] = value


def _emit(payload: dict, output_format: str) -> None:
    if output_format == "text":
        infra_probe.render_text(payload)
    else:
        print(json.dumps(payload, ensure_ascii=True))


def _emit_probe_result(result: ProbeResult, output_format: str) -> None:
    payload = {
        "status": result.status.value,
        "detail": result.detail,
        "metrics": result.metrics,
        "payload": result.payload,
    }
    if output_format == "text":
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=True))


def cmd_infra(args: argparse.Namespace) -> None:
    if args.target == "flink":
        payload = infra_probe.snapshot_flink()
    elif args.target == "docker":
        payload = infra_probe.snapshot_docker(args.containers)
    elif args.target == "clickhouse":
        payload = infra_probe.snapshot_clickhouse()
    elif args.target == "kafka":
        payload = infra_probe.snapshot_kafka(args.topic)
    elif args.target == "stack":
        payload = infra_probe.snapshot_stack()
    else:
        raise RuntimeError(f"unknown infra target: {args.target}")

    _emit(payload, args.format)


def cmd_kafka_topic_check(args: argparse.Namespace) -> None:
    ctx = RunContext(run_id=args.run_id or "manual", scenario="kafka_topic_check", env=args.env)
    result = kafka_probe.topic_check(ctx, args.topic)
    _emit_probe_result(result, args.format)


def cmd_kafka_topic_watch(args: argparse.Namespace) -> None:
    ctx = RunContext(run_id=args.run_id or "manual", scenario="kafka_topic_watch", env=args.env)
    result = kafka_probe.topic_watch(ctx, args.topic)
    _emit_probe_result(result, args.format)


def cmd_kafka_has_run_id(args: argparse.Namespace) -> None:
    ctx = RunContext(run_id=args.run_id, scenario="kafka_run_id", env=args.env, metadata={
        "kafka_wait_timeout": args.wait_timeout,
        "kafka_wait_interval": args.wait_interval,
        "kafka_max_messages": args.max_messages,
    })
    result = kafka_probe.has_message_with_run_id(ctx, args.topic)
    _emit_probe_result(result, args.format)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    infra = sub.add_parser("infra", help="run infra probes")
    infra.add_argument("target", choices=["flink", "docker", "clickhouse", "kafka", "stack"])
    infra.add_argument("containers", nargs="*", help="containers for docker target")
    infra.add_argument("--topic", default=None, help="kafka topic hint")
    infra.add_argument("--format", choices=["json", "text"], default="json")
    infra.set_defaults(func=cmd_infra)

    kafka = sub.add_parser("kafka", help="kafka topic probes")
    kafka_sub = kafka.add_subparsers(dest="kafka_command", required=True)

    kafka_check = kafka_sub.add_parser("topic-check", help="check kafka topic")
    kafka_check.add_argument("--topic", default="dwd_dex_swap", help="kafka topic")
    kafka_check.add_argument("--format", choices=["json", "text"], default="json")
    kafka_check.add_argument("--env", default="local")
    kafka_check.add_argument("--run-id", default=None)
    kafka_check.set_defaults(func=cmd_kafka_topic_check)

    kafka_watch = kafka_sub.add_parser("topic-watch", help="watch kafka topic")
    kafka_watch.add_argument("--topic", default="dwd_dex_swap", help="kafka topic")
    kafka_watch.add_argument("--format", choices=["json", "text"], default="json")
    kafka_watch.add_argument("--env", default="local")
    kafka_watch.add_argument("--run-id", default=None)
    kafka_watch.set_defaults(func=cmd_kafka_topic_watch)

    kafka_run_id = kafka_sub.add_parser("has-run-id", help="check run_id in kafka topic")
    kafka_run_id.add_argument("--topic", required=True, help="kafka topic")
    kafka_run_id.add_argument("--run-id", required=True, help="run_id to match")
    kafka_run_id.add_argument("--wait-timeout", type=int, default=60)
    kafka_run_id.add_argument("--wait-interval", type=int, default=5)
    kafka_run_id.add_argument("--max-messages", type=int, default=50)
    kafka_run_id.add_argument("--format", choices=["json", "text"], default="json")
    kafka_run_id.add_argument("--env", default="local")
    kafka_run_id.set_defaults(func=cmd_kafka_has_run_id)

    return parser


def main() -> None:
    load_infra_env()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

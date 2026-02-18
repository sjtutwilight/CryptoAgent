#!/usr/bin/env python3
"""
周期性将 Kafka 微结构 topic 增量导出为 ODS 样式目录。

默认优先使用 kcat；若系统未安装 kcat，则回退到容器内 kafka-console-consumer。
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


UTC = dt.timezone.utc

DEFAULT_TOPICS = [
    "perp.orderbook",
    "spot.orderbook",
    "perp.aggtrades",
    "spot.aggtrades",
]


def now_utc() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def now_iso() -> str:
    return now_utc().isoformat()


def parse_int_ts(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def pick_event_ts_ms(record: Dict[str, Any]) -> Optional[int]:
    for key in ("exchange_ts", "event_time", "ingest_ts"):
        ts = parse_int_ts(record.get(key))
        if ts is not None:
            return ts
    return None


def iso_from_ms(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=UTC).isoformat()


def topic_domain(topic: str) -> str:
    if "orderbook" in topic:
        if topic.startswith("spot."):
            return "cex.spot.orderbook"
        return "cex.perp.orderbook"
    if "aggtrades" in topic:
        if topic.startswith("spot."):
            return "cex.spot.trades"
        return "cex.perp.trades"
    return "cex.stream"


def topic_grain(topic: str) -> str:
    if "orderbook" in topic:
        return "orderbook_diff"
    if "aggtrades" in topic:
        return "aggtrade"
    return "stream"


def topic_resource_path(prefix: str, topic: str) -> str:
    return f"{prefix.rstrip('/')}/{topic}"


def infer_token_from_symbol(symbol: str) -> Optional[str]:
    text = (symbol or "").strip().upper()
    if not text:
        return None

    if "/" in text:
        base = text.split("/", 1)[0]
    elif "_" in text:
        base = text.split("_", 1)[0]
    else:
        base = text

    if base.endswith("PERP") and len(base) > 4:
        base = base[:-4]

    quote_suffixes = (
        "USDT",
        "USDC",
        "FDUSD",
        "BUSD",
        "TUSD",
        "USDP",
        "DAI",
        "USD",
        "BTC",
        "ETH",
        "BNB",
        "EUR",
    )
    for suffix in quote_suffixes:
        if base.endswith(suffix) and len(base) > len(suffix):
            base = base[: -len(suffix)]
            break

    base = base.strip()
    return base.lower() if base else None


def extract_symbol_token_from_first_record(records: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    first = next((item for item in records if isinstance(item, dict)), None)
    if first is None:
        return None, None

    symbol_raw = first.get("symbol")
    symbol: Optional[str] = None
    if isinstance(symbol_raw, str) and symbol_raw.strip():
        symbol = symbol_raw.strip().upper()

    token_raw = first.get("token")
    token: Optional[str] = None
    if isinstance(token_raw, str) and token_raw.strip():
        token = token_raw.strip().lower()
    elif symbol:
        token = infer_token_from_symbol(symbol)

    return symbol, token


def build_fingerprint(datasource_id: str, resource_path: str, group_id: str) -> str:
    payload = {
        "datasource_id": datasource_id,
        "resource_path": resource_path,
        "group_id": group_id,
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def parse_iso8601(value: str) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def to_rel_posix(path: Path, root: Path) -> str:
    return str(path.resolve().relative_to(root.resolve())).replace("\\", "/")


def parse_message_lines(output: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for line in output.splitlines():
        if "@@{" not in line:
            continue
        _, payload = line.split("@@", 1)
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            records.append(obj)
    return records


def run_subprocess(cmd: List[str], timeout_seconds: Optional[float] = None) -> Tuple[str, str, int]:
    try:
        cp = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        return cp.stdout, cp.stderr, cp.returncode
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout or b"").decode("utf-8", errors="ignore")
        stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr or b"").decode("utf-8", errors="ignore")
        return stdout, stderr, 124


def consume_with_kcat(
    bootstrap_server: str,
    group_id: str,
    topic: str,
    max_messages: int,
    poll_timeout_ms: int,
    from_beginning: bool,
) -> List[Dict[str, Any]]:
    cmd = [
        "kcat",
        "-b",
        bootstrap_server,
        "-G",
        group_id,
        topic,
        "-q",
        "-f",
        "%k@@%s\n",
        "-c",
        str(max_messages),
        "-X",
        "enable.auto.commit=true",
        "-X",
        "auto.commit.interval.ms=1000",
    ]
    if from_beginning:
        cmd.extend(["-o", "beginning"])
    timeout_seconds = max(1.0, poll_timeout_ms / 1000.0)
    stdout, stderr, _ = run_subprocess(cmd, timeout_seconds=timeout_seconds)
    return parse_message_lines(stdout + "\n" + stderr)


def consume_with_docker_console(
    kafka_container: str,
    bootstrap_server_in_container: str,
    group_id: str,
    topic: str,
    max_messages: int,
    poll_timeout_ms: int,
    from_beginning: bool,
) -> List[Dict[str, Any]]:
    inner = (
        "kafka-console-consumer "
        f"--bootstrap-server {shlex.quote(bootstrap_server_in_container)} "
        f"--topic {shlex.quote(topic)} "
        f"--group {shlex.quote(group_id)} "
        f"--timeout-ms {int(poll_timeout_ms)} "
        f"--max-messages {int(max_messages)} "
        "--property print.key=true "
        "--property key.separator=@@"
    )
    if from_beginning:
        inner += " --from-beginning"
    cmd = ["docker", "exec", kafka_container, "bash", "-lc", inner]
    stdout, stderr, _ = run_subprocess(cmd)
    return parse_message_lines(stdout + "\n" + stderr)


def resolve_backend(requested_backend: str) -> str:
    if requested_backend in {"kcat", "docker-console"}:
        return requested_backend
    return "kcat" if shutil.which("kcat") else "docker-console"


def next_response_index(dataset_dir: Path) -> int:
    max_idx = -1
    for p in dataset_dir.glob("response_*.json"):
        stem = p.stem
        suffix = stem.split("_")[-1]
        if suffix.isdigit():
            max_idx = max(max_idx, int(suffix))
    return max_idx + 1


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def update_or_create_metadata(
    metadata_path: Path,
    topic: str,
    datasource_id: str,
    resource_path: str,
    time_bucket: str,
    fingerprint: str,
    fields: List[str],
    coverage_start_iso: str,
    coverage_end_iso: str,
    symbol: Optional[str],
    token: Optional[str],
) -> Dict[str, Any]:
    existing: Dict[str, Any] = {}
    if metadata_path.exists():
        try:
            existing = json.loads(metadata_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = {}

    existing_fields = set()
    if isinstance(existing.get("schema"), dict):
        response = existing["schema"].get("response")
        if isinstance(response, dict) and isinstance(response.get("fields"), list):
            existing_fields = {str(x) for x in response["fields"]}

    all_fields = sorted(existing_fields.union(fields))
    domain = topic_domain(topic)
    table_id = (
        f"ods_{datasource_id}_{topic}"
        .lower()
        .replace(".", "_")
        .replace("-", "_")
    )
    table = {
        "table_id": table_id,
        "partition_keys": ["datasource_id", "time_bucket"],
        "primary_keys": [],
    }

    start_iso = coverage_start_iso
    end_iso = coverage_end_iso
    if isinstance(existing.get("time"), dict):
        coverage = existing["time"].get("coverage")
        if isinstance(coverage, dict):
            old_start = parse_iso8601(str(coverage.get("start", "")))
            old_end = parse_iso8601(str(coverage.get("end", "")))
            cur_start = parse_iso8601(coverage_start_iso)
            cur_end = parse_iso8601(coverage_end_iso)
            if old_start and cur_start:
                start_iso = min(old_start, cur_start).isoformat()
            if old_end and cur_end:
                end_iso = max(old_end, cur_end).isoformat()

    existing_dataset = existing.get("dataset") if isinstance(existing.get("dataset"), dict) else {}
    final_symbol = symbol or (existing_dataset.get("symbol") if isinstance(existing_dataset.get("symbol"), str) else None)
    final_token = token or (existing_dataset.get("token") if isinstance(existing_dataset.get("token"), str) else None)

    dataset_payload: Dict[str, Any] = {
        "domain": domain,
        "datasource_id": datasource_id,
        "stream_name": topic,
        "entity": "token",
        "grain": topic_grain(topic),
    }
    if final_symbol:
        dataset_payload["symbol"] = final_symbol
    if final_token:
        dataset_payload["token"] = final_token

    metadata = {
        "schema_version": "v1",
        "dataset": dataset_payload,
        "storage": {
            "format": "json",
            "path_template": "data/ods/{datasource_id}/{resource_path}/{time_bucket}/{request_fingerprint}",
            "partition_keys": [
                "datasource_id",
                "resource_path",
                "time_bucket",
                "request_fingerprint",
            ],
        },
        "time": {
            "event_time": "exchange_ts",
            "granularity": "snapshot",
            "timezone": "UTC",
            "coverage": {
                "start": start_iso,
                "end": end_iso,
            },
        },
        "schema": {
            "response": {
                "format": "json",
                "extractor": {
                    "type": "DpathExtractor",
                    "field_path": [],
                },
                "fields": all_fields,
            }
        },
        "table": table,
    }
    write_json(metadata_path, metadata)
    return metadata


def write_manifest(
    dataset_dir: Path,
    dataset_rel: str,
    execution_id: str,
    datasource_id: str,
    topic: str,
    group_id: str,
    bucket: str,
    metadata_rel: str,
    response_rel: str,
    coverage_start_iso: str,
    coverage_end_iso: str,
    record_count: int,
    symbol: Optional[str],
    token: Optional[str],
) -> None:
    now = now_iso()
    dataset_payload: Dict[str, Any] = {
        "domain": topic_domain(topic),
        "datasource_id": datasource_id,
        "stream_name": topic,
    }
    if symbol:
        dataset_payload["symbol"] = symbol
    if token:
        dataset_payload["token"] = token

    params_payload: Dict[str, Any] = {
        "topic": topic,
        "consumer_group": group_id,
    }
    if symbol:
        params_payload["symbol"] = symbol

    manifest = {
        "schema_version": "1.0.0",
        "manifest_type": "ingestion",
        "execution_id": execution_id,
        "status": "success",
        "created_at": now,
        "updated_at": now,
        "inputs": {
            "dataset": dataset_payload,
            "params": params_payload,
        },
        "outputs": {
            "output_dir": dataset_rel,
            "metadata_path": metadata_rel,
            "raw_paths": [response_rel],
        },
        "datasource_id": datasource_id,
        "time_range": {
            "start": coverage_start_iso,
            "end": coverage_end_iso,
            "timezone": "UTC",
        },
        "bucket": bucket,
        "records": record_count,
        "raw_paths": [response_rel],
    }
    manifest_path = dataset_dir / ".meta" / execution_id / "manifest.json"
    write_json(manifest_path, manifest)


def compute_time_bucket(ts_min_ms: int, granularity: str) -> str:
    dt_obj = dt.datetime.fromtimestamp(ts_min_ms / 1000.0, tz=UTC)
    if granularity == "hour":
        return dt_obj.strftime("%Y-%m-%dT%H")
    return dt_obj.strftime("%Y-%m-%d")


def export_topic_once(args: argparse.Namespace, topic: str, backend: str, root_dir: Path) -> int:
    if backend == "kcat":
        records = consume_with_kcat(
            bootstrap_server=args.bootstrap_server,
            group_id=args.consumer_group,
            topic=topic,
            max_messages=args.max_messages_per_topic,
            poll_timeout_ms=args.poll_timeout_ms,
            from_beginning=args.from_beginning,
        )
    else:
        records = consume_with_docker_console(
            kafka_container=args.kafka_container,
            bootstrap_server_in_container=args.bootstrap_server_in_container,
            group_id=args.consumer_group,
            topic=topic,
            max_messages=args.max_messages_per_topic,
            poll_timeout_ms=args.poll_timeout_ms,
            from_beginning=args.from_beginning,
        )

    if not records:
        print(f"[skip] {topic}: no new records")
        return 0

    ts_list = [t for t in (pick_event_ts_ms(r) for r in records) if t is not None]
    if ts_list:
        ts_min = min(ts_list)
        ts_max = max(ts_list)
    else:
        now_ms = int(now_utc().timestamp() * 1000)
        ts_min = now_ms
        ts_max = now_ms

    coverage_start_iso = iso_from_ms(ts_min)
    coverage_end_iso = iso_from_ms(ts_max)
    time_bucket = compute_time_bucket(ts_min, args.time_bucket_granularity)

    resource_path = topic_resource_path(args.resource_prefix, topic)
    fingerprint = build_fingerprint(args.datasource_id, resource_path, args.consumer_group)

    output_root_raw = Path(args.output_root)
    if output_root_raw.is_absolute():
        output_root = output_root_raw.resolve()
    else:
        output_root = (root_dir / output_root_raw).resolve()
    dataset_dir = output_root / args.datasource_id / resource_path / time_bucket / fingerprint
    dataset_dir.mkdir(parents=True, exist_ok=True)

    idx = next_response_index(dataset_dir)
    response_name = f"response_{idx:04d}.json"
    response_path = dataset_dir / response_name
    write_json(response_path, records)

    fields = sorted({k for rec in records for k in rec.keys()})
    symbol, token = extract_symbol_token_from_first_record(records)
    metadata_path = dataset_dir / "metadata.json"
    update_or_create_metadata(
        metadata_path=metadata_path,
        topic=topic,
        datasource_id=args.datasource_id,
        resource_path=resource_path,
        time_bucket=time_bucket,
        fingerprint=fingerprint,
        fields=fields,
        coverage_start_iso=coverage_start_iso,
        coverage_end_iso=coverage_end_iso,
        symbol=symbol,
        token=token,
    )

    execution_id = now_utc().strftime("%Y%m%d-%H%M%S")
    dataset_rel = to_rel_posix(dataset_dir, root_dir)
    metadata_rel = to_rel_posix(metadata_path, root_dir)
    response_rel = to_rel_posix(response_path, root_dir)
    write_manifest(
        dataset_dir=dataset_dir,
        dataset_rel=dataset_rel,
        execution_id=execution_id,
        datasource_id=args.datasource_id,
        topic=topic,
        group_id=args.consumer_group,
        bucket=time_bucket,
        metadata_rel=metadata_rel,
        response_rel=response_rel,
        coverage_start_iso=coverage_start_iso,
        coverage_end_iso=coverage_end_iso,
        record_count=len(records),
        symbol=symbol,
        token=token,
    )

    print(f"[ok] {topic}: {len(records)} records -> {response_path}")
    return len(records)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Kafka microstructure topics to ODS-style local JSON files.")
    parser.add_argument("--topics", default=",".join(DEFAULT_TOPICS), help="comma-separated topic list")
    parser.add_argument("--consumer-group", default="ods-microstructure-exporter", help="Kafka consumer group id")
    parser.add_argument("--backend", choices=["auto", "kcat", "docker-console"], default="auto")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="bootstrap server for kcat")
    parser.add_argument("--kafka-container", default="crypto-kafka", help="Kafka container name for docker-console")
    parser.add_argument(
        "--bootstrap-server-in-container",
        default="kafka:29092",
        help="bootstrap server used inside Kafka container",
    )
    parser.add_argument("--poll-timeout-ms", type=int, default=5000, help="poll timeout per topic")
    parser.add_argument("--max-messages-per-topic", type=int, default=5000, help="max messages per topic per cycle")
    parser.add_argument("--interval-seconds", type=int, default=300, help="interval seconds between export cycles")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument("--datasource-id", default="binance.ws", help="ODS datasource_id")
    parser.add_argument("--resource-prefix", default="microstructure", help="ODS resource path prefix")
    parser.add_argument(
        "--output-root",
        default="runtime/data/ods",
        help="ODS output root (relative to DataPlatform or absolute)",
    )
    parser.add_argument(
        "--time-bucket-granularity",
        choices=["day", "hour"],
        default="day",
        help="time bucket granularity",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="consume from beginning for topics when group has no committed offsets",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    topics = [x.strip() for x in args.topics.split(",") if x.strip()]
    if not topics:
        print("error: no topics specified", file=sys.stderr)
        return 1

    root_dir = Path(__file__).resolve().parents[1]
    backend = resolve_backend(args.backend)
    print(f"[info] backend={backend}, group={args.consumer_group}, topics={','.join(topics)}")

    cycle = 0
    while True:
        cycle += 1
        total = 0
        print(f"[cycle {cycle}] start {now_iso()}")
        for topic in topics:
            total += export_topic_once(args, topic, backend, root_dir)
        print(f"[cycle {cycle}] exported={total}")

        if args.once:
            break
        time.sleep(max(1, args.interval_seconds))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

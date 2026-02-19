#!/usr/bin/env python3
"""
周期性将 Kafka 微结构 topic 增量导出为 ODS 样式目录。

默认优先使用 kcat；若系统未安装 kcat，则回退到容器内 kafka-console-consumer。
支持：
1) 从 roles 配置自动提取 topic
2) 多 topic 并发导出
3) from-beginning + stop-at-log-end 批量分区读取
4) 按 exchange_ts 时间桶拆分写出（避免跨天数据写入同一分区）
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
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
    "perp.orderbook.diff",
    "perp.orderbook.snapshot",
    "perp.aggtrades",
    "spot.orderbook.diff",
    "spot.orderbook.snapshot",
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
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def pick_event_ts_ms(record: Dict[str, Any]) -> Optional[int]:
    for key in ("exchange_ts", "event_time", "ingest_ts", "time", "trade_time"):
        ts = parse_int_ts(record.get(key))
        if ts is not None:
            return ts
    return None


def iso_from_ms(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=UTC).isoformat()


def topic_domain(topic: str) -> str:
    market = "stream"
    if topic.startswith("spot."):
        market = "spot"
    elif topic.startswith("perp."):
        market = "perp"

    if "orderbook" in topic:
        return f"cex.{market}.orderbook" if market != "stream" else "cex.stream.orderbook"
    if "aggtrades" in topic:
        return f"cex.{market}.trades" if market != "stream" else "cex.stream.trades"
    return "cex.stream"


def topic_grain(topic: str) -> str:
    if "orderbook.snapshot" in topic:
        return "orderbook_snapshot"
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
        payload = line.strip()
        if not payload:
            continue
        if "@@" in payload:
            _, payload = payload.split("@@", 1)
            payload = payload.strip()
        if not payload or payload[0] not in "[{":
            continue

        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            continue

        if isinstance(obj, dict):
            records.append(obj)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    records.append(item)
    return records


def dedupe_keep_order(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def parse_topic_csv(raw: str) -> List[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def resolve_input_path(raw_path: str, root_dir: Path) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    if path.exists():
        return path.resolve()
    return (root_dir / path).resolve()


def load_topics_from_roles(roles_config_path: Path) -> List[str]:
    payload = json.loads(roles_config_path.read_text(encoding="utf-8"))
    roles = payload.get("roles")
    if not isinstance(roles, list):
        return []

    topics: List[str] = []
    for role in roles:
        if not isinstance(role, dict):
            continue

        sink = role.get("sink")
        if not isinstance(sink, dict):
            continue
        sink_with = sink.get("with")
        if not isinstance(sink_with, dict):
            continue

        topic = sink_with.get("topic")
        if isinstance(topic, str) and topic.strip():
            topics.append(topic.strip())

        topic_map = sink_with.get("topic_map")
        if isinstance(topic_map, dict):
            for mapped in topic_map.values():
                if isinstance(mapped, str) and mapped.strip():
                    topics.append(mapped.strip())

    return dedupe_keep_order(topics)


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
        "%s\\n",
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
        "--property print.key=false"
    )
    if from_beginning:
        inner += " --from-beginning"
    cmd = ["docker", "exec", kafka_container, "bash", "-lc", inner]
    stdout, stderr, _ = run_subprocess(cmd)
    return parse_message_lines(stdout + "\n" + stderr)


def get_topic_offsets_with_docker_console(
    kafka_container: str,
    bootstrap_server_in_container: str,
    topic: str,
    time_flag: int,
) -> Dict[int, int]:
    inner = (
        "kafka-run-class kafka.tools.GetOffsetShell "
        f"--broker-list {shlex.quote(bootstrap_server_in_container)} "
        f"--topic {shlex.quote(topic)} "
        f"--time {int(time_flag)}"
    )
    cmd = ["docker", "exec", kafka_container, "bash", "-lc", inner]
    stdout, stderr, rc = run_subprocess(cmd)
    if rc != 0:
        return {}

    result: Dict[int, int] = {}
    for line in (stdout + "\n" + stderr).splitlines():
        parts = line.strip().split(":")
        if len(parts) != 3:
            continue
        _, partition_raw, offset_raw = parts
        try:
            partition = int(partition_raw)
            offset = int(offset_raw)
        except ValueError:
            continue
        if partition >= 0 and offset >= 0:
            result[partition] = offset
    return result


def get_topic_offset_ranges_with_docker_console(
    kafka_container: str,
    bootstrap_server_in_container: str,
    topic: str,
) -> Dict[int, Tuple[int, int]]:
    earliest = get_topic_offsets_with_docker_console(
        kafka_container=kafka_container,
        bootstrap_server_in_container=bootstrap_server_in_container,
        topic=topic,
        time_flag=-2,
    )
    latest = get_topic_offsets_with_docker_console(
        kafka_container=kafka_container,
        bootstrap_server_in_container=bootstrap_server_in_container,
        topic=topic,
        time_flag=-1,
    )

    ranges: Dict[int, Tuple[int, int]] = {}
    for partition, end in latest.items():
        start = earliest.get(partition, end)
        if end > start >= 0:
            ranges[partition] = (start, end)
    return ranges


def consume_with_docker_console_to_log_end_snapshot(
    kafka_container: str,
    bootstrap_server_in_container: str,
    topic: str,
    offset_ranges: Dict[int, Tuple[int, int]],
    batch_size: int,
    intra_topic_concurrency: int,
    poll_timeout_ms: int,
    batch_retry: int,
) -> List[Dict[str, Any]]:
    safe_batch_size = max(1, int(batch_size))
    safe_timeout_ms = max(15000, int(poll_timeout_ms))
    jobs: List[Tuple[int, int, int]] = []  # (partition, offset, max_messages)

    for partition in sorted(offset_ranges.keys()):
        start, end_offset = offset_ranges[partition]
        start = int(start)
        end_offset = int(end_offset)
        if end_offset <= start:
            continue
        while start < end_offset:
            chunk = min(safe_batch_size, end_offset - start)
            jobs.append((partition, start, chunk))
            start += chunk

    if not jobs:
        return []

    def consume_partition_chunk(partition: int, offset: int, max_messages: int) -> List[Dict[str, Any]]:
        attempts = max(1, int(batch_retry) + 1)
        for attempt in range(1, attempts + 1):
            inner = (
                "kafka-console-consumer "
                f"--bootstrap-server {shlex.quote(bootstrap_server_in_container)} "
                f"--topic {shlex.quote(topic)} "
                f"--partition {partition} "
                f"--offset {offset} "
                f"--max-messages {max_messages} "
                f"--timeout-ms {safe_timeout_ms} "
                "--property print.key=false"
            )
            cmd = ["docker", "exec", kafka_container, "bash", "-lc", inner]
            stdout, stderr, _ = run_subprocess(cmd)
            records = parse_message_lines(stdout + "\n" + stderr)
            if records or attempt >= attempts:
                return records
            time.sleep(min(0.5 * attempt, 2.0))
        return []

    max_workers = max(1, min(int(intra_topic_concurrency), len(jobs)))
    if max_workers <= 1:
        records: List[Dict[str, Any]] = []
        for partition, offset, max_messages in jobs:
            records.extend(consume_partition_chunk(partition, offset, max_messages))
        return records

    chunk_results: List[Optional[List[Dict[str, Any]]]] = [None] * len(jobs)
    with cf.ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(consume_partition_chunk, partition, offset, max_messages): idx
            for idx, (partition, offset, max_messages) in enumerate(jobs)
        }
        for future in cf.as_completed(future_map):
            idx = future_map[future]
            chunk_results[idx] = future.result()

    records: List[Dict[str, Any]] = []
    for chunk in chunk_results:
        if chunk:
            records.extend(chunk)
    return records


def resolve_backend(requested_backend: str) -> str:
    if requested_backend in {"kcat", "docker-console"}:
        return requested_backend
    return "kcat" if shutil.which("kcat") else "docker-console"


def resolve_topic_concurrency(raw: int, topic_count: int) -> int:
    if topic_count <= 1:
        return 1
    if raw > 0:
        return max(1, min(raw, topic_count))
    return max(1, min(topic_count, 8))


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
    table_id = f"ods_{datasource_id}_{topic}".lower().replace(".", "_").replace("-", "_")
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
            "partition_keys": ["datasource_id", "resource_path", "time_bucket", "request_fingerprint"],
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


def compute_time_bucket(ts_ms: int, granularity: str) -> str:
    dt_obj = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
    if granularity == "hour":
        return dt_obj.strftime("%Y-%m-%dT%H")
    return dt_obj.strftime("%Y-%m-%d")


def split_records_by_time_bucket(records: List[Dict[str, Any]], granularity: str) -> Dict[str, List[Dict[str, Any]]]:
    now_ms = int(now_utc().timestamp() * 1000)
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for rec in records:
        ts = pick_event_ts_ms(rec)
        if ts is None:
            ts = now_ms
        bucket = compute_time_bucket(ts, granularity)
        if bucket not in buckets:
            buckets[bucket] = []
        buckets[bucket].append(rec)
    return buckets


def export_bucket(
    args: argparse.Namespace,
    topic: str,
    root_dir: Path,
    output_root: Path,
    resource_path: str,
    fingerprint: str,
    bucket: str,
    records: List[Dict[str, Any]],
) -> int:
    if not records:
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

    dataset_dir = output_root / args.datasource_id / resource_path / bucket / fingerprint
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
        time_bucket=bucket,
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
        bucket=bucket,
        metadata_rel=metadata_rel,
        response_rel=response_rel,
        coverage_start_iso=coverage_start_iso,
        coverage_end_iso=coverage_end_iso,
        record_count=len(records),
        symbol=symbol,
        token=token,
    )

    print(f"[ok] {topic} [{bucket}]: {len(records)} records -> {response_path}")
    return len(records)


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
        if args.stop_at_log_end and args.from_beginning:
            offset_ranges = get_topic_offset_ranges_with_docker_console(
                kafka_container=args.kafka_container,
                bootstrap_server_in_container=args.bootstrap_server_in_container,
                topic=topic,
            )
            records = consume_with_docker_console_to_log_end_snapshot(
                kafka_container=args.kafka_container,
                bootstrap_server_in_container=args.bootstrap_server_in_container,
                topic=topic,
                offset_ranges=offset_ranges,
                batch_size=args.topic_batch_size,
                intra_topic_concurrency=args.intra_topic_concurrency,
                poll_timeout_ms=args.poll_timeout_ms,
                batch_retry=args.batch_retry,
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

    output_root_raw = Path(args.output_root)
    if output_root_raw.is_absolute():
        output_root = output_root_raw.resolve()
    else:
        output_root = (root_dir / output_root_raw).resolve()

    resource_path = topic_resource_path(args.resource_prefix, topic)
    fingerprint = build_fingerprint(args.datasource_id, resource_path, args.consumer_group)
    buckets = split_records_by_time_bucket(records, args.time_bucket_granularity)

    exported = 0
    for bucket in sorted(buckets.keys()):
        exported += export_bucket(
            args=args,
            topic=topic,
            root_dir=root_dir,
            output_root=output_root,
            resource_path=resource_path,
            fingerprint=fingerprint,
            bucket=bucket,
            records=buckets[bucket],
        )
    return exported


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Kafka microstructure topics to ODS-style local JSON files.")
    parser.add_argument(
        "--topics",
        default="",
        help="comma-separated topic list; empty uses built-in defaults",
    )
    parser.add_argument(
        "--roles-config",
        default="",
        help="optional roles json; auto-append sink topics/topic_map topics",
    )
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
    parser.add_argument(
        "--topic-batch-size",
        type=int,
        default=50000,
        help="messages per batch when --from-beginning --stop-at-log-end",
    )
    parser.add_argument(
        "--intra-topic-concurrency",
        type=int,
        default=4,
        help="parallel batch consumers inside one topic when --from-beginning --stop-at-log-end",
    )
    parser.add_argument(
        "--batch-retry",
        type=int,
        default=2,
        help="retry times for one offset batch when batch returns empty",
    )
    parser.add_argument(
        "--topic-concurrency",
        type=int,
        default=0,
        help="parallel workers for topics per cycle (0=auto)",
    )
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
    parser.add_argument(
        "--stop-at-log-end",
        action="store_true",
        help="with --from-beginning and docker-console: snapshot topic end offsets and exit right after catching up",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root_dir = Path(__file__).resolve().parents[1]

    if args.topics.strip():
        topics = parse_topic_csv(args.topics)
    else:
        topics = list(DEFAULT_TOPICS)

    if args.roles_config.strip():
        roles_path = resolve_input_path(args.roles_config, root_dir)
        if not roles_path.exists():
            print(f"error: roles config not found: {roles_path}", file=sys.stderr)
            return 1
        try:
            topics.extend(load_topics_from_roles(roles_path))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"error: failed to load roles config {roles_path}: {exc}", file=sys.stderr)
            return 1

    topics = dedupe_keep_order(topics)
    if not topics:
        print("error: no topics specified", file=sys.stderr)
        return 1

    backend = resolve_backend(args.backend)
    if args.stop_at_log_end and backend != "docker-console":
        print("[warn] --stop-at-log-end is only supported on docker-console backend; fallback to normal consume")

    topic_concurrency = resolve_topic_concurrency(args.topic_concurrency, len(topics))
    print(
        f"[info] backend={backend}, group={args.consumer_group}, "
        f"topic_concurrency={topic_concurrency}, topics={','.join(topics)}"
    )

    cycle = 0
    while True:
        cycle += 1
        total = 0
        failed = 0
        print(f"[cycle {cycle}] start {now_iso()}")

        if topic_concurrency <= 1 or len(topics) <= 1:
            for topic in topics:
                try:
                    total += export_topic_once(args, topic, backend, root_dir)
                except Exception as exc:
                    failed += 1
                    print(f"[error] {topic}: {exc}", file=sys.stderr)
        else:
            with cf.ThreadPoolExecutor(max_workers=topic_concurrency) as pool:
                future_map = {
                    pool.submit(export_topic_once, args, topic, backend, root_dir): topic
                    for topic in topics
                }
                for future in cf.as_completed(future_map):
                    topic = future_map[future]
                    try:
                        total += future.result()
                    except Exception as exc:
                        failed += 1
                        print(f"[error] {topic}: {exc}", file=sys.stderr)

        print(f"[cycle {cycle}] exported={total}, failed={failed}")
        if failed > 0:
            return 1

        if args.once:
            break
        time.sleep(max(1, args.interval_seconds))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

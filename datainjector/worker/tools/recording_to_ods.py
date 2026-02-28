#!/usr/bin/env python3
"""
Convert worker websocket recordings (jsonl) into ODS-compatible layout.

Input (example):
  runtime/data/recording_run3/20260211_101704/aggtrade/aaveusdt/aggtrade_000.jsonl
  runtime/data/recording_run3/20260211_101704/orderbook/aaveusdt/orderbook_000.jsonl

Output (example):
  data/ods/binance.usdm.ws/ws/aggtrade/aaveusdt/{time_bucket}/{fingerprint}/
    - response_0000.json
    - metadata.json
    - manifest.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


UTC = dt.timezone.utc


def now_iso() -> str:
    return dt.datetime.now(tz=UTC).isoformat()


def utc_from_ms(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=UTC)


def bucket_from_ms(start_ms: int, end_ms: int) -> str:
    start = utc_from_ms(start_ms).strftime("%Y-%m-%dT%H-%MZ")
    end = utc_from_ms(end_ms).strftime("%Y-%m-%dT%H-%MZ")
    return f"{start}__{end}"


def normalize_path(p: Path) -> str:
    return str(p).replace("\\", "/")


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{lineno} invalid json: {exc}") from exc
            if not isinstance(obj, dict):
                raise ValueError(f"{path}:{lineno} expects json object")
            yield obj


def sanitize_table_id(parts: List[str]) -> str:
    text = "_".join(parts).lower().replace(".", "_").replace("-", "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text


def domain_for_stream(stream: str) -> str:
    s = stream.lower()
    if s in {"aggtrade", "agg_trade", "trade", "trades"}:
        return "cex.perp.trades"
    if s in {"orderbook", "depth"}:
        return "cex.perp.orderbook"
    return "cex.perp.stream"


def split_chunks(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def pick_event_ts(record: Dict[str, Any]) -> Optional[int]:
    keys = ("exchange_ts", "event_time", "trade_time", "ingest_ts")
    for key in keys:
        value = record.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def build_fingerprint(payload: Dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def load_registry_paths(registry_path: Path) -> set[str]:
    if not registry_path.exists():
        return set()
    out: set[str] = set()
    with registry_path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                meta_path = obj.get("metadata_path")
                if isinstance(meta_path, str) and meta_path:
                    out.add(meta_path)
    return out


def append_registry_record(registry_path: Path, record: Dict[str, Any]) -> None:
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    with registry_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")


def detect_stream_symbol(path: Path, input_dir: Path) -> Tuple[str, str, List[str]]:
    rel = path.relative_to(input_dir)
    parts = rel.parts
    if len(parts) < 3:
        raise ValueError(f"path structure too short: {rel}")
    stream = parts[-3]
    symbol = parts[-2]
    prefix_parts = list(parts[:-3])
    return stream, symbol, prefix_parts


def infer_context(prefix_parts: List[str], default_exchange: str, default_market: str) -> Tuple[str, str, str]:
    exchange = default_exchange
    market = default_market
    session = ""
    if len(prefix_parts) >= 2:
        exchange = prefix_parts[0]
        market = prefix_parts[1]
        if len(prefix_parts) > 2:
            session = "/".join(prefix_parts[2:])
    elif len(prefix_parts) == 1:
        session = prefix_parts[0]
    return exchange, market, session


def infer_datasource_id(base_datasource_id: str, exchange: str, market: str, auto_datasource: bool) -> str:
    if not auto_datasource:
        return base_datasource_id

    ex = exchange.lower()
    mk = market.lower()
    if ex == "binance":
        if mk == "spot":
            return "binance.spot.ws"
        if mk in {"futures", "usdm", "perp"}:
            return "binance.usdm.ws"
    return base_datasource_id


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert worker recording jsonl into ODS layout")
    parser.add_argument("--input-dir", required=True, help="recording_run directory")
    parser.add_argument(
        "--output-root",
        default="crypto_research_lab/data/ods",
        help="ODS root directory",
    )
    parser.add_argument("--datasource-id", default="binance.usdm.ws", help="target datasource_id")
    parser.add_argument(
        "--auto-datasource",
        action="store_true",
        default=True,
        help="infer datasource_id by path (binance spot/futures)",
    )
    parser.add_argument(
        "--no-auto-datasource",
        dest="auto_datasource",
        action="store_false",
        help="disable datasource_id inference by path",
    )
    parser.add_argument("--exchange", default="binance", help="exchange name")
    parser.add_argument("--market", default="futures", help="market name")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50000,
        help="records per response file",
    )
    parser.add_argument(
        "--registry-path",
        default="crypto_research_lab/data/ods/_catalog/ods_dataset_registry.jsonl",
        help="ODS dataset registry file",
    )
    parser.add_argument(
        "--metadata-prefix",
        default="data/ods",
        help="prefix used in registry metadata_path",
    )
    parser.add_argument("--overwrite", action="store_true", help="overwrite existing output folders")
    parser.add_argument("--dry-run", action="store_true", help="print plan without writing files")
    args = parser.parse_args()

    input_dir = Path(args.input_dir).resolve()
    output_root = Path(args.output_root).resolve()
    registry_path = Path(args.registry_path).resolve()
    metadata_prefix = Path(args.metadata_prefix)

    if not input_dir.exists():
        raise SystemExit(f"input dir not found: {input_dir}")
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be > 0")

    files = sorted(input_dir.rglob("*.jsonl"))
    if not files:
        raise SystemExit(f"no jsonl found under: {input_dir}")

    existing_registry = load_registry_paths(registry_path)
    converted = 0

    for src_file in files:
        stream, symbol, prefix_parts = detect_stream_symbol(src_file, input_dir)
        exchange, market, session = infer_context(prefix_parts, args.exchange, args.market)
        datasource_id = infer_datasource_id(args.datasource_id, exchange, market, args.auto_datasource)
        records = list(iter_jsonl(src_file))
        if not records:
            continue

        event_ts = [ts for ts in (pick_event_ts(r) for r in records) if ts is not None]
        if event_ts:
            start_ms = min(event_ts)
            end_ms = max(event_ts)
        else:
            fallback_ms = int(src_file.stat().st_mtime * 1000)
            start_ms = fallback_ms
            end_ms = fallback_ms

        time_bucket = bucket_from_ms(start_ms, end_ms)
        resource_path = f"ws/{exchange.lower()}/{market.lower()}/{stream.lower()}/{symbol.lower()}"
        rel_source = normalize_path(src_file.relative_to(input_dir))
        fp_payload = {
            "source": "worker.websocket.recording",
            "input_dir": normalize_path(input_dir),
            "source_file": rel_source,
            "datasource_id": datasource_id,
            "resource_path": resource_path,
            "time_bucket": time_bucket,
            "exchange": exchange,
            "market": market,
            "session": session,
        }
        request_fingerprint = build_fingerprint(fp_payload)
        out_dir = output_root / datasource_id / resource_path / time_bucket / request_fingerprint

        if out_dir.exists() and not args.overwrite:
            print(f"[skip] exists: {out_dir}")
            continue

        if args.dry_run:
            print(f"[plan] {src_file} -> {out_dir}")
            converted += 1
            continue

        out_dir.mkdir(parents=True, exist_ok=True)

        response_files: List[str] = []
        for idx, chunk in enumerate(split_chunks(records, args.chunk_size)):
            file_name = f"response_{idx:04d}.json"
            write_json(out_dir / file_name, chunk)
            response_files.append(file_name)

        fields = sorted({k for record in records for k in record.keys()})
        domain = domain_for_stream(stream)
        table_id = sanitize_table_id(["ods", datasource_id, stream])
        table = {
            "table_id": table_id,
            "partition_keys": ["datasource_id", "time_bucket"],
            "primary_keys": [],
        }
        metadata = {
            "schema_version": "v1",
            "dataset": {
                "domain": domain,
                "datasource_id": datasource_id,
                "stream_name": stream.lower(),
                "exchange": exchange,
                "market": market,
                "symbol": symbol.upper(),
                "session_id": session,
                "origin": "worker.websocket.recording",
            },
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
                "granularity": "stream",
                "timezone": "UTC",
                "coverage": {
                    "start": utc_from_ms(start_ms).isoformat(),
                    "end": utc_from_ms(end_ms).isoformat(),
                },
            },
            "schema": {
                "response": {
                    "type": "array",
                    "fields": fields,
                    "format": "json",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
                "fields": [{"name": key} for key in fields],
            },
            "table": table,
            "paimon": table,
        }
        write_json(out_dir / "metadata.json", metadata)

        manifest = {
            "created_at": now_iso(),
            "converter": "recording_to_ods.py",
            "source_file": rel_source,
            "source_input_dir": normalize_path(input_dir),
            "response_files": response_files,
            "record_count": len(records),
            "time_bucket": time_bucket,
            "request_fingerprint": request_fingerprint,
        }
        write_json(out_dir / "manifest.json", manifest)

        metadata_path = metadata_prefix / out_dir.relative_to(output_root) / "metadata.json"
        metadata_path_str = normalize_path(metadata_path)
        if metadata_path_str not in existing_registry:
            registry_record = {
                "dataset": metadata["dataset"],
                "storage": metadata["storage"],
                "time": metadata["time"],
                "table": metadata["table"],
                "metadata_path": metadata_path_str,
                "created_at": now_iso(),
            }
            append_registry_record(registry_path, registry_record)
            existing_registry.add(metadata_path_str)

        converted += 1
        print(f"[ok] {src_file} -> {out_dir}")

    print(f"converted datasets: {converted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

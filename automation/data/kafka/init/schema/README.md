# Kafka Topic Initialization

This folder contains a manual topic inventory and a one-off initialization script.
It is **not** wired into any bootstrap flow by default; run it only when you need to
ensure required topics exist before starting jobs.

## Files
- `topics.list`: Canonical list of Kafka topics (grouped by category).
- `schema.sh`: Creates all topics from the list (idempotent with `--if-not-exists`).

## Usage
```bash
./automation/data/kafka/init/schema.sh
```

Optional environment overrides:
```bash
TOPIC_PARTITIONS=3 TOPIC_REPLICATION_FACTOR=1 ./automation/data/kafka/init/schema.sh
TOPIC_FILE=./automation/data/kafka/init/schema/topics.list DRY_RUN=true ./automation/data/kafka/init/schema.sh
```

## Topic Grouping (for Kafka UI filters)
Use prefixes to keep the UI manageable:
- `ods_`, `dim_`, `dwd_`: realtime pipeline layers
- `chain.*`: chain-level streams
- `perp.*`: normalized perp market streams
- `binance.*`: exchange raw streams
- `metadata.*`, `quality.*`: system metadata and alerts
- `tasks.*`, `batch.*`, `http.*`, `data.*`: control-plane and scheduling

Adjust naming as new domains are introduced; keep `topics.list` as the source of truth.

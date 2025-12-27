#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TOPIC_FILE="${TOPIC_FILE:-$SCRIPT_DIR/topics.list}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS_LOCAL:-localhost:9092}"
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:-3}"
TOPIC_REPLICATION_FACTOR="${TOPIC_REPLICATION_FACTOR:-1}"
DRY_RUN="${DRY_RUN:-false}"

if [[ ! -f "$TOPIC_FILE" ]]; then
  echo "Topic list not found: $TOPIC_FILE" >&2
  exit 1
fi

echo "=========================================="
echo "Kafka bootstrap: $KAFKA_BOOTSTRAP"
echo "Topic list:      $TOPIC_FILE"
echo "Partitions:      $TOPIC_PARTITIONS"
echo "Replication:     $TOPIC_REPLICATION_FACTOR"
echo "Dry run:         $DRY_RUN"
echo "=========================================="
echo ""

if ! nc -z "${KAFKA_BOOTSTRAP%%:*}" "${KAFKA_BOOTSTRAP##*:}" 2>/dev/null; then
  echo "Kafka is not reachable at ${KAFKA_BOOTSTRAP}" >&2
  exit 1
fi

create_topic() {
  local topic="$1"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[dry-run] create topic: $topic"
    return 0
  fi

  docker exec "${KAFKA_CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${KAFKA_BOOTSTRAP}" \
    --create \
    --topic "${topic}" \
    --partitions "${TOPIC_PARTITIONS}" \
    --replication-factor "${TOPIC_REPLICATION_FACTOR}" \
    --if-not-exists >/dev/null
  echo "created/exists: $topic"
}

while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
  line="${raw_line%%#*}"
  line="$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  if [[ -z "$line" ]]; then
    continue
  fi
  if [[ "${line:0:1}" == "#" ]]; then
    continue
  fi
  create_topic "$line"
done <"$TOPIC_FILE"

echo ""
echo "Done."

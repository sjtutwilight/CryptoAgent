#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
INFRA_ENV_FILE="${INFRA_ENV_FILE:-$ROOT_DIR/config/infrastructure/env/docker.env}"

KAFKA_CONTAINER_NAME="${KAFKA_CONTAINER_NAME:-crypto-kafka}"
KAFKA_BOOTSTRAP_SERVERS_LOCAL="${KAFKA_BOOTSTRAP_SERVERS_LOCAL:-localhost:9092}"

load_infra_env() {
  if [[ -f "$INFRA_ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$INFRA_ENV_FILE"
    set +a
  fi
}

run_kafka_topics_init() {
  local topic_file="$ROOT_DIR/automation/data/kafka/init/schema/topics.list"
  local dry_run="false"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --topic-file)
        topic_file="${2:-}"
        shift 2
        ;;
      --dry-run)
        dry_run="true"
        shift
        ;;
      *)
        echo "unknown option: $1" >&2
        exit 1
        ;;
    esac
  done

  if [[ ! -f "$topic_file" ]]; then
    echo "topic list not found: $topic_file" >&2
    exit 1
  fi

  local kafka_bootstrap="${KAFKA_BOOTSTRAP_SERVERS_LOCAL:-localhost:9092}"
  local topic_partitions="${TOPIC_PARTITIONS:-3}"
  local topic_replication="${TOPIC_REPLICATION_FACTOR:-1}"

  echo "=========================================="
  echo "Kafka bootstrap: $kafka_bootstrap"
  echo "Topic list:      $topic_file"
  echo "Partitions:      $topic_partitions"
  echo "Replication:     $topic_replication"
  echo "Dry run:         $dry_run"
  echo "=========================================="
  echo ""

  if ! nc -z "${kafka_bootstrap%%:*}" "${kafka_bootstrap##*:}" 2>/dev/null; then
    echo "Kafka is not reachable at ${kafka_bootstrap}" >&2
    exit 1
  fi

  create_topic() {
    local topic="$1"
    if [[ "$dry_run" == "true" ]]; then
      echo "[dry-run] create topic: $topic"
      return 0
    fi

    docker exec "${KAFKA_CONTAINER_NAME}" kafka-topics \
      --bootstrap-server "${kafka_bootstrap}" \
      --create \
      --topic "${topic}" \
      --partitions "${topic_partitions}" \
      --replication-factor "${topic_replication}" \
      --if-not-exists >/dev/null
    echo "created/exists: $topic"
  }

  while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
    local line
    line="${raw_line%%#*}"
    line="$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    if [[ -z "$line" ]]; then
      continue
    fi
    if [[ "${line:0:1}" == "#" ]]; then
      continue
    fi
    create_topic "$line"
  done <"$topic_file"

  echo ""
  echo "Done."
}

main() {
  load_infra_env
  run_kafka_topics_init "$@"
}

main "$@"

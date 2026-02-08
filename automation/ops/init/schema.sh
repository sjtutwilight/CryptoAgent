#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
INFRA_ENV_FILE="${INFRA_ENV_FILE:-$ROOT_DIR/config/infrastructure/env/docker.env}"

load_infra_env() {
  if [[ -f "$INFRA_ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$INFRA_ENV_FILE"
    set +a
  fi
}

run_schema_init_all() {
  local skip_postgres="false"
  local skip_clickhouse="false"
  local skip_kafka="false"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --skip-postgres)
        skip_postgres="true"
        shift
        ;;
      --skip-clickhouse)
        skip_clickhouse="true"
        shift
        ;;
      --skip-kafka)
        skip_kafka="true"
        shift
        ;;
      *)
        echo "unknown option: $1" >&2
        exit 1
        ;;
    esac
  done

  if [[ "$skip_postgres" != "true" ]]; then
    "$ROOT_DIR/automation/data/postgres/init/schema.sh"
  fi
  if [[ "$skip_clickhouse" != "true" ]]; then
    "$ROOT_DIR/automation/data/clickhouse/init/schema.sh"
  fi
  if [[ "$skip_kafka" != "true" ]]; then
    "$ROOT_DIR/automation/data/kafka/init/schema.sh"
  fi
}

main() {
  load_infra_env
  run_schema_init_all "$@"
}

main "$@"

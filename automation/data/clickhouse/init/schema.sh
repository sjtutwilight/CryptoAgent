#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
INFRA_ENV_FILE="${INFRA_ENV_FILE:-$ROOT_DIR/config/infrastructure/env/docker.env}"

CLICKHOUSE_CONTAINER_NAME="${CLICKHOUSE_CONTAINER_NAME:-clickhouse}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-default}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"

load_infra_env() {
  if [[ -f "$INFRA_ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$INFRA_ENV_FILE"
    set +a
  fi
}

run_clickhouse_schema_init() {
  local ddl_dir="$ROOT_DIR/automation/data/clickhouse/init/schema/ddl"
  local view_dir="$ROOT_DIR/automation/data/clickhouse/init/schema/view"
  local ddl_files=()
  local view_files=()

  shopt -s nullglob
  ddl_files=("$ddl_dir"/*.sql)
  view_files=("$view_dir"/*.sql)
  shopt -u nullglob

  if [[ ${#ddl_files[@]} -eq 0 && ${#view_files[@]} -eq 0 ]]; then
    echo "no clickhouse sql files found in $ddl_dir or $view_dir" >&2
    exit 1
  fi

  local docker_args=(docker exec -i "$CLICKHOUSE_CONTAINER_NAME" clickhouse-client --database "$CLICKHOUSE_DB" --multiquery)
  if [[ -n "$CLICKHOUSE_USER" ]]; then
    docker_args+=(--user "$CLICKHOUSE_USER")
  fi
  if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
    docker_args+=(--password "$CLICKHOUSE_PASSWORD")
  fi

  for file in "${ddl_files[@]}"; do
    echo "[clickhouse] apply: $file"
    cat "$file" | "${docker_args[@]}"
  done

  for file in "${view_files[@]}"; do
    echo "[clickhouse] apply: $file"
    cat "$file" | "${docker_args[@]}"
  done
}

main() {
  load_infra_env
  run_clickhouse_schema_init "$@"
}

main "$@"

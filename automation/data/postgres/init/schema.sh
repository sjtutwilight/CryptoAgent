#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
INFRA_ENV_FILE="${INFRA_ENV_FILE:-$ROOT_DIR/config/infrastructure/env/docker.env}"

POSTGRES_CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-crypto-postgres}"
POSTGRES_USER="${POSTGRES_USER:-twilight}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-twilight123}"
POSTGRES_DB="${POSTGRES_DB:-twilight}"

load_infra_env() {
  if [[ -f "$INFRA_ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$INFRA_ENV_FILE"
    set +a
  fi
}

run_postgres_schema_init() {
  local ddl_dir="$ROOT_DIR/automation/data/postgres/init/schema/ddl"
  local ddl_files=()

  if [[ ! -d "$ddl_dir" ]]; then
    echo "postgres ddl dir not found: $ddl_dir" >&2
    exit 1
  fi

  shopt -s nullglob
  ddl_files=("$ddl_dir"/*.sql)
  shopt -u nullglob

  if [[ ${#ddl_files[@]} -eq 0 ]]; then
    echo "no postgres ddl files found in $ddl_dir" >&2
    exit 1
  fi

  local docker_args=(docker exec -i -e "PGPASSWORD=$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB")
  for file in "${ddl_files[@]}"; do
    echo "[postgres] apply: $file"
    cat "$file" | "${docker_args[@]}"
  done
}

main() {
  load_infra_env
  run_postgres_schema_init "$@"
}

main "$@"

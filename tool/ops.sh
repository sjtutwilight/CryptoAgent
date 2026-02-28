#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS_DIR="$ROOT_DIR/automation/ops"

usage() {
  cat <<'USAGE'
Usage: ./tool/ops.sh <domain:action> [args...]

Commands:
  role:start        Apply roles by role_id list
  role:stop         Stop roles by role_id list or "all"
  role:alive_list   List roles
  role:task         Send task for role_id (Kafka batch.tasks)

  init:schema       Init Postgres/ClickHouse/Kafka schema
  init:data         Init data assets (not yet configured)
  init:all          Init schema + data

  http:get          HTTP GET
  http:post         HTTP POST

  flink:build       Build aggregator JAR
  flink:upload      Upload JAR to Flink
  flink:list        List Flink JARs
  flink:run         Run Flink JAR
  flink:job         Upload and run by keyword
  flink:status      Job status
  flink:cancel      Cancel job

  sqlite:query      Metadata query helper
  sqlite:clean      Metadata cleanup helper

  starrocks:query   Query StarRocks via container
  codex:otel       Codex OTel 分析/审核/回归工具
USAGE
}

mcp_meta() {
  cat <<'JSON'
{"tool_name":"ops_execute","description":"DataPlatform Ops Entrypoint (tool/ops.sh): roles/init/http/flink/sqlite/starrocks","supports_output_json":true}
JSON
}

main() {
  if [[ "${1:-}" == "--mcp" ]]; then
    mcp_meta
    exit 0
  fi

  local cmd="${1:-}"
  if [[ -z "$cmd" ]]; then
    usage
    exit 1
  fi
  shift || true

  case "$cmd" in
    role:start)
      python3 "$OPS_DIR/role/start.py" "$@"
      ;;
    role:stop)
      python3 "$OPS_DIR/role/stop.py" "$@"
      ;;
    role:alive_list)
      python3 "$OPS_DIR/role/alive_list.py" "$@"
      ;;
    role:task)
      python3 "$OPS_DIR/role/task.py" "$@"
      ;;
    init:schema)
      "$OPS_DIR/init/schema.sh" "$@"
      ;;
    init:data)
      "$OPS_DIR/init/data.sh" "$@"
      ;;
    init:all)
      "$OPS_DIR/init/all.sh" "$@"
      ;;
    http:get)
      python3 "$OPS_DIR/http/get.py" "$@"
      ;;
    http:post)
      python3 "$OPS_DIR/http/post.py" "$@"
      ;;
    flink:build)
      python3 "$OPS_DIR/flink/build.py" "$@"
      ;;
    flink:upload)
      python3 "$OPS_DIR/flink/upload.py" "$@"
      ;;
    flink:list)
      python3 "$OPS_DIR/flink/list.py" "$@"
      ;;
    flink:run)
      python3 "$OPS_DIR/flink/run.py" "$@"
      ;;
    flink:job)
      python3 "$OPS_DIR/flink/job.py" "$@"
      ;;
    flink:status)
      python3 "$OPS_DIR/flink/status.py" "$@"
      ;;
    flink:cancel)
      python3 "$OPS_DIR/flink/cancel.py" "$@"
      ;;
    sqlite:query)
      "$OPS_DIR/sqlite/query.sh" "$@"
      ;;
    sqlite:clean)
      "$OPS_DIR/sqlite/clean.sh" "$@"
      ;;
    starrocks:query)
      "$OPS_DIR/starrocks/query.sh" "$@"
      ;;
    codex:otel)
      python3 "$OPS_DIR/codex/otel_analytics.py" "$@"
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      echo "unknown command: $cmd" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"

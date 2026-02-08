#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
METADATA_MANAGER="$ROOT_DIR/automation/ops/sqlite/query.py"
METADATA_DB="$ROOT_DIR/runtime/data/.metadata/registry.db"

usage() {
  cat <<USAGE
Usage: ./tool/ops.sh sqlite:query <command> [args]

Commands:
  list-sources
  query [--datasource NAME] [--category NAME] [--tags TAG1,TAG2]
  show <dataset_id>
  stats
  stale [--days N]
  gui
  sql
  view <table> [--limit N]
USAGE
}

run_metadata_list_sources() {
  python3 "$METADATA_MANAGER" list-sources
}

run_metadata_query() {
  python3 "$METADATA_MANAGER" query "$@"
}

run_metadata_show() {
  if [[ $# -eq 0 ]]; then
    echo "error: dataset_id required" >&2
    echo "usage: ./tool/ops.sh sqlite:query show <dataset_id>" >&2
    exit 1
  fi
  python3 "$METADATA_MANAGER" show --dataset-id "$1"
}

run_metadata_stats() {
  python3 "$METADATA_MANAGER" stats
}

run_metadata_stale() {
  local days=30
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --days)
        days="$2"
        shift 2
        ;;
      *)
        echo "unknown option: $1" >&2
        exit 1
        ;;
    esac
  done
  python3 "$METADATA_MANAGER" stale --days "$days"
}

run_metadata_gui() {
  if [[ ! -f "$METADATA_DB" ]]; then
    echo "error: database not found: $METADATA_DB" >&2
    echo "tip: run a role with metadata enabled first" >&2
    exit 1
  fi

  echo "starting SQLite GUI..."

  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    if command -v sqlitebrowser >/dev/null 2>&1; then
      echo "launching DB Browser for SQLite..."
      sqlitebrowser "$METADATA_DB" &
    else
      echo "error: sqlitebrowser not found" >&2
      echo "install: brew install --cask db-browser-for-sqlite" >&2
      echo "falling back to SQL mode..." >&2
      run_metadata_sql
    fi
  elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    if command -v sqlitebrowser >/dev/null 2>&1; then
      echo "launching DB Browser for SQLite..."
      sqlitebrowser "$METADATA_DB" &
    else
      echo "error: sqlitebrowser not found" >&2
      echo "install: sudo apt install sqlitebrowser" >&2
      echo "falling back to SQL mode..." >&2
      run_metadata_sql
    fi
  else
    echo "error: unsupported OS: $OSTYPE" >&2
    exit 1
  fi
}

run_metadata_sql() {
  if [[ ! -f "$METADATA_DB" ]]; then
    echo "error: database not found: $METADATA_DB" >&2
    exit 1
  fi

  if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "error: sqlite3 not found" >&2
    echo "install:" >&2
    echo "  macOS: brew install sqlite" >&2
    echo "  Linux: sudo apt install sqlite3" >&2
    exit 1
  fi

  echo "SQLite Interactive Shell"
  echo "========================"
  echo "Common commands:"
  echo "  .tables              - list tables"
  echo "  .schema <table>      - show table structure"
  echo "  .mode column         - column display mode"
  echo "  .headers on          - show column names"
  echo "  .quit                - exit"
  echo ""
  echo "Example queries:"
  echo "  SELECT * FROM datasources;"
  echo "  SELECT id, category, created_at FROM datasets;"
  echo "  SELECT COUNT(*) FROM files;"
  echo ""

  sqlite3 "$METADATA_DB"
}

run_metadata_view() {
  if [[ $# -eq 0 ]]; then
    echo "error: table name required" >&2
    echo "usage: ./tool/ops.sh sqlite:query view <table_name> [--limit N]" >&2
    echo "available tables: datasources, datasets, files" >&2
    exit 1
  fi

  local table="$1"
  shift

  local limit=50
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --limit)
        limit="$2"
        shift 2
        ;;
      *)
        echo "unknown option: $1" >&2
        exit 1
        ;;
    esac
  done

  python3 "$METADATA_MANAGER" view "$table" --limit "$limit"
}

main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    list-sources)
      run_metadata_list_sources "$@"
      ;;
    query)
      run_metadata_query "$@"
      ;;
    show)
      run_metadata_show "$@"
      ;;
    stats)
      run_metadata_stats "$@"
      ;;
    stale)
      run_metadata_stale "$@"
      ;;
    gui)
      run_metadata_gui "$@"
      ;;
    sql)
      run_metadata_sql "$@"
      ;;
    view)
      run_metadata_view "$@"
      ;;
    -h|--help|help|"")
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

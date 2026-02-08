#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
METADATA_MANAGER="$ROOT_DIR/automation/ops/sqlite/query.py"

run_metadata_cleanup() {
  if [[ $# -eq 0 ]]; then
    echo "error: dataset_id required" >&2
    echo "usage: ./tool/ops.sh sqlite:clean <dataset_id> [--delete-files] [--confirm]" >&2
    exit 1
  fi
  python3 "$METADATA_MANAGER" cleanup "$@"
}

run_metadata_cleanup "$@"

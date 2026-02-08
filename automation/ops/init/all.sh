#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

skip_schema="false"
skip_data="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-schema)
      skip_schema="true"
      shift
      ;;
    --skip-data)
      skip_data="true"
      shift
      ;;
    *)
      echo "unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "$skip_schema" != "true" ]]; then
  "$ROOT_DIR/automation/ops/init/schema.sh"
fi
if [[ "$skip_data" != "true" ]]; then
  "$ROOT_DIR/automation/ops/init/data.sh"
fi

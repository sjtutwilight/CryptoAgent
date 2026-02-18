#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER_NAME="${CONTAINER_NAME:-datainjector-worker}"
ROLE_CONFIG_PATH="${ROLE_CONFIG_PATH:-$ROOT_DIR/datainjector/worker/configs/aave/roles_aave_full_stable.json}"
CHECK_INTERVAL="${CHECK_INTERVAL:-30}"         # seconds
STALE_SECONDS="${STALE_SECONDS:-300}"          # no-growth warning threshold
AUTO_RESTART_ON_STALE="${AUTO_RESTART_ON_STALE:-false}"

# Host-side recording paths (mounted from container /app/runtime/data)
DATA_FILES=(
  "$ROOT_DIR/runtime/data/recording/binance/spot/aggtrade/aaveusdt/aggtrade_000.jsonl"
  "$ROOT_DIR/runtime/data/recording/binance/spot/orderbook/aaveusdt/orderbook_000.jsonl"
  "$ROOT_DIR/runtime/data/recording/binance/futures/aggtrade/aaveusdt/aggtrade_000.jsonl"
  "$ROOT_DIR/runtime/data/recording/binance/futures/orderbook/aaveusdt/orderbook_000.jsonl"
)

ROLE_IDS=(
  "rec-binance-perp-aave-orderbook-full"
  "rec-binance-perp-aave-aggtrade-full"
  "rec-binance-spot-aave-orderbook-full"
  "rec-binance-spot-aave-aggtrade-full"
)

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  log "FATAL: $*"
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

container_is_running() {
  local running
  running="$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null || true)"
  [[ "$running" == "true" ]]
}

start_worker() {
  log "worker not running, starting via ./tool/orchestration.sh w"
  (cd "$ROOT_DIR" && ./tool/orchestration.sh w >/dev/null)
}

apply_roles() {
  [[ -f "$ROLE_CONFIG_PATH" ]] || die "role config not found: $ROLE_CONFIG_PATH"

  log "applying AAVE roles from $ROLE_CONFIG_PATH"
  cat "$ROLE_CONFIG_PATH" | docker exec -i "$CONTAINER_NAME" sh -lc \
    "curl -sS -X POST http://localhost:8090/api/roles/apply -H 'Content-Type: application/json' --data-binary @-" >/dev/null
}

get_roles_json() {
  docker exec "$CONTAINER_NAME" sh -lc "curl -sS http://localhost:8090/api/roles" 2>/dev/null || true
}

roles_ready() {
  local roles_json="$1"
  local id
  for id in "${ROLE_IDS[@]}"; do
    if [[ "$roles_json" != *"$id"* ]]; then
      return 1
    fi
  done
  return 0
}

print_recent_errors() {
  local logs
  logs="$(docker logs --since "${CHECK_INTERVAL}s" "$CONTAINER_NAME" 2>&1 || true)"
  # Focus on high-signal runtime failures; avoid flooding on generic INFO lines.
  local hits
  hits="$(printf '%s\n' "$logs" | grep -Ei 'panic|fatal|segmentation fault|concurrent write to websocket connection|role\.stop|ws\.read\.error|websocket .*failed' || true)"
  if [[ -n "$hits" ]]; then
    log "recent error signals:"
    printf '%s\n' "$hits" | tail -n 20 | sed 's/^/  /'
  fi
}

file_size_or_zero() {
  local f="$1"
  if [[ -f "$f" ]]; then
    stat -f %z "$f"
  else
    echo 0
  fi
}

declare -A PREV_SIZE
last_growth_ts="$(date +%s)"

init_file_state() {
  local f
  for f in "${DATA_FILES[@]}"; do
    PREV_SIZE["$f"]="$(file_size_or_zero "$f")"
  done
}

check_growth() {
  local any_growth=false
  local total_delta=0
  local f old now delta

  for f in "${DATA_FILES[@]}"; do
    old="${PREV_SIZE[$f]:-0}"
    now="$(file_size_or_zero "$f")"

    if (( now > old )); then
      delta=$((now - old))
      any_growth=true
      total_delta=$((total_delta + delta))
      log "growth +${delta}B :: ${f#$ROOT_DIR/}"
    elif (( now == 0 )); then
      log "waiting file    :: ${f#$ROOT_DIR/}"
    else
      log "no growth       :: ${f#$ROOT_DIR/}"
    fi

    PREV_SIZE["$f"]="$now"
  done

  if [[ "$any_growth" == "true" ]]; then
    last_growth_ts="$(date +%s)"
    log "total growth in this interval: ${total_delta}B"
  else
    local now_ts idle
    now_ts="$(date +%s)"
    idle=$((now_ts - last_growth_ts))
    if (( idle >= STALE_SECONDS )); then
      log "WARNING: no file growth for ${idle}s (threshold=${STALE_SECONDS}s)"
      if [[ "$AUTO_RESTART_ON_STALE" == "true" ]]; then
        log "AUTO_RESTART_ON_STALE=true -> restarting worker and re-applying roles"
        docker restart "$CONTAINER_NAME" >/dev/null || true
        sleep 3
        apply_roles || true
        last_growth_ts="$(date +%s)"
      fi
    fi
  fi
}

ensure_healthy_runtime() {
  if ! container_is_running; then
    start_worker
    sleep 4
  fi

  if ! container_is_running; then
    die "worker container still not running after start attempt"
  fi

  local roles_json
  roles_json="$(get_roles_json)"

  # roles endpoint not ready yet
  if [[ -z "$roles_json" || "$roles_json" != *"roles"* ]]; then
    log "roles API not ready, re-check next loop"
    return
  fi

  if ! roles_ready "$roles_json"; then
    log "AAVE roles missing/incomplete, re-applying"
    apply_roles
  fi
}

usage() {
  cat <<USAGE
Usage: ./tool/aave_watchdog.sh

Environment variables:
  CONTAINER_NAME          worker container name (default: datainjector-worker)
  ROLE_CONFIG_PATH        role config JSON path
                          (default: datainjector/worker/configs/aave/roles_aave_full_stable.json)
  CHECK_INTERVAL          poll interval seconds (default: 30)
  STALE_SECONDS           no-growth warning threshold in seconds (default: 300)
  AUTO_RESTART_ON_STALE   true/false, auto restart on stale growth (default: false)

What it does:
  1) Ensures worker container is running (auto start)
  2) Ensures AAVE roles are applied (auto apply)
  3) Monitors recording file growth
  4) Prints recent high-signal error logs
  5) Auto restarts worker only when container is down
USAGE
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_cmd docker
  require_cmd stat
  require_cmd grep

  log "starting AAVE watchdog"
  log "container=$CONTAINER_NAME interval=${CHECK_INTERVAL}s stale=${STALE_SECONDS}s auto_restart_on_stale=$AUTO_RESTART_ON_STALE"
  log "role_config=${ROLE_CONFIG_PATH#$ROOT_DIR/}"

  init_file_state

  while true; do
    ensure_healthy_runtime
    check_growth
    print_recent_errors
    sleep "$CHECK_INTERVAL"
  done
}

main "$@"

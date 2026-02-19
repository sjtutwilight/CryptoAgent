#!/opt/homebrew/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${STATE_DIR:-$ROOT_DIR/runtime/data}"
PUSH_STATE_DIR="${PUSH_STATE_DIR:-$STATE_DIR/monitor_event_push}"
CODEX_PUSH_OUTPUT_DIR="${CODEX_PUSH_OUTPUT_DIR:-$PUSH_STATE_DIR/codex_output}"
CODEX_DAILY_STATE_FILE="${CODEX_DAILY_STATE_FILE:-$PUSH_STATE_DIR/codex_daily.state}"
AUDIT_LOG_FILE="${AUDIT_LOG_FILE:-$PUSH_STATE_DIR/audit_$(date +%Y%m%d).log}"

ENABLE_CODEX_PUSH="${ENABLE_CODEX_PUSH:-true}"
CODEX_CMD="${CODEX_CMD:-codex}"
CODEX_PUSH_MODEL="${CODEX_PUSH_MODEL:-gpt-5.3-codex}"
CODEX_PUSH_FLAGS="${CODEX_PUSH_FLAGS:-}"
CODEX_PUSH_TIMEOUT_SECONDS="${CODEX_PUSH_TIMEOUT_SECONDS:-45}"
CODEX_PUSH_DAILY_BUDGET="${CODEX_PUSH_DAILY_BUDGET:-96}"
CODEX_PUSH_REASONING_EFFORT_NORMAL="${CODEX_PUSH_REASONING_EFFORT_NORMAL:-medium}"
CODEX_PUSH_REASONING_EFFORT_HIGH="${CODEX_PUSH_REASONING_EFFORT_HIGH:-high}"
CODEX_PERIODIC_TEMPLATE="${CODEX_PERIODIC_TEMPLATE:-$ROOT_DIR/tool/prompts/codex_event_periodic.md}"
CODEX_INCIDENT_TEMPLATE="${CODEX_INCIDENT_TEMPLATE:-$ROOT_DIR/tool/prompts/codex_event_incident.md}"

ENABLE_OPENCLAW_PUSH="${ENABLE_OPENCLAW_PUSH:-false}"
OPENCLAW_CMD="${OPENCLAW_CMD:-openclaw}"
OPENCLAW_AGENT_ID="${OPENCLAW_AGENT_ID:-main}"
OPENCLAW_PUSH_TIMEOUT="${OPENCLAW_PUSH_TIMEOUT:-20}"
OPENCLAW_PUSH_COOLDOWN_SECONDS="${OPENCLAW_PUSH_COOLDOWN_SECONDS:-120}"

ENABLE_WEBHOOK_PUSH="${ENABLE_WEBHOOK_PUSH:-false}"
ALERT_WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
WEBHOOK_TIMEOUT_SECONDS="${WEBHOOK_TIMEOUT_SECONDS:-10}"
EVENT_PUSH_COOLDOWN_SECONDS="${EVENT_PUSH_COOLDOWN_SECONDS:-${OPENCLAW_PUSH_COOLDOWN_SECONDS:-120}}"

mkdir -p "$PUSH_STATE_DIR" "$CODEX_PUSH_OUTPUT_DIR"

TYPE=""
SEVERITY="info"
TITLE=""
DETAIL=""
FORCE="false"
EVENT_ID=""
TS_ISO=""
HOST=""

usage() {
  cat <<'USAGE'
Usage:
  monitor_event_push.sh --type TYPE --severity LEVEL --title TITLE --detail DETAIL [--force]

Options:
  --type       event type, e.g. periodic_status / monitor_failure / risk_critical / monitor_recovered
  --severity   info | warn | critical
  --title      short title
  --detail     detail text
  --force      bypass cooldown
USAGE
}

json_escape() {
  printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])'
}

safe_key() {
  printf '%s' "$1" | tr -cs 'a-zA-Z0-9._-' '_' | sed 's/^_//;s/_$//'
}

now_ts() {
  date +%s
}

audit_log() {
  local channel="$1"
  local status="$2"
  local detail="$3"
  local detail_one_line
  detail_one_line="$(printf '%s' "$detail" | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g')"
  printf '%s|event_id=%s|type=%s|severity=%s|channel=%s|status=%s|detail=%s\n' \
    "$(date '+%Y-%m-%d %H:%M:%S')" "$EVENT_ID" "$TYPE" "$SEVERITY" "$channel" "$status" "$detail_one_line" >> "$AUDIT_LOG_FILE"
}

cooldown_ok() {
  local key="$1"
  local now last_file last=0
  now="$(now_ts)"
  last_file="$PUSH_STATE_DIR/${key}.last"
  if [[ "$FORCE" == "true" ]]; then
    echo "$now" > "$last_file"
    return 0
  fi
  if [[ -f "$last_file" ]]; then
    last="$(cat "$last_file" 2>/dev/null || echo 0)"
  fi
  [[ "$last" =~ ^[0-9]+$ ]] || last=0
  if (( now - last < EVENT_PUSH_COOLDOWN_SECONDS )); then
    return 1
  fi
  echo "$now" > "$last_file"
  return 0
}

codex_daily_budget_ok() {
  local today count=0 state_day=""
  today="$(date +%F)"

  if [[ -f "$CODEX_DAILY_STATE_FILE" ]]; then
    state_day="$(awk -F, 'NR==1{print $1}' "$CODEX_DAILY_STATE_FILE" 2>/dev/null || true)"
    count="$(awk -F, 'NR==1{print $2}' "$CODEX_DAILY_STATE_FILE" 2>/dev/null || echo 0)"
  fi

  if [[ "$state_day" != "$today" ]]; then
    echo "$today,0" > "$CODEX_DAILY_STATE_FILE"
    count=0
  fi

  [[ "$count" =~ ^[0-9]+$ ]] || count=0
  if (( count >= CODEX_PUSH_DAILY_BUDGET )); then
    return 1
  fi

  echo "$today,$((count + 1))" > "$CODEX_DAILY_STATE_FILE"
  return 0
}

build_codex_prompt() {
  local prompt_file="$1"
  local template_file
  template_file="$CODEX_INCIDENT_TEMPLATE"
  if [[ "$TYPE" == "periodic_status" ]]; then
    template_file="$CODEX_PERIODIC_TEMPLATE"
  fi

  if [[ -f "$template_file" ]]; then
    cat "$template_file" > "$prompt_file"
  else
    cat > "$prompt_file" <<'EOF'
你是巡检事件助手。请根据事件内容给出结构化结论和下一步建议。
EOF
  fi

  cat >> "$prompt_file" <<EOF

事件上下文：
- event_id: $EVENT_ID
- type: $TYPE
- severity: $SEVERITY
- title: $TITLE
- ts: $TS_ISO
- host: $HOST

detail:
$DETAIL
EOF
}

run_codex_push() {
  local now prompt_file output_file rc=0
  local -a cmd=()
  local -a extra_flags=()
  local effort="$CODEX_PUSH_REASONING_EFFORT_NORMAL"

  if [[ "$ENABLE_CODEX_PUSH" != "true" ]]; then
    return 0
  fi
  if ! command -v "$CODEX_CMD" >/dev/null 2>&1; then
    audit_log "codex" "skip" "未找到 codex 命令"
    return 0
  fi
  if ! codex_daily_budget_ok; then
    audit_log "codex" "skip" "超出每日预算 CODEX_PUSH_DAILY_BUDGET=$CODEX_PUSH_DAILY_BUDGET"
    return 0
  fi

  now="$(date +%Y%m%d_%H%M%S)"
  prompt_file="$CODEX_PUSH_OUTPUT_DIR/codex_event_prompt_${now}_${EVENT_ID}.txt"
  output_file="$CODEX_PUSH_OUTPUT_DIR/codex_event_output_${now}_${EVENT_ID}.log"
  build_codex_prompt "$prompt_file"

  if [[ "$SEVERITY" == "critical" ]] || [[ "$TYPE" == "monitor_failure" ]] || [[ "$TYPE" == "risk_critical" ]] || [[ "$TYPE" == "codex_failed" ]] || [[ "$TYPE" == "codex_verify_failed" ]]; then
    effort="$CODEX_PUSH_REASONING_EFFORT_HIGH"
  fi

  cmd=("$CODEX_CMD" exec -C "$ROOT_DIR" --skip-git-repo-check)
  if [[ -n "$CODEX_PUSH_MODEL" ]]; then
    cmd+=(-m "$CODEX_PUSH_MODEL")
  fi
  cmd+=(-c "reasoning.effort=\"$effort\"")
  if [[ -n "$CODEX_PUSH_FLAGS" ]]; then
    read -r -a extra_flags <<< "$CODEX_PUSH_FLAGS"
    cmd+=("${extra_flags[@]}")
  fi
  cmd+=(-)

  set +e
  if command -v timeout >/dev/null 2>&1; then
    timeout "$CODEX_PUSH_TIMEOUT_SECONDS" "${cmd[@]}" < "$prompt_file" > "$output_file" 2>&1
    rc=$?
  else
    "${cmd[@]}" < "$prompt_file" > "$output_file" 2>&1
    rc=$?
  fi
  set -e

  if (( rc == 0 )); then
    audit_log "codex" "ok" "output=$output_file"
    echo "[monitor_event_push] codex sent: type=$TYPE severity=$SEVERITY output=$output_file"
  else
    audit_log "codex" "failed" "rc=$rc output=$output_file"
    echo "[monitor_event_push] codex failed: rc=$rc output=$output_file" >&2
  fi
}

run_openclaw_push() {
  local message
  if [[ "$ENABLE_OPENCLAW_PUSH" != "true" ]]; then
    return 0
  fi
  if ! command -v "$OPENCLAW_CMD" >/dev/null 2>&1; then
    audit_log "openclaw" "skip" "未找到 openclaw 命令"
    return 0
  fi

  message="[AAVE_MONITOR][$SEVERITY][$TYPE] $TITLE
ts=$TS_ISO host=$HOST event_id=$EVENT_ID
detail:
$DETAIL"
  if command -v timeout >/dev/null 2>&1; then
    timeout "$OPENCLAW_PUSH_TIMEOUT" "$OPENCLAW_CMD" agent --agent "$OPENCLAW_AGENT_ID" --message "$message" --json >/dev/null || true
  else
    "$OPENCLAW_CMD" agent --agent "$OPENCLAW_AGENT_ID" --message "$message" --json >/dev/null || true
  fi
  audit_log "openclaw" "ok" "agent=$OPENCLAW_AGENT_ID"
}

run_webhook_push() {
  local payload
  if [[ "$ENABLE_WEBHOOK_PUSH" != "true" || -z "$ALERT_WEBHOOK_URL" ]]; then
    return 0
  fi
  if [[ "$ALERT_WEBHOOK_URL" == *"YOUR_OPENCLAW_WEBHOOK"* ]]; then
    return 0
  fi
  if ! command -v curl >/dev/null 2>&1; then
    audit_log "webhook" "skip" "缺少 curl"
    return 0
  fi

  payload=$(cat <<EOF
{"source":"aave_adaptive_monitor","title":"$(json_escape "$TITLE")","detail":"$(json_escape "$DETAIL")","severity":"$(json_escape "$SEVERITY")","type":"$(json_escape "$TYPE")","event_id":"$(json_escape "$EVENT_ID")","ts":"$TS_ISO"}
EOF
)
  if curl -sS -m "$WEBHOOK_TIMEOUT_SECONDS" -X POST -H 'Content-Type: application/json' -d "$payload" "$ALERT_WEBHOOK_URL" >/dev/null; then
    audit_log "webhook" "ok" "url=$ALERT_WEBHOOK_URL"
  else
    audit_log "webhook" "failed" "url=$ALERT_WEBHOOK_URL"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --type)
      TYPE="${2:-}"; shift 2 ;;
    --severity)
      SEVERITY="${2:-}"; shift 2 ;;
    --title)
      TITLE="${2:-}"; shift 2 ;;
    --detail)
      DETAIL="${2:-}"; shift 2 ;;
    --force)
      FORCE="true"; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "unknown arg: $1" >&2
      usage
      exit 2 ;;
  esac
done

if [[ -z "$TYPE" || -z "$TITLE" ]]; then
  echo "missing required args: --type and --title" >&2
  usage
  exit 2
fi

EVENT_KEY="$(safe_key "${TYPE}_${SEVERITY}")"
if ! cooldown_ok "$EVENT_KEY"; then
  echo "[monitor_event_push] cooldown skip: key=$EVENT_KEY"
  exit 0
fi

TS_ISO="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
HOST="$(hostname -s 2>/dev/null || hostname || echo unknown-host)"
EVENT_ID="$(date +%Y%m%d%H%M%S)_$RANDOM"

run_codex_push
run_openclaw_push
run_webhook_push

echo "[monitor_event_push] sent: type=$TYPE severity=$SEVERITY title=$TITLE event_id=$EVENT_ID"

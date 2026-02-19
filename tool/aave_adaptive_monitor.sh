#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKER_CONTAINER="${WORKER_CONTAINER:-datainjector-worker}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-crypto-kafka}"
ROLE_CONFIG_IN_CONTAINER="${ROLE_CONFIG_IN_CONTAINER:-/app/configs/aave/roles_aave_full_stable.json}"

INTERVAL_STEPS_CSV="${INTERVAL_STEPS_CSV:-120,300,600,1200,1800}"
IFS=',' read -r -a INTERVAL_STEPS <<< "$INTERVAL_STEPS_CSV"
if (( ${#INTERVAL_STEPS[@]} == 0 )); then
  INTERVAL_STEPS=(120 300 600 1200 1800)
fi
for i in "${!INTERVAL_STEPS[@]}"; do
  INTERVAL_STEPS[$i]="$(printf '%s' "${INTERVAL_STEPS[$i]}" | xargs)"
  if [[ ! "${INTERVAL_STEPS[$i]}" =~ ^[0-9]+$ ]] || (( INTERVAL_STEPS[$i] <= 0 )); then
    INTERVAL_STEPS[$i]=120
  fi
done
STEP_INDEX=0
STABLE_CYCLES=0
STABLE_PROMOTE_THRESHOLD="${STABLE_PROMOTE_THRESHOLD:-2}"
TOPIC_NO_GROWTH_FAIL_CYCLES="${TOPIC_NO_GROWTH_FAIL_CYCLES:-3}"
ERROR_SCAN_WINDOW_SECONDS="${ERROR_SCAN_WINDOW_SECONDS:-120}"

ERROR_REGEX="${ERROR_REGEX:-panic|fatal|segmentation fault|concurrent write to websocket connection|role\\.stop|ws\\.read\\.error|handler\\.error|sink\\.error|pipeline\\.error|caller\\.error|websocket .*failed}"
# 收敛正则：避免把 trace/span 等十六进制或长数字中的“429”误判为风控
# 仅匹配独立 429/418 或典型字段（status_code/code），并保留常见文本提示
BINANCE_RISK_REGEX="${BINANCE_RISK_REGEX:-(\b429\b|\b418\b|status_code[^0-9A-Za-z]*429|code[^0-9A-Za-z]*-?1003|too many requests|too_many_requests|ip banned|banned until|rate limit|ratelimit)}"
BINANCE_RISK_BURST_THRESHOLD="${BINANCE_RISK_BURST_THRESHOLD:-5}"
BINANCE_RISK_COUNT_WINDOW_SECONDS="${BINANCE_RISK_COUNT_WINDOW_SECONDS:-600}"
BINANCE_RISK_COUNT_THRESHOLD="${BINANCE_RISK_COUNT_THRESHOLD:-20}"

ALERT_WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
ALERT_COOLDOWN_SECONDS="${ALERT_COOLDOWN_SECONDS:-180}"

ENABLE_CODEX_ESCALATION="${ENABLE_CODEX_ESCALATION:-false}"
CODEX_TRIGGER_FAIL_CYCLES="${CODEX_TRIGGER_FAIL_CYCLES:-3}"
CODEX_ESCALATION_COOLDOWN_SECONDS="${CODEX_ESCALATION_COOLDOWN_SECONDS:-1800}"
CODEX_MAX_ESCALATIONS_PER_DAY="${CODEX_MAX_ESCALATIONS_PER_DAY:-6}"
CODEX_MODEL="${CODEX_MODEL:-gpt-5.3-codex}"
CODEX_FLAGS="${CODEX_FLAGS:---dangerously-bypass-approvals-and-sandbox}"
CODEX_ESCALATION_REASONING_EFFORT="${CODEX_ESCALATION_REASONING_EFFORT:-high}"
CODEX_PROMPT_TEMPLATE="${CODEX_PROMPT_TEMPLATE:-$ROOT_DIR/tool/prompts/codex_escalation.md}"
POST_FIX_OBSERVE_SECONDS="${POST_FIX_OBSERVE_SECONDS:-180}"
POST_FIX_CHECK_CYCLES="${POST_FIX_CHECK_CYCLES:-3}"

ENABLE_RISK_ROLE_PAUSE="${ENABLE_RISK_ROLE_PAUSE:-false}"
RISK_ROLE_IDS_CSV="${RISK_ROLE_IDS_CSV:-rec-binance-perp-aave-orderbook-diff,rec-binance-perp-aave-aggtrade-full,rec-binance-spot-aave-orderbook-diff,rec-binance-spot-aave-aggtrade-full}"
RISK_PAUSE_COOLDOWN_SECONDS="${RISK_PAUSE_COOLDOWN_SECONDS:-1800}"

ENABLE_EVENT_PUSH="${ENABLE_EVENT_PUSH:-true}"
EVENT_PUSH_SCRIPT="${EVENT_PUSH_SCRIPT:-$ROOT_DIR/tool/monitor_event_push.sh}"
PERIODIC_PUSH_SECONDS="${PERIODIC_PUSH_SECONDS:-900}"

STATE_DIR="${STATE_DIR:-$ROOT_DIR/runtime/data}"
LOG_FILE="${LOG_FILE:-$STATE_DIR/aave_role_monitor_$(date +%Y%m%d_%H%M%S).log}"
PID_FILE="${PID_FILE:-$STATE_DIR/aave_role_monitor.pid}"
LOCK_FILE="${LOCK_FILE:-$STATE_DIR/aave_role_monitor.lock}"
LOCK_META_FILE="${LOCK_META_FILE:-$STATE_DIR/aave_role_monitor.lock.meta}"
STATUS_FILE="${STATUS_FILE:-$STATE_DIR/aave_role_monitor_status.json}"
EVENTS_FILE="${EVENTS_FILE:-$STATE_DIR/aave_role_monitor_events.jsonl}"
ALERT_TS_FILE="${ALERT_TS_FILE:-$STATE_DIR/aave_role_monitor_last_alert.ts}"
CODEX_TS_FILE="${CODEX_TS_FILE:-$STATE_DIR/aave_role_monitor_last_codex.ts}"
CODEX_DAILY_FILE="${CODEX_DAILY_FILE:-$STATE_DIR/aave_role_monitor_codex_daily.state}"
CODEX_OUTPUT_DIR="${CODEX_OUTPUT_DIR:-$STATE_DIR/aave_codex_escalation}"
RISK_PAUSE_TS_FILE="${RISK_PAUSE_TS_FILE:-$STATE_DIR/aave_role_monitor_last_risk_pause.ts}"
PERIODIC_PUSH_TS_FILE="${PERIODIC_PUSH_TS_FILE:-$STATE_DIR/aave_role_monitor_last_periodic_push.ts}"

ROLE_IDS=(
  "rec-binance-perp-aave-orderbook-diff"
  "rec-binance-perp-aave-orderbook-snapshot"
  "rec-binance-perp-aave-aggtrade-full"
  "rec-binance-spot-aave-orderbook-diff"
  "rec-binance-spot-aave-orderbook-snapshot"
  "rec-binance-spot-aave-aggtrade-full"
)

TOPICS=(
  "perp.orderbook.diff"
  "perp.orderbook.snapshot"
  "perp.aggtrades"
  "spot.orderbook.diff"
  "spot.orderbook.snapshot"
  "spot.aggtrades"
)

mkdir -p "$STATE_DIR" "$CODEX_OUTPUT_DIR"

declare -A PREV_OFFSET
declare -A NO_GROWTH_CYCLES
CONSEC_FAIL_CYCLES=0
LAST_HEALTHY=true
LAST_ERROR_HITS=""
LAST_BINANCE_RISK_HITS=""
RISK_CRITICAL_THIS_CYCLE=false
RISK_CRITICAL_REASON=""
POST_FIX_VERIFY_REASON=""
LAST_REASON_TEXT=""

TOTAL_CYCLES=0
HEALTHY_CYCLES=0
FAILED_CYCLES=0
SELF_HEAL_SUCCESS_COUNT=0
SELF_HEAL_FAIL_COUNT=0
CODEX_TRIGGER_COUNT=0
CODEX_SUCCESS_COUNT=0
CODEX_FAIL_COUNT=0
CODEX_VERIFY_PASS_COUNT=0
CODEX_VERIFY_FAIL_COUNT=0
LOCK_WITH_FLOCK=false

IFS=',' read -r -a RISK_ROLE_IDS <<< "$RISK_ROLE_IDS_CSV"
for i in "${!RISK_ROLE_IDS[@]}"; do
  RISK_ROLE_IDS[$i]="$(printf '%s' "${RISK_ROLE_IDS[$i]}" | xargs)"
done

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE"
}

json_escape() {
  printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1])'
}

now_ts() {
  date +%s
}

emit_event() {
  local event_type="$1"
  local severity="$2"
  local detail="${3:-}"
  printf '{"ts":"%s","event":"%s","severity":"%s","pid":%s,"interval_seconds":%s,"stable_cycles":%s,"consecutive_failures":%s,"detail":"%s"}\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "$(json_escape "$event_type")" \
    "$(json_escape "$severity")" \
    "$$" \
    "${INTERVAL_STEPS[$STEP_INDEX]}" \
    "$STABLE_CYCLES" \
    "$CONSEC_FAIL_CYCLES" \
    "$(json_escape "$detail")" >> "$EVENTS_FILE"
}

write_status_snapshot() {
  local state="$1"
  local reason="${2:-}"
  local tmp_file="${STATUS_FILE}.tmp"
  cat > "$tmp_file" <<EOF
{"ts":"$(date -u '+%Y-%m-%dT%H:%M:%SZ')","pid":$$,"state":"$(json_escape "$state")","reason":"$(json_escape "$reason")","interval_seconds":${INTERVAL_STEPS[$STEP_INDEX]},"stable_cycles":$STABLE_CYCLES,"consecutive_failures":$CONSEC_FAIL_CYCLES,"total_cycles":$TOTAL_CYCLES,"healthy_cycles":$HEALTHY_CYCLES,"failed_cycles":$FAILED_CYCLES,"self_heal_success_count":$SELF_HEAL_SUCCESS_COUNT,"self_heal_fail_count":$SELF_HEAL_FAIL_COUNT,"codex_trigger_count":$CODEX_TRIGGER_COUNT,"codex_success_count":$CODEX_SUCCESS_COUNT,"codex_fail_count":$CODEX_FAIL_COUNT,"codex_verify_pass_count":$CODEX_VERIFY_PASS_COUNT,"codex_verify_fail_count":$CODEX_VERIFY_FAIL_COUNT,"log_file":"$(json_escape "$LOG_FILE")","last_reason":"$(json_escape "$LAST_REASON_TEXT")"}
EOF
  mv "$tmp_file" "$STATUS_FILE"
}

cleanup_on_exit() {
  if [[ "$LOCK_WITH_FLOCK" != "true" ]]; then
    rm -f "$LOCK_FILE"
  fi
  rm -f "$LOCK_META_FILE"
  if [[ -f "$PID_FILE" ]] && [[ "$(cat "$PID_FILE" 2>/dev/null || true)" == "$$" ]]; then
    rm -f "$PID_FILE"
  fi
}

acquire_single_instance_lock() {
  if command -v flock >/dev/null 2>&1; then
    LOCK_WITH_FLOCK=true
    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
      log "检测到已有巡检实例在运行，当前实例退出（lock: $LOCK_FILE）"
      emit_event "instance_conflict" "critical" "lock=$LOCK_FILE"
      exit 2
    fi
  else
    if [[ -f "$LOCK_FILE" ]]; then
      local old_pid
      old_pid="$(cat "$LOCK_FILE" 2>/dev/null || true)"
      if [[ "$old_pid" =~ ^[0-9]+$ ]] && kill -0 "$old_pid" 2>/dev/null; then
        log "检测到已有巡检实例在运行 pid=${old_pid}，当前实例退出"
        emit_event "instance_conflict" "critical" "pid=${old_pid}"
        exit 2
      fi
    fi
    echo "$$" > "$LOCK_FILE"
  fi
  printf '{"pid":%s,"ts":"%s","log_file":"%s"}\n' "$$" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$LOG_FILE" > "$LOCK_META_FILE"
  trap cleanup_on_exit EXIT INT TERM
}

worker_running() {
  local running
  running="$(docker inspect -f '{{.State.Running}}' "$WORKER_CONTAINER" 2>/dev/null || true)"
  [[ "$running" == "true" ]]
}

ensure_worker_up() {
  if worker_running; then
    return 0
  fi
  log "worker 未运行，尝试启动 worker-app"
  (cd "$ROOT_DIR" && ./tool/orchestration.sh w >/dev/null)
  sleep 4
  worker_running
}

apply_roles() {
  log "执行 roles apply"
  docker exec "$WORKER_CONTAINER" sh -lc \
    "curl -sS -X POST http://127.0.0.1:8090/api/roles/apply -H 'Content-Type: application/json' --data-binary @$ROLE_CONFIG_IN_CONTAINER"
}

roles_ok() {
  local roles_json id
  roles_json="$(docker exec "$WORKER_CONTAINER" sh -lc 'curl -sS http://127.0.0.1:8090/api/roles' 2>/dev/null || true)"
  [[ -n "$roles_json" ]] || return 1
  for id in "${ROLE_IDS[@]}"; do
    [[ "$roles_json" == *"$id"* ]] || return 1
  done
  return 0
}

sum_topic_offsets() {
  local topic="$1"
  local out
  out="$(docker exec "$KAFKA_CONTAINER" bash -lc "kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic $topic --time -1" 2>/dev/null || true)"
  if [[ -z "$out" ]]; then
    echo "-1"
    return 0
  fi
  printf '%s\n' "$out" | awk -F: 'NF>=3 && $3 ~ /^[0-9]+$/ {s+=$3} END {if (NR==0) print -1; else print s+0}'
}

get_recent_error_hits() {
  local interval="$1"
  docker logs --since "${interval}s" "$WORKER_CONTAINER" 2>&1 | grep -Ei "$ERROR_REGEX" | tail -n 50 || true
}

print_recent_errors() {
  local interval="$1"
  local hits
  hits="$(get_recent_error_hits "$interval")"
  LAST_ERROR_HITS="$hits"
  if [[ -n "$hits" ]]; then
    log "检测到高信号错误日志："
    printf '%s\n' "$hits" | tail -n 20 | sed 's/^/  /' | tee -a "$LOG_FILE"
    return 1
  fi
  return 0
}

topic_snapshot_text() {
  local lines=()
  local topic curr
  for topic in "${TOPICS[@]}"; do
    curr="$(sum_topic_offsets "$topic")"
    lines+=("$topic=$curr")
  done
  printf '%s\n' "${lines[@]}"
}

maybe_send_alert() {
  local title="$1"
  local detail="$2"
  local force="${3:-false}"
  local now last=0 payload

  if [[ -z "$ALERT_WEBHOOK_URL" ]]; then
    return 0
  fi
  if [[ "$ALERT_WEBHOOK_URL" == *"YOUR_OPENCLAW_WEBHOOK"* ]]; then
    return 0
  fi
  if ! command -v curl >/dev/null 2>&1; then
    log "告警跳过：curl 不存在"
    return 0
  fi

  now="$(now_ts)"
  if [[ -f "$ALERT_TS_FILE" ]]; then
    last="$(cat "$ALERT_TS_FILE" 2>/dev/null || echo 0)"
  fi
  if [[ "$force" != "true" ]] && [[ "$last" =~ ^[0-9]+$ ]] && (( now - last < ALERT_COOLDOWN_SECONDS )); then
    log "告警节流：距离上次不足 ${ALERT_COOLDOWN_SECONDS}s"
    return 0
  fi

  payload=$(cat <<EOF
{"source":"aave_adaptive_monitor","title":"$(json_escape "$title")","detail":"$(json_escape "$detail")","ts":"$(date -u '+%Y-%m-%dT%H:%M:%SZ')"}
EOF
)

  if curl -sS -m 10 -X POST -H 'Content-Type: application/json' -d "$payload" "$ALERT_WEBHOOK_URL" >/dev/null; then
    echo "$now" > "$ALERT_TS_FILE"
    log "告警已发送: $title"
  else
    log "告警发送失败: $title"
  fi
}

push_event() {
  local event_type="$1"
  local severity="$2"
  local title="$3"
  local detail="$4"
  local force="${5:-false}"

  if [[ "$ENABLE_EVENT_PUSH" != "true" ]]; then
    return 0
  fi
  if [[ ! -x "$EVENT_PUSH_SCRIPT" ]]; then
    log "事件推送跳过：脚本不存在或不可执行: $EVENT_PUSH_SCRIPT"
    return 0
  fi

  if [[ "$force" == "true" ]]; then
    "$EVENT_PUSH_SCRIPT" --type "$event_type" --severity "$severity" --title "$title" --detail "$detail" --force >/dev/null 2>&1 || true
  else
    "$EVENT_PUSH_SCRIPT" --type "$event_type" --severity "$severity" --title "$title" --detail "$detail" >/dev/null 2>&1 || true
  fi
}

periodic_push_due() {
  local now last=0
  now="$(now_ts)"
  if [[ -f "$PERIODIC_PUSH_TS_FILE" ]]; then
    last="$(cat "$PERIODIC_PUSH_TS_FILE" 2>/dev/null || echo 0)"
  fi
  [[ "$last" =~ ^[0-9]+$ ]] || last=0
  if (( now - last >= PERIODIC_PUSH_SECONDS )); then
    echo "$now" > "$PERIODIC_PUSH_TS_FILE"
    return 0
  fi
  return 1
}

observe_topic_growth_after_fix() {
  local observe_seconds="$1"
  local check_cycles="$2"
  local interval i topic start curr
  local -a growth_reasons=()
  local -A start_offsets=()
  local -A end_offsets=()

  [[ "$observe_seconds" =~ ^[0-9]+$ ]] || observe_seconds=180
  [[ "$check_cycles" =~ ^[0-9]+$ ]] || check_cycles=3
  (( observe_seconds > 0 )) || observe_seconds=180
  (( check_cycles > 0 )) || check_cycles=3

  interval=$((observe_seconds / check_cycles))
  (( interval > 0 )) || interval=1

  for topic in "${TOPICS[@]}"; do
    start="$(sum_topic_offsets "$topic")"
    start_offsets["$topic"]="$start"
    if [[ "$start" == "-1" ]]; then
      POST_FIX_VERIFY_REASON="topic偏移读取失败:$topic"
      return 1
    fi
    log "Codex后置校验初始 topic=$topic offset=$start"
  done

  for ((i=1; i<=check_cycles; i++)); do
    sleep "$interval"
    for topic in "${TOPICS[@]}"; do
      curr="$(sum_topic_offsets "$topic")"
      end_offsets["$topic"]="$curr"
      if [[ "$curr" == "-1" ]]; then
        POST_FIX_VERIFY_REASON="topic偏移读取失败:$topic"
        return 1
      fi
      log "Codex后置观测[$i/$check_cycles] topic=$topic offset=$curr"
    done
  done

  for topic in "${TOPICS[@]}"; do
    start="${start_offsets[$topic]}"
    curr="${end_offsets[$topic]:-${start_offsets[$topic]}}"
    if (( curr <= start )); then
      growth_reasons+=("topic无增量:$topic(start=$start,end=$curr)")
    fi
  done

  if (( ${#growth_reasons[@]} > 0 )); then
    POST_FIX_VERIFY_REASON="$(join_reasons "${growth_reasons[@]}")"
    return 1
  fi

  return 0
}

post_codex_hard_verify() {
  POST_FIX_VERIFY_REASON=""

  if ! ensure_worker_up; then
    POST_FIX_VERIFY_REASON="worker不可用"
    return 1
  fi

  if ! roles_ok; then
    POST_FIX_VERIFY_REASON="role缺失"
    return 1
  fi

  if ! observe_topic_growth_after_fix "$POST_FIX_OBSERVE_SECONDS" "$POST_FIX_CHECK_CYCLES"; then
    return 1
  fi

  if ! print_recent_errors "$POST_FIX_OBSERVE_SECONDS"; then
    POST_FIX_VERIFY_REASON="仍有高信号错误日志"
    return 1
  fi

  return 0
}

get_recent_binance_risk_hits() {
  local interval="$1"
  docker logs --since "${interval}s" "$WORKER_CONTAINER" 2>&1 | grep -Ei "$BINANCE_RISK_REGEX" | tail -n 200 || true
}

count_recent_binance_risk_hits() {
  local interval="$1"
  local hits
  hits="$(get_recent_binance_risk_hits "$interval")"
  if [[ -z "$hits" ]]; then
    echo 0
  else
    printf '%s\n' "$hits" | wc -l | awk '{print $1}'
  fi
}

risk_pause_cooldown_ok() {
  local now last=0
  now="$(now_ts)"
  if [[ -f "$RISK_PAUSE_TS_FILE" ]]; then
    last="$(cat "$RISK_PAUSE_TS_FILE" 2>/dev/null || echo 0)"
  fi
  [[ "$last" =~ ^[0-9]+$ ]] || last=0
  if (( now - last < RISK_PAUSE_COOLDOWN_SECONDS )); then
    log "高风险 role 暂停节流：距离上次不足 ${RISK_PAUSE_COOLDOWN_SECONDS}s"
    return 1
  fi
  echo "$now" > "$RISK_PAUSE_TS_FILE"
  return 0
}

pause_high_risk_roles() {
  local payload=""
  local role_id
  local resp

  if [[ "$ENABLE_RISK_ROLE_PAUSE" != "true" ]]; then
    return 0
  fi
  if (( ${#RISK_ROLE_IDS[@]} == 0 )); then
    log "高风险 role 暂停跳过：RISK_ROLE_IDS 为空"
    return 0
  fi
  if ! risk_pause_cooldown_ok; then
    return 0
  fi

  payload='{"role_ids":['
  for role_id in "${RISK_ROLE_IDS[@]}"; do
    [[ -n "$role_id" ]] || continue
    payload="${payload}\"${role_id}\","
  done
  payload="${payload%,}]}"

  log "触发高风险 role 暂停: ${RISK_ROLE_IDS[*]}"
  resp="$(printf '%s' "$payload" | docker exec -i "$WORKER_CONTAINER" sh -lc \
    "curl -sS -X POST http://127.0.0.1:8090/api/roles/stop -H 'Content-Type: application/json' --data-binary @-" || true)"
  log "高风险 role 暂停响应: $resp"
}

evaluate_binance_risk() {
  local c_short c_long
  local reason_parts=()

  RISK_CRITICAL_THIS_CYCLE=false
  RISK_CRITICAL_REASON=""
  LAST_BINANCE_RISK_HITS=""

  c_short="$(count_recent_binance_risk_hits "$ERROR_SCAN_WINDOW_SECONDS")"
  c_long="$(count_recent_binance_risk_hits "$BINANCE_RISK_COUNT_WINDOW_SECONDS")"

  if (( c_short >= BINANCE_RISK_BURST_THRESHOLD )); then
    reason_parts+=("binance风控错误120s内${c_short}次")
  fi
  if (( c_long >= BINANCE_RISK_COUNT_THRESHOLD )); then
    reason_parts+=("binance风控错误${BINANCE_RISK_COUNT_WINDOW_SECONDS}s内${c_long}次")
  fi

  if (( ${#reason_parts[@]} == 0 )); then
    return 0
  fi

  LAST_BINANCE_RISK_HITS="$(get_recent_binance_risk_hits "$BINANCE_RISK_COUNT_WINDOW_SECONDS" | tail -n 50)"
  RISK_CRITICAL_REASON="$(join_reasons "${reason_parts[@]}")"
  RISK_CRITICAL_THIS_CYCLE=true
  log "命中 Binance 风控关键风险: $RISK_CRITICAL_REASON"
  if [[ -n "$LAST_BINANCE_RISK_HITS" ]]; then
    printf '%s\n' "$LAST_BINANCE_RISK_HITS" | sed 's/^/  /' | tee -a "$LOG_FILE"
  fi
  return 1
}

codex_daily_budget_ok() {
  local today count=0 state_day=""
  today="$(date +%F)"

  if [[ -f "$CODEX_DAILY_FILE" ]]; then
    state_day="$(awk -F, 'NR==1{print $1}' "$CODEX_DAILY_FILE" 2>/dev/null || true)"
    count="$(awk -F, 'NR==1{print $2}' "$CODEX_DAILY_FILE" 2>/dev/null || echo 0)"
  fi

  if [[ "$state_day" != "$today" ]]; then
    echo "$today,0" > "$CODEX_DAILY_FILE"
    count=0
  fi

  [[ "$count" =~ ^[0-9]+$ ]] || count=0
  if (( count >= CODEX_MAX_ESCALATIONS_PER_DAY )); then
    log "Codex 升级跳过：已达当日上限 ${CODEX_MAX_ESCALATIONS_PER_DAY}"
    return 1
  fi

  echo "$today,$((count + 1))" > "$CODEX_DAILY_FILE"
  return 0
}

codex_cooldown_ok() {
  local now last=0
  now="$(now_ts)"
  if [[ -f "$CODEX_TS_FILE" ]]; then
    last="$(cat "$CODEX_TS_FILE" 2>/dev/null || echo 0)"
  fi
  [[ "$last" =~ ^[0-9]+$ ]] || last=0
  if (( now - last < CODEX_ESCALATION_COOLDOWN_SECONDS )); then
    log "Codex 升级节流：距离上次不足 ${CODEX_ESCALATION_COOLDOWN_SECONDS}s"
    return 1
  fi
  echo "$now" > "$CODEX_TS_FILE"
  return 0
}

run_codex_escalation() {
  local reason="$1"
  local interval="$2"
  local now prompt_file output_file roles_json topic_snapshot
  local rc=0

  if [[ "$ENABLE_CODEX_ESCALATION" != "true" ]]; then
    return 0
  fi
  if ! command -v codex >/dev/null 2>&1; then
    log "Codex 升级跳过：未找到 codex 命令"
    return 0
  fi
  if ! codex_cooldown_ok; then
    return 0
  fi
  if ! codex_daily_budget_ok; then
    return 0
  fi

  now="$(date +%Y%m%d_%H%M%S)"
  prompt_file="$CODEX_OUTPUT_DIR/codex_prompt_${now}.txt"
  output_file="$CODEX_OUTPUT_DIR/codex_output_${now}.log"
  CODEX_TRIGGER_COUNT=$((CODEX_TRIGGER_COUNT + 1))
  emit_event "codex_triggered" "critical" "reason=$reason; output=$output_file"
  write_status_snapshot "codex_running" "$reason"

  roles_json="$(docker exec "$WORKER_CONTAINER" sh -lc 'curl -sS http://127.0.0.1:8090/api/roles' 2>/dev/null || true)"
  topic_snapshot="$(topic_snapshot_text)"

  if [[ -f "$CODEX_PROMPT_TEMPLATE" ]]; then
    cat "$CODEX_PROMPT_TEMPLATE" > "$prompt_file"
  else
    cat > "$prompt_file" <<'EOF'
你正在执行无人值守巡检故障排查。
请完成定位、最小修复、重启必要组件，并持续观测一段时间后再给结论。
EOF
  fi

  cat >> "$prompt_file" <<EOF

当前巡检失败上下文：
- 失败原因: $reason
- 连续失败轮次: $CONSEC_FAIL_CYCLES
- 当前检测间隔: ${interval}s
- 角色状态: $roles_json
- topic 快照:
$topic_snapshot

后置校验约束（必须满足）：
- 请在修复后重启必要服务；
- 修复后至少观测 ${POST_FIX_OBSERVE_SECONDS}s；
- 6 个 topic 在观测窗口内必须有增量；
- /api/roles 必须包含 6 个目标 role；
- 最近高信号错误日志必须清零。

最近高信号错误日志（最多 50 行）：
$LAST_ERROR_HITS

最近 Binance 风控相关日志（最多 50 行）：
$LAST_BINANCE_RISK_HITS
EOF

  log "触发 Codex 升级排障: $reason"
  maybe_send_alert "AAVE巡检触发Codex" "reason=$reason, fail_cycles=$CONSEC_FAIL_CYCLES"
  push_event "codex_triggered" "critical" "AAVE巡检触发Codex" "reason=$reason; fail_cycles=$CONSEC_FAIL_CYCLES"

  set +e
  if [[ -n "$CODEX_MODEL" ]]; then
    codex exec -m "$CODEX_MODEL" -c "reasoning.effort=\"$CODEX_ESCALATION_REASONING_EFFORT\"" $CODEX_FLAGS -C "$ROOT_DIR" - < "$prompt_file" > "$output_file" 2>&1
    rc=$?
  else
    codex exec -c "reasoning.effort=\"$CODEX_ESCALATION_REASONING_EFFORT\"" $CODEX_FLAGS -C "$ROOT_DIR" - < "$prompt_file" > "$output_file" 2>&1
    rc=$?
  fi
  set -e

  if (( rc == 0 )); then
    CODEX_SUCCESS_COUNT=$((CODEX_SUCCESS_COUNT + 1))
    log "Codex 升级执行完成: $output_file"
    maybe_send_alert "AAVE Codex执行完成" "output=$output_file"
    push_event "codex_completed" "warn" "AAVE Codex执行完成" "output=$output_file"
    emit_event "codex_completed" "warn" "output=$output_file"
    if post_codex_hard_verify; then
      CODEX_VERIFY_PASS_COUNT=$((CODEX_VERIFY_PASS_COUNT + 1))
      log "Codex 后置硬校验通过"
      maybe_send_alert "AAVE Codex闭环验证通过" "output=$output_file"
      push_event "codex_verified" "warn" "AAVE Codex闭环验证通过" "output=$output_file"
      emit_event "codex_verified" "warn" "output=$output_file"
      write_status_snapshot "healthy" "codex_verified"
    else
      CODEX_VERIFY_FAIL_COUNT=$((CODEX_VERIFY_FAIL_COUNT + 1))
      log "Codex 后置硬校验失败: $POST_FIX_VERIFY_REASON"
      maybe_send_alert "AAVE Codex闭环验证失败" "reason=$POST_FIX_VERIFY_REASON, output=$output_file"
      push_event "codex_verify_failed" "critical" "AAVE Codex闭环验证失败" "reason=$POST_FIX_VERIFY_REASON; output=$output_file" "true"
      emit_event "codex_verify_failed" "critical" "reason=$POST_FIX_VERIFY_REASON; output=$output_file"
      write_status_snapshot "degraded" "$POST_FIX_VERIFY_REASON"
    fi
  else
    CODEX_FAIL_COUNT=$((CODEX_FAIL_COUNT + 1))
    log "Codex 升级执行失败 rc=$rc, 输出: $output_file"
    maybe_send_alert "AAVE Codex执行失败" "rc=$rc, output=$output_file"
    push_event "codex_failed" "critical" "AAVE Codex执行失败" "rc=$rc; output=$output_file" "true"
    emit_event "codex_failed" "critical" "rc=$rc; output=$output_file"
    write_status_snapshot "degraded" "codex_failed"
  fi
}

recover_runtime() {
  local reason="$1"
  local apply_resp

  log "触发自愈: $reason"
  if ! ensure_worker_up; then
    log "自愈失败: worker 启动失败"
    return 1
  fi

  apply_resp="$(apply_roles || true)"
  log "roles apply 响应: $apply_resp"
  sleep 3

  if ! roles_ok; then
    log "角色仍不完整，尝试重启 worker 后再 apply"
    docker restart "$WORKER_CONTAINER" >/dev/null || true
    sleep 6
    apply_resp="$(apply_roles || true)"
    log "重启后 roles apply 响应: $apply_resp"
  fi

  roles_ok
}

join_reasons() {
  local arr=("$@")
  local out=""
  local item
  for item in "${arr[@]}"; do
    if [[ -z "$out" ]]; then
      out="$item"
    else
      out="$out; $item"
    fi
  done
  printf '%s' "$out"
}

main_loop() {
  acquire_single_instance_lock
  log "启动 AAVE 自适应巡检"
  log "日志文件: $LOG_FILE"
  echo $$ > "$PID_FILE"
  emit_event "monitor_started" "info" "log_file=$LOG_FILE; pid=$$"
  write_status_snapshot "starting" "monitor_started"
  push_event "monitor_started" "info" "AAVE巡检已启动" "log_file=$LOG_FILE; pid=$$; interval=${INTERVAL_STEPS[$STEP_INDEX]}s"

  while true; do
    local interval healthy topic curr prev delta reason_text
    local skip_recover=false
    local codex_triggered=false
    local -a fail_reasons=()
    TOTAL_CYCLES=$((TOTAL_CYCLES + 1))
    interval="${INTERVAL_STEPS[$STEP_INDEX]}"
    healthy=true

    if ! ensure_worker_up; then
      healthy=false
      fail_reasons+=("worker不可用")
    fi

    if [[ "$healthy" == true ]] && ! roles_ok; then
      healthy=false
      fail_reasons+=("role缺失")
    fi

    if [[ "$healthy" == true ]]; then
      for topic in "${TOPICS[@]}"; do
        curr="$(sum_topic_offsets "$topic")"
        prev="${PREV_OFFSET[$topic]:--1}"

        if [[ "$curr" == "-1" ]]; then
          healthy=false
          fail_reasons+=("topic偏移读取失败:$topic")
          log "topic 偏移读取失败: $topic"
          continue
        fi

        if [[ "$prev" == "-1" ]]; then
          PREV_OFFSET["$topic"]="$curr"
          NO_GROWTH_CYCLES["$topic"]=0
          log "topic=$topic offset=$curr delta=INIT"
          continue
        fi

        delta=$((curr - prev))
        PREV_OFFSET["$topic"]="$curr"

        if (( delta > 0 )); then
          NO_GROWTH_CYCLES["$topic"]=0
        else
          NO_GROWTH_CYCLES["$topic"]=$(( ${NO_GROWTH_CYCLES[$topic]:-0} + 1 ))
        fi

        log "topic=$topic offset=$curr delta=$delta no_growth_cycles=${NO_GROWTH_CYCLES[$topic]}"

        if (( NO_GROWTH_CYCLES[$topic] >= TOPIC_NO_GROWTH_FAIL_CYCLES )); then
          healthy=false
          fail_reasons+=("topic无新数据:$topic(${NO_GROWTH_CYCLES[$topic]}轮)")
        fi
      done

      if ! print_recent_errors "$ERROR_SCAN_WINDOW_SECONDS"; then
        healthy=false
        fail_reasons+=("高信号错误日志")
      fi

      if ! evaluate_binance_risk; then
        healthy=false
        fail_reasons+=("$RISK_CRITICAL_REASON")
      fi
    fi

    if [[ "$healthy" == true ]]; then
      HEALTHY_CYCLES=$((HEALTHY_CYCLES + 1))
      STABLE_CYCLES=$((STABLE_CYCLES + 1))
      CONSEC_FAIL_CYCLES=0
      LAST_REASON_TEXT=""

      if [[ "$LAST_HEALTHY" != "true" ]]; then
        maybe_send_alert "AAVE巡检恢复" "角色和topic已恢复健康"
        push_event "monitor_recovered" "warn" "AAVE巡检恢复" "interval=${interval}s; stable_cycles=$STABLE_CYCLES"
      fi
      LAST_HEALTHY=true

      log "本轮健康通过 interval=${interval}s stable_cycles=$STABLE_CYCLES"
      emit_event "cycle_healthy" "info" "interval=${interval}s; stable_cycles=$STABLE_CYCLES"
      write_status_snapshot "healthy" ""
      if periodic_push_due; then
        push_event "periodic_status" "info" "AAVE巡检定期状态" "health=ok; interval=${interval}s; stable_cycles=$STABLE_CYCLES; topic_snapshot=$(topic_snapshot_text | tr '\n' ';')"
      fi
      if (( STABLE_CYCLES >= STABLE_PROMOTE_THRESHOLD )) && (( STEP_INDEX < ${#INTERVAL_STEPS[@]} - 1 )); then
        STEP_INDEX=$((STEP_INDEX + 1))
        STABLE_CYCLES=0
        log "提升巡检间隔到 ${INTERVAL_STEPS[$STEP_INDEX]}s"
      fi
    else
      FAILED_CYCLES=$((FAILED_CYCLES + 1))
      STABLE_CYCLES=0
      STEP_INDEX=0
      CONSEC_FAIL_CYCLES=$((CONSEC_FAIL_CYCLES + 1))
      LAST_HEALTHY=false
      reason_text="$(join_reasons "${fail_reasons[@]}")"
      LAST_REASON_TEXT="$reason_text"

      log "本轮失败: $reason_text, consecutive_failures=$CONSEC_FAIL_CYCLES"
      emit_event "cycle_failed" "critical" "reasons=$reason_text; consecutive_failures=$CONSEC_FAIL_CYCLES; interval=${interval}s"
      write_status_snapshot "degraded" "$reason_text"
      maybe_send_alert "AAVE巡检异常" "reasons=$reason_text, consecutive_failures=$CONSEC_FAIL_CYCLES"
      push_event "monitor_failure" "critical" "AAVE巡检异常" "reasons=$reason_text; consecutive_failures=$CONSEC_FAIL_CYCLES; interval=${interval}s" "true"

      if [[ "$RISK_CRITICAL_THIS_CYCLE" == "true" ]]; then
        maybe_send_alert "AAVE_BINANCE_RISK_CRITICAL" "reason=$RISK_CRITICAL_REASON" true
        push_event "risk_critical" "critical" "AAVE_BINANCE_RISK_CRITICAL" "reason=$RISK_CRITICAL_REASON; consecutive_failures=$CONSEC_FAIL_CYCLES" "true"
        pause_high_risk_roles || true
        if [[ "$ENABLE_RISK_ROLE_PAUSE" == "true" ]]; then
          skip_recover=true
          log "Binance 风控关键风险已命中且已启用高风险 role 暂停，跳过自动 re-apply/restart"
        fi
        run_codex_escalation "Binance风控关键风险: $RISK_CRITICAL_REASON" "$interval"
        codex_triggered=true
      fi

      if [[ "$skip_recover" != "true" ]]; then
        emit_event "recover_started" "warn" "reason=$reason_text"
        if recover_runtime "健康检查未通过: $reason_text"; then
          SELF_HEAL_SUCCESS_COUNT=$((SELF_HEAL_SUCCESS_COUNT + 1))
          log "自愈完成，下一轮回到 120s 检查"
          emit_event "recover_succeeded" "warn" "reason=$reason_text"
        else
          SELF_HEAL_FAIL_COUNT=$((SELF_HEAL_FAIL_COUNT + 1))
          log "自愈未完全成功，下一轮继续 120s 检查"
          emit_event "recover_failed" "critical" "reason=$reason_text"
        fi
      else
        log "本轮已跳过自动自愈，等待 OpenClaw/Codex 处理"
        emit_event "recover_skipped" "warn" "reason=$reason_text"
      fi

      if [[ "$codex_triggered" != "true" ]] && (( CONSEC_FAIL_CYCLES >= CODEX_TRIGGER_FAIL_CYCLES )); then
        run_codex_escalation "$reason_text" "$interval"
      fi
    fi

    sleep "${INTERVAL_STEPS[$STEP_INDEX]}"
  done
}

main_loop

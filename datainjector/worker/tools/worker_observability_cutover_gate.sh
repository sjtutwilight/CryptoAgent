#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROM_URL="${PROM_URL:-http://localhost:9090}"
LOKI_URL="${LOKI_URL:-http://localhost:3100}"
RUN_RUNTIME_CHECKS="${RUN_RUNTIME_CHECKS:-0}"

WORKER_DASHBOARD="$ROOT_DIR/observability/provisioning/dashboards/worker-observability-dashboard.json"
LOGS_DASHBOARD="$ROOT_DIR/observability/provisioning/dashboards/logs-dashboard.json"
ALERT_RULES="$ROOT_DIR/observability/prometheus/rules/alerts.yml"
PROMTAIL_CFG="$ROOT_DIR/observability/promtail/config.yml"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "缺少命令: $1" >&2
    exit 1
  fi
}

need_cmd rg
need_cmd jq
need_cmd docker
need_cmd curl

run_static_checks() {
  echo "[1/6] 规则语法校验 (promtool)"
  docker run --rm --entrypoint promtool \
    -v "$ROOT_DIR/observability/prometheus/rules:/rules:ro" \
    prom/prometheus:latest check rules /rules/alerts.yml >/dev/null

  echo "[2/6] Promtail 配置语法校验"
  docker run --rm --entrypoint promtail \
    -v "$PROMTAIL_CFG:/etc/promtail/config.yml:ro" \
    grafana/promtail:3.3.1 -check-syntax -config.file=/etc/promtail/config.yml >/dev/null

  echo "[3/6] 清理旧指标残留"
  if rg -n "worker_integrity_backfills_total" \
    "$ROOT_DIR/datainjector/worker/internal" \
    "$ALERT_RULES" \
    "$WORKER_DASHBOARD" \
    "$ROOT_DIR/observability/agent/README.md" >/tmp/worker_obs_legacy_hits.txt; then
    cat /tmp/worker_obs_legacy_hits.txt >&2
    echo "发现旧指标残留，门禁失败" >&2
    exit 1
  fi

  echo "[4/6] Worker Dashboard 新指标接入校验"
  local required_metrics=(
    "worker_integrity_backfill_result_total"
    "worker_integrity_backfill_sessions_inflight"
    "worker_integrity_backfill_pending_duration_seconds"
    "worker_integrity_backfill_schedule_dedup_total"
    "worker_integrity_backfill_enqueue_latency_seconds"
    "worker_integrity_backfill_compensation_backlog"
    "worker_task_stage_total"
    "worker_websocket_drops_total"
  )
  for metric in "${required_metrics[@]}"; do
    rg -q "$metric" "$WORKER_DASHBOARD" || {
      echo "Dashboard 缺少指标: $metric" >&2
      exit 1
    }
  done

  echo "[5/6] Logs Dashboard 维度检索校验"
  for key in event role_id error_class session_key cmd_id; do
    rg -q "$key" "$LOGS_DASHBOARD" || {
      echo "Logs Dashboard 缺少检索维度: $key" >&2
      exit 1
    }
  done

  echo "[6/6] JSON 文件合法性"
  jq empty "$WORKER_DASHBOARD" >/dev/null
  jq empty "$LOGS_DASHBOARD" >/dev/null

  echo "静态门禁通过"
}

query_prometheus() {
  local query="$1"
  curl -fsS --get "$PROM_URL/api/v1/query" \
    --data-urlencode "query=$query"
}

query_loki() {
  local query="$1"
  curl -fsS --get "$LOKI_URL/loki/api/v1/query" \
    --data-urlencode "query=$query"
}

run_runtime_checks() {
  echo "[runtime 1/3] Worker up 稳定性检查"
  query_prometheus 'max(min_over_time(up{job="worker"}[5m]))' \
    | jq -e '.status=="success" and (.data.result | length) > 0 and ((.data.result[0].value[1] | tonumber) >= 1)' >/dev/null

  echo "[runtime 2/3] Backfill 闭环指标可观测性检查"
  query_prometheus 'sum(increase(worker_integrity_backfill_result_total[10m]))' \
    | jq -e '.status=="success"' >/dev/null
  query_prometheus 'sum(max_over_time(worker_integrity_backfill_sessions_inflight[10m]))' \
    | jq -e '.status=="success"' >/dev/null

  echo "[runtime 3/3] Worker 日志维度过滤检查"
  query_loki 'sum(count_over_time({service="datainjector-worker",event=~".+",role_id=~".+",error_class=~".+"}[5m]))' \
    | jq -e '.status=="success"' >/dev/null

  echo "运行时门禁通过"
}

run_static_checks

if [[ "$RUN_RUNTIME_CHECKS" == "1" ]]; then
  run_runtime_checks
else
  echo "跳过运行时门禁（设置 RUN_RUNTIME_CHECKS=1 可启用）"
fi

echo "worker_observability_cutover_gate: PASS"

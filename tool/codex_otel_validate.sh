#!/usr/bin/env bash
set -euo pipefail

# Codex OTel 接入验收脚本
# 目标：验证关键事件可达，并可选验证“导出失败不阻塞主流程”。

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/config/infrastructure/env/docker.env}"
AUDIT_FILE="${AUDIT_FILE:-$ROOT_DIR/runtime/data/otel-audit/codex-otel-events.jsonl}"
SIMULATE_EXPORTER_FAILURE="false"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

OTLP_HTTP_PORT="${OTEL_COLLECTOR_OTLP_HTTP_PORT:-4318}"
COLLECTOR_HEALTH_PORT="${OTEL_COLLECTOR_HEALTH_PORT:-13133}"
COLLECTOR_METRICS_PORT="${OTEL_COLLECTOR_METRICS_PORT:-8888}"

usage() {
  cat <<USAGE
Usage: ./tool/codex_otel_validate.sh [--simulate-exporter-failure]

Options:
  --simulate-exporter-failure   临时停止 loki，验证 Collector 仍能 200 接收 OTLP 请求
USAGE
}

metric_value() {
  local metric_name="$1"
  curl -fsS "http://127.0.0.1:${COLLECTOR_METRICS_PORT}/metrics" \
    | awk -v m="$metric_name" '$1 ~ "^"m"($|\\{)" {print $2; exit}'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --simulate-exporter-failure)
      SIMULATE_EXPORTER_FAILURE="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "未知参数: $1" >&2
      usage
      exit 1
      ;;
  esac
done

mkdir -p "$(dirname "$AUDIT_FILE")"

# 1) 健康检查
curl -fsS "http://127.0.0.1:${COLLECTOR_HEALTH_PORT}/" >/dev/null

echo "[validate] Collector 健康检查通过"

before_lines=0
if [[ -f "$AUDIT_FILE" ]]; then
  before_lines="$(wc -l < "$AUDIT_FILE" | tr -d ' ')"
fi

# 2) 发送一次最小 OTLP 日志请求
payload_file="$(mktemp)"
cat > "$payload_file" <<JSON
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "codex-cli"}},
          {"key": "session_id", "value": {"stringValue": "validate-session"}}
        ]
      },
      "scopeLogs": [
        {
          "scope": {"name": "codex.validate"},
          "logRecords": [
            {
              "timeUnixNano": "$(($(date +%s) * 1000000000))",
              "body": {"stringValue": "{\"event\":\"session_start\",\"tool_name\":\"validate\",\"outcome\":\"ok\"}"},
              "attributes": [
                {"key": "event.name", "value": {"stringValue": "session_start"}}
              ]
            }
          ]
        }
      ]
    }
  ]
}
JSON

http_code="$(curl -sS -o /tmp/codex_otel_validate_resp.txt -w '%{http_code}' -X POST \
  -H 'Content-Type: application/json' \
  --data-binary @"$payload_file" \
  "http://127.0.0.1:${OTLP_HTTP_PORT}/v1/logs")"
rm -f "$payload_file"

if [[ "$http_code" != "200" ]]; then
  echo "[validate] OTLP 请求失败，HTTP=$http_code" >&2
  exit 1
fi

echo "[validate] OTLP 事件接收成功（HTTP 200）"

# 3) 审计文件确认事件已落地
found="false"
for _ in {1..8}; do
  after_lines=0
  if [[ -f "$AUDIT_FILE" ]]; then
    after_lines="$(wc -l < "$AUDIT_FILE" | tr -d ' ')"
  fi
  if (( after_lines > before_lines )); then
    found="true"
    break
  fi
  sleep 1
done

if [[ "$found" != "true" ]]; then
  echo "[validate] 审计文件未观察到新增事件: $AUDIT_FILE" >&2
  exit 1
fi

echo "[validate] 审计落盘确认成功"

# 4) 可选：导出失败不阻塞验证
if [[ "$SIMULATE_EXPORTER_FAILURE" == "true" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "[validate] 缺少 docker，无法执行失败模拟" >&2
    exit 1
  fi

  before_fail="$(metric_value otelcol_exporter_send_failed_log_records || echo 0)"
  echo "[validate] 停止 Loki，模拟下游不可用"
  docker stop obs-loki >/dev/null

  trap 'docker start obs-loki >/dev/null 2>&1 || true' EXIT

  payload_file="$(mktemp)"
  cat > "$payload_file" <<JSON
{
  "resourceLogs": [
    {
      "resource": {"attributes": [{"key":"service.name","value":{"stringValue":"codex-cli"}}]},
      "scopeLogs": [
        {
          "scope": {"name": "codex.validate"},
          "logRecords": [
            {
              "timeUnixNano": "$(($(date +%s) * 1000000000))",
              "body": {"stringValue": "{\"event\":\"tool_error\",\"tool_name\":\"validate\",\"outcome\":\"error\"}"}
            }
          ]
        }
      ]
    }
  ]
}
JSON

  http_code="$(curl -sS -o /tmp/codex_otel_validate_resp2.txt -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    --data-binary @"$payload_file" \
    "http://127.0.0.1:${OTLP_HTTP_PORT}/v1/logs")"
  rm -f "$payload_file"

  if [[ "$http_code" != "200" ]]; then
    echo "[validate] 下游故障场景下 OTLP 请求未返回 200，HTTP=$http_code" >&2
    exit 1
  fi

  sleep 2
  after_fail="$(metric_value otelcol_exporter_send_failed_log_records || echo 0)"
  echo "[validate] 导出失败计数: before=${before_fail} after=${after_fail}"

  docker start obs-loki >/dev/null
  trap - EXIT

  echo "[validate] 失败模拟通过：Collector 可接收事件且导出失败可观测"
fi

echo "[validate] 全部检查通过"

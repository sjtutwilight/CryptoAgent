#!/opt/homebrew/bin/bash
set -euo pipefail

# 给 monitor_event_push.sh 用的 codex 包装器
# 透传所有 codex 参数，并在网络异常/命令失败时自动切换出口 IP 后重试。

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "$SCRIPT_DIR/run_with_vpn_failover.sh" \
  --max-attempts "${CODEX_VPN_MAX_ATTEMPTS:-4}" \
  --retry-sleep "${CODEX_VPN_RETRY_SLEEP_SECONDS:-3}" \
  --check-timeout "${CODEX_VPN_CHECK_TIMEOUT_SECONDS:-6}" \
  --switch-cooldown "${CODEX_VPN_SWITCH_COOLDOWN_SECONDS:-20}" \
  --check-url "${CODEX_VPN_CHECK_URL_1:-https://api.binance.com/api/v3/ping}" \
  --check-url "${CODEX_VPN_CHECK_URL_2:-https://fapi.binance.com/fapi/v1/ping}" \
  --switch-cmd "${VPN_SWITCH_CMD:-}" \
  -- codex "$@"

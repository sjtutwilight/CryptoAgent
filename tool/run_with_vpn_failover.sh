#!/opt/homebrew/bin/bash
set -euo pipefail

# 通用失败切 IP 重试包装器：
# 1) 先做网络连通性检测
# 2) 执行目标命令
# 3) 命令失败时执行 VPN_SWITCH_CMD 切换出口 IP 后重试
#
# 示例：
# VPN_SWITCH_CMD="networksetup -setairportpower en0 off; sleep 1; networksetup -setairportpower en0 on" \
#   tool/run_with_vpn_failover.sh --check-url https://api.binance.com/api/v3/ping -- codex exec -m gpt-5.3-codex "hello"

MAX_ATTEMPTS="${MAX_ATTEMPTS:-4}"
RETRY_SLEEP_SECONDS="${RETRY_SLEEP_SECONDS:-3}"
CHECK_TIMEOUT_SECONDS="${CHECK_TIMEOUT_SECONDS:-6}"
SWITCH_COOLDOWN_SECONDS="${SWITCH_COOLDOWN_SECONDS:-20}"
VPN_SWITCH_CMD="${VPN_SWITCH_CMD:-}"
CHECK_URLS=()

usage() {
  cat <<'EOF'
用法：
  run_with_vpn_failover.sh [options] -- <command> [args...]

选项：
  --max-attempts N        最大尝试次数，默认读取 MAX_ATTEMPTS(默认4)
  --retry-sleep SEC       重试前等待秒数，默认读取 RETRY_SLEEP_SECONDS(默认3)
  --check-timeout SEC     连通性检测超时秒数，默认读取 CHECK_TIMEOUT_SECONDS(默认6)
  --switch-cooldown SEC   两次切换 IP 最小间隔秒数，默认读取 SWITCH_COOLDOWN_SECONDS(默认20)
  --switch-cmd CMD        切换 IP 命令，默认读取 VPN_SWITCH_CMD
  --check-url URL         可重复指定，检测 URL（可选）

环境变量：
  VPN_SWITCH_CMD          必填（如果要自动切换），例如：
                          VPN_SWITCH_CMD="mihomo -d ~/.config/mihomo profile switch us-node-2"
EOF
}

now_ts() {
  date +%s
}

last_switch_file() {
  local root
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  printf '%s' "$root/runtime/data/vpn_switch.last"
}

network_ok() {
  local url
  if (( ${#CHECK_URLS[@]} == 0 )); then
    return 0
  fi
  for url in "${CHECK_URLS[@]}"; do
    if ! curl -fsS -m "$CHECK_TIMEOUT_SECONDS" "$url" >/dev/null 2>&1; then
      return 1
    fi
  done
  return 0
}

switch_vpn_if_allowed() {
  local f now last=0
  if [[ -z "$VPN_SWITCH_CMD" ]]; then
    echo "[vpn-failover] 未配置 VPN_SWITCH_CMD，跳过切换" >&2
    return 1
  fi

  f="$(last_switch_file)"
  mkdir -p "$(dirname "$f")"
  now="$(now_ts)"
  if [[ -f "$f" ]]; then
    last="$(cat "$f" 2>/dev/null || echo 0)"
  fi
  [[ "$last" =~ ^[0-9]+$ ]] || last=0
  if (( now - last < SWITCH_COOLDOWN_SECONDS )); then
    echo "[vpn-failover] 距离上次切换不足 ${SWITCH_COOLDOWN_SECONDS}s，跳过切换" >&2
    return 1
  fi

  echo "[vpn-failover] 执行切换命令: $VPN_SWITCH_CMD" >&2
  set +e
  bash -lc "$VPN_SWITCH_CMD"
  local rc=$?
  set -e
  if (( rc != 0 )); then
    echo "[vpn-failover] 切换命令失败 rc=$rc" >&2
    return 1
  fi
  echo "$now" > "$f"
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-attempts)
      MAX_ATTEMPTS="${2:-}"; shift 2 ;;
    --retry-sleep)
      RETRY_SLEEP_SECONDS="${2:-}"; shift 2 ;;
    --check-timeout)
      CHECK_TIMEOUT_SECONDS="${2:-}"; shift 2 ;;
    --switch-cooldown)
      SWITCH_COOLDOWN_SECONDS="${2:-}"; shift 2 ;;
    --switch-cmd)
      VPN_SWITCH_CMD="${2:-}"; shift 2 ;;
    --check-url)
      CHECK_URLS+=("${2:-}"); shift 2 ;;
    --help|-h)
      usage; exit 0 ;;
    --)
      shift
      break ;;
    *)
      echo "[vpn-failover] 未知参数: $1" >&2
      usage
      exit 2 ;;
  esac
done

if [[ $# -eq 0 ]]; then
  echo "[vpn-failover] 缺少待执行命令" >&2
  usage
  exit 2
fi

if ! [[ "$MAX_ATTEMPTS" =~ ^[0-9]+$ ]] || (( MAX_ATTEMPTS < 1 )); then
  echo "[vpn-failover] --max-attempts 必须是正整数" >&2
  exit 2
fi

if ! [[ "$RETRY_SLEEP_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "[vpn-failover] --retry-sleep 必须是非负整数" >&2
  exit 2
fi

attempt=1
while (( attempt <= MAX_ATTEMPTS )); do
  if ! network_ok; then
    echo "[vpn-failover] 网络检测失败，尝试切换出口 IP (attempt=$attempt/$MAX_ATTEMPTS)" >&2
    switch_vpn_if_allowed || true
    sleep "$RETRY_SLEEP_SECONDS"
  fi

  set +e
  "$@"
  rc=$?
  set -e
  if (( rc == 0 )); then
    exit 0
  fi

  echo "[vpn-failover] 命令失败 rc=$rc (attempt=$attempt/$MAX_ATTEMPTS)" >&2
  switch_vpn_if_allowed || true
  if (( attempt < MAX_ATTEMPTS )); then
    sleep "$RETRY_SLEEP_SECONDS"
  fi
  attempt=$((attempt + 1))
done

echo "[vpn-failover] 已达到最大重试次数: $MAX_ATTEMPTS" >&2
exit 1

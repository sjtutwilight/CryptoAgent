#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage: ./tool/test.sh <command> [args...]

Commands:
  list
    列出所有可用的测试场景

  scenario:run <name> [--stages=stage1,stage2] [--env=local]
    运行指定场景
    --stages: 可选，只运行指定的 stages（逗号分隔）
    --env: 环境，默认 local

  scenario:all [--env=local]
    运行所有场景

  stage:list <scenario>
    列出指定场景的所有 stages

  stage:run <scenario> <stage_name> [--env=local]
    单独运行某个场景的某个 stage

Examples:
  # 列出所有场景
  ./tool/test.sh list

  # 运行完整场景
  ./tool/test.sh scenario:run binance_kline

  # 只运行 infra 和 ingress stages
  ./tool/test.sh scenario:run binance_kline --stages=infra,ingress

  # 列出场景的所有 stages
  ./tool/test.sh stage:list binance_kline

  # 单独运行 ingress stage
  ./tool/test.sh stage:run binance_kline ingress

  # 运行所有场景
  ./tool/test.sh scenario:all
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "❌ 缺少命令: $1" >&2
    exit 1
  fi
}

list_scenarios() {
  local path
  for path in "$ROOT_DIR/automation/test/scenarios/"*.py; do
    local name
    name="$(basename "$path" .py)"
    if [[ "$name" == "__init__" ]]; then
      continue
    fi
    echo "$name"
  done
}

list_stages() {
  local scenario="$1"
  python3 -c "
import sys
sys.path.insert(0, '$ROOT_DIR')
from automation.test.scenarios.${scenario} import build_scenario
scenario = build_scenario()
for stage in scenario.stages:
    print(stage.name)
"
}

run_scenario() {
  local scenario="$1"
  shift
  python3 "$ROOT_DIR/automation/test/tools/run_scenario.py" "$scenario" "$@"
}

run_stage() {
  local scenario="$1"
  local stage_name="$2"
  shift 2
  python3 "$ROOT_DIR/automation/test/tools/run_scenario.py" "$scenario" --stages="$stage_name" "$@"
}

main() {
  require_cmd docker
  require_cmd python3

  local cmd="${1:-}"
  if [[ -z "$cmd" ]]; then
    usage
    exit 1
  fi

  case "$cmd" in
    list|--list)
      list_scenarios
      ;;
    
    scenario:run)
      if [[ $# -lt 2 ]]; then
        echo "❌ 缺少场景名称" >&2
        usage
        exit 1
      fi
      shift
      run_scenario "$@"
      ;;
    
    scenario:all|all)
      shift
      local scenario
      for scenario in $(list_scenarios); do
        echo "🚀 运行场景: $scenario"
        run_scenario "$scenario" "$@" || echo "⚠️  场景 $scenario 失败"
      done
      ;;
    
    stage:list)
      if [[ $# -lt 2 ]]; then
        echo "❌ 缺少场景名称" >&2
        usage
        exit 1
      fi
      list_stages "$2"
      ;;
    
    stage:run)
      if [[ $# -lt 3 ]]; then
        echo "❌ 缺少场景名称或 stage 名称" >&2
        usage
        exit 1
      fi
      shift
      run_stage "$@"
      ;;
    
    -h|--help|help)
      usage
      ;;
    
    *)
      echo "❌ 未知命令: $cmd" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"

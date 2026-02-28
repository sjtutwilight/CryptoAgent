#!/usr/bin/env bash
set -euo pipefail

# 关键路径说明：统一质量门禁入口，串行执行 golangci-lint 与 go-arch-lint，
# 并输出标准化 JSON 结果用于 Codex/CI 消费。

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly WORKER_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DEFAULT_OUTPUT_DIR="${WORKER_ROOT}/runtime/quality_gate"
readonly DEFAULT_BASELINE_FILE="${WORKER_ROOT}/tools/quality/baseline.json"
readonly DEFAULT_VERSION_FILE="${WORKER_ROOT}/tools/quality/tool_versions.env"
readonly DEFAULT_GOLANGCI_CONFIG="${WORKER_ROOT}/.golangci.yml"
readonly DEFAULT_ARCH_CONFIG="${WORKER_ROOT}/.go-arch-lint.yml"

MODE="check"
OUTPUT_DIR="${DEFAULT_OUTPUT_DIR}"
BASELINE_FILE="${DEFAULT_BASELINE_FILE}"
VERSION_FILE="${DEFAULT_VERSION_FILE}"
CURRENT_FILE=""
SUMMARY_FILE=""

usage() {
  cat <<'USAGE'
用法:
  tools/quality_gate.sh [--mode check|update-baseline] [--output-dir DIR] [--baseline-file FILE] [--current-file FILE] [--summary-file FILE]

参数:
  --mode             check(默认): 校验并阻断新增违规；update-baseline: 生成/更新基线文件
  --output-dir       运行产物目录（默认: runtime/quality_gate）
  --baseline-file    基线文件路径（默认: tools/quality/baseline.json）
  --current-file     跳过工具执行，直接使用指定 current JSON（用于回归测试）
  --summary-file     汇总结果 JSON 路径（默认: <output-dir>/summary.json）
  -h, --help         显示帮助
USAGE
}

log_stage() {
  printf '[quality-gate] %s\n' "$*"
}

strip_ansi() {
  sed -E 's/\x1B\[[0-9;]*[mK]//g'
}

normalize_version() {
  echo "$1" | sed -E 's/^v//' | sed -E 's/[^0-9.].*$//'
}

version_ge() {
  local current="$1"
  local minimum="$2"
  [[ "$(printf '%s\n%s\n' "${minimum}" "${current}" | sort -V | head -n 1)" == "${minimum}" ]]
}

check_cmd() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

ensure_tools() {
  if [[ ! -f "${VERSION_FILE}" ]]; then
    echo "版本约束文件不存在: ${VERSION_FILE}" >&2
    exit 2
  fi

  # shellcheck disable=SC1090
  source "${VERSION_FILE}"

  local golangci_bin="golangci-lint"
  local arch_bin="go-arch-lint"

  if ! check_cmd "${golangci_bin}" && [[ -x "${HOME}/go/bin/golangci-lint" ]]; then
    golangci_bin="${HOME}/go/bin/golangci-lint"
  fi
  if ! check_cmd "${arch_bin}" && [[ -x "${HOME}/go/bin/go-arch-lint" ]]; then
    arch_bin="${HOME}/go/bin/go-arch-lint"
  fi

  if [[ ! -x "$(command -v "${golangci_bin}" 2>/dev/null || true)" && ! -x "${golangci_bin}" ]]; then
    echo "未找到 golangci-lint，请先安装后重试" >&2
    exit 2
  fi
  if [[ ! -x "$(command -v "${arch_bin}" 2>/dev/null || true)" && ! -x "${arch_bin}" ]]; then
    echo "未找到 go-arch-lint，请先安装后重试" >&2
    exit 2
  fi

  GOLANGCI_BIN="${golangci_bin}"
  GO_ARCH_LINT_BIN="${arch_bin}"

  local golangci_version_raw arch_version_raw golangci_version arch_version
  golangci_version_raw="$(${GOLANGCI_BIN} version | head -n 1)"
  arch_version_raw="$(${GO_ARCH_LINT_BIN} version | strip_ansi | head -n 1)"

  golangci_version="$(normalize_version "$(echo "${golangci_version_raw}" | sed -n 's/.*version \([0-9][0-9.]*\).*/\1/p')")"
  arch_version="$(normalize_version "$(echo "${arch_version_raw}" | sed -n 's/.*v\([0-9][0-9.]*\).*/\1/p')")"

  if [[ -z "${golangci_version}" || -z "${arch_version}" ]]; then
    echo "无法解析工具版本，请检查工具安装" >&2
    exit 2
  fi

  if ! version_ge "${golangci_version}" "${GOLANGCI_LINT_MIN_VERSION}"; then
    echo "golangci-lint 版本过低: ${golangci_version} < ${GOLANGCI_LINT_MIN_VERSION}" >&2
    exit 2
  fi
  if ! version_ge "${arch_version}" "${GO_ARCH_LINT_MIN_VERSION}"; then
    echo "go-arch-lint 版本过低: ${arch_version} < ${GO_ARCH_LINT_MIN_VERSION}" >&2
    exit 2
  fi

  log_stage "工具版本检查通过: golangci-lint=${golangci_version}, go-arch-lint=${arch_version}"
}

run_tools() {
  local golangci_json="$1"
  local golangci_text="$2"
  local arch_json="$3"

  local golangci_exit=0
  local arch_exit=0

  log_stage "执行 golangci-lint..."
  set +e
  (
    cd "${WORKER_ROOT}"
    "${GOLANGCI_BIN}" run \
      -c "${DEFAULT_GOLANGCI_CONFIG}" \
      --issues-exit-code 1 \
      --output.json.path "${golangci_json}" \
      --output.text.path "${golangci_text}"
  )
  golangci_exit=$?
  set -e

  log_stage "执行 go-arch-lint..."
  set +e
  (
    cd "${WORKER_ROOT}"
    "${GO_ARCH_LINT_BIN}" check \
      --arch-file "${DEFAULT_ARCH_CONFIG}" \
      --project-path "${WORKER_ROOT}" \
      --json \
      --output-json-one-line > "${arch_json}"
  )
  arch_exit=$?
  set -e

  GOLANGCI_EXIT="${golangci_exit}"
  ARCH_EXIT="${arch_exit}"
}

build_current_json() {
  local golangci_json="$1"
  local arch_json="$2"
  local current_json="$3"

  local golangci_violations arch_violations

  golangci_violations="$(jq -c '
    [ .Issues[]? |
      {
        id: (.FromLinter + "|" + .Pos.Filename + ":" + (.Pos.Line|tostring) + ":" + (.Pos.Column|tostring) + "|" + .Text),
        file: .Pos.Filename,
        line: (.Pos.Line // 0),
        column: (.Pos.Column // 0),
        rule: .FromLinter,
        message: .Text
      }
    ]
  ' "${golangci_json}" 2>/dev/null || echo '[]')"

  arch_violations="$(jq -c '
    (
      (.Payload.ArchWarningsDeps // []) | map({
        id: ("deps|" + .ComponentName + "|" + .FileRelativePath + "|" + .ResolvedImportName + "|" + ((.Reference.Line // 0)|tostring)),
        file: .FileRelativePath,
        line: (.Reference.Line // 0),
        column: 0,
        rule: "dependency",
        message: ("组件 " + .ComponentName + " 非法依赖 " + .ResolvedImportName)
      })
    ) + (
      (.Payload.ArchWarningsDeepScan // []) | map({
        id: ("deep|" + .FileRelativePath + "|" + .ImportName),
        file: .FileRelativePath,
        line: 0,
        column: 0,
        rule: "deep_scan",
        message: ("深度扫描告警: " + .ImportName)
      })
    ) + (
      (.Payload.ArchWarningsNotMatched // []) | map({
        id: ("not_matched|" + .FileRelativePath),
        file: .FileRelativePath,
        line: 0,
        column: 0,
        rule: "not_matched",
        message: "文件未匹配到任何组件"
      })
    )
  ' "${arch_json}" 2>/dev/null || echo '[]')"

  jq -n \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg golangci_report "${golangci_json}" \
    --arg arch_report "${arch_json}" \
    --argjson golangci_exit "${GOLANGCI_EXIT}" \
    --argjson arch_exit "${ARCH_EXIT}" \
    --argjson golangci_violations "${golangci_violations}" \
    --argjson arch_violations "${arch_violations}" \
    '
    {
      schema_version: 1,
      generated_at: $generated_at,
      tools: {
        "golangci-lint": {
          exit_code: $golangci_exit,
          report_path: $golangci_report,
          violations: $golangci_violations
        },
        "go-arch-lint": {
          exit_code: $arch_exit,
          report_path: $arch_report,
          violations: $arch_violations
        }
      }
    }
    ' > "${current_json}"
}

update_baseline() {
  local current_json="$1"
  mkdir -p "$(dirname "${BASELINE_FILE}")"

  jq '
    {
      schema_version: .schema_version,
      generated_at: .generated_at,
      tools: {
        "golangci-lint": { violations: .tools["golangci-lint"].violations },
        "go-arch-lint": { violations: .tools["go-arch-lint"].violations }
      }
    }
  ' "${current_json}" > "${BASELINE_FILE}"

  log_stage "已更新基线文件: ${BASELINE_FILE}"
}

compare_with_baseline() {
  local current_json="$1"
  local delta_json="$2"

  if [[ ! -f "${BASELINE_FILE}" ]]; then
    echo "基线文件不存在: ${BASELINE_FILE}，请先执行 --mode update-baseline" >&2
    exit 2
  fi

  jq -n \
    --argfile base "${BASELINE_FILE}" \
    --argfile cur "${current_json}" \
    '
    def new_items($tool):
      ($base.tools[$tool].violations // []) as $base_items
      | ($cur.tools[$tool].violations // []) as $cur_items
      | ($base_items | map(.id)) as $base_ids
      | [ $cur_items[] as $item | select(($base_ids | index($item.id)) == null) | $item ];

    {
      schema_version: 1,
      generated_at: $cur.generated_at,
      tools: {
        "golangci-lint": new_items("golangci-lint"),
        "go-arch-lint": new_items("go-arch-lint")
      }
    }
    ' > "${delta_json}"
}

write_summary() {
  local current_json="$1"
  local delta_json="$2"
  local summary_file="$3"
  local policy_status="$4"

  jq -n \
    --arg mode "${MODE}" \
    --arg baseline "${BASELINE_FILE}" \
    --arg policy_status "${policy_status}" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argfile current "${current_json}" \
    --argfile delta "${delta_json}" \
    '
    {
      schema_version: 1,
      mode: $mode,
      generated_at: $generated_at,
      baseline_file: $baseline,
      policy_status: $policy_status,
      tools: [
        {
          name: "golangci-lint",
          exit_code: $current.tools["golangci-lint"].exit_code,
          report_path: $current.tools["golangci-lint"].report_path,
          total_violations: ($current.tools["golangci-lint"].violations | length),
          new_violations: ($delta.tools["golangci-lint"] | length),
          violations: $delta.tools["golangci-lint"]
        },
        {
          name: "go-arch-lint",
          exit_code: $current.tools["go-arch-lint"].exit_code,
          report_path: $current.tools["go-arch-lint"].report_path,
          total_violations: ($current.tools["go-arch-lint"].violations | length),
          new_violations: ($delta.tools["go-arch-lint"] | length),
          violations: $delta.tools["go-arch-lint"]
        }
      ]
    }
    ' > "${summary_file}"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --mode)
        MODE="$2"
        shift 2
        ;;
      --output-dir)
        OUTPUT_DIR="$2"
        shift 2
        ;;
      --baseline-file)
        BASELINE_FILE="$2"
        shift 2
        ;;
      --current-file)
        CURRENT_FILE="$2"
        shift 2
        ;;
      --summary-file)
        SUMMARY_FILE="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "未知参数: $1" >&2
        usage
        exit 2
        ;;
    esac
  done

  if [[ "${MODE}" != "check" && "${MODE}" != "update-baseline" ]]; then
    echo "--mode 仅支持 check 或 update-baseline" >&2
    exit 2
  fi
}

main() {
  parse_args "$@"

  mkdir -p "${OUTPUT_DIR}"

  local golangci_json="${OUTPUT_DIR}/golangci.json"
  local golangci_text="${OUTPUT_DIR}/golangci.txt"
  local arch_json="${OUTPUT_DIR}/go-arch-lint.json"
  local current_json="${OUTPUT_DIR}/current.json"
  local delta_json="${OUTPUT_DIR}/delta.json"

  if [[ -z "${SUMMARY_FILE}" ]]; then
    SUMMARY_FILE="${OUTPUT_DIR}/summary.json"
  fi

  if [[ -n "${CURRENT_FILE}" ]]; then
    log_stage "使用外部 current 文件: ${CURRENT_FILE}"
    cp "${CURRENT_FILE}" "${current_json}"
  else
    ensure_tools
    run_tools "${golangci_json}" "${golangci_text}" "${arch_json}"
    build_current_json "${golangci_json}" "${arch_json}" "${current_json}"
  fi

  local golangci_exit arch_exit new_total policy_status
  golangci_exit="$(jq -r '.tools["golangci-lint"].exit_code // 0' "${current_json}")"
  arch_exit="$(jq -r '.tools["go-arch-lint"].exit_code // 0' "${current_json}")"

  if [[ "${MODE}" == "update-baseline" ]]; then
    if (( golangci_exit > 1 || arch_exit > 1 )); then
      policy_status="tool_error"
      jq -n '{schema_version:1, generated_at: (now | todate), tools:{"golangci-lint":[],"go-arch-lint":[]}}' > "${delta_json}"
      write_summary "${current_json}" "${delta_json}" "${SUMMARY_FILE}" "${policy_status}"
      echo "工具执行异常（exit_code > 1），已中止基线更新。详情见 ${SUMMARY_FILE}" >&2
      exit 2
    fi
    update_baseline "${current_json}"
    # 基线更新完成后，delta 视为 0 结果输出。
    jq -n '{schema_version:1, generated_at: (now | todate), tools:{"golangci-lint":[],"go-arch-lint":[]}}' > "${delta_json}"
    policy_status="baseline_updated"
    write_summary "${current_json}" "${delta_json}" "${SUMMARY_FILE}" "${policy_status}"
    log_stage "基线更新完成，汇总文件: ${SUMMARY_FILE}"
    exit 0
  fi

  compare_with_baseline "${current_json}" "${delta_json}"
  new_total="$(jq '[.tools["golangci-lint"], .tools["go-arch-lint"]] | map(length) | add' "${delta_json}")"

  if (( golangci_exit > 1 || arch_exit > 1 )); then
    policy_status="tool_error"
    write_summary "${current_json}" "${delta_json}" "${SUMMARY_FILE}" "${policy_status}"
    echo "工具执行异常（exit_code > 1），详情见 ${SUMMARY_FILE}" >&2
    exit 2
  fi

  if (( new_total > 0 )); then
    policy_status="failed_new_violations"
    write_summary "${current_json}" "${delta_json}" "${SUMMARY_FILE}" "${policy_status}"
    echo "检测到基线外新增违规（${new_total}），详情见 ${SUMMARY_FILE}" >&2
    exit 1
  fi

  policy_status="passed"
  write_summary "${current_json}" "${delta_json}" "${SUMMARY_FILE}" "${policy_status}"
  log_stage "质量门禁通过（仅命中基线或零违规）。汇总文件: ${SUMMARY_FILE}"
}

main "$@"

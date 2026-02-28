#!/usr/bin/env bash
set -euo pipefail

# 关键验证说明：该脚本覆盖门禁策略的最小回归场景，
# 验证“成功路径 / lint 新违规 / 架构新违规”三类结果是否符合预期。

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly WORKER_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly QUALITY_GATE="${SCRIPT_DIR}/quality_gate.sh"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

BASELINE_FILE="${TMP_DIR}/baseline.json"

cat > "${BASELINE_FILE}" <<'JSON'
{
  "schema_version": 1,
  "generated_at": "2026-02-28T00:00:00Z",
  "tools": {
    "golangci-lint": {
      "violations": [
        {
          "id": "staticcheck|internal/role/role.go:10:2|old issue",
          "file": "internal/role/role.go",
          "line": 10,
          "column": 2,
          "rule": "staticcheck",
          "message": "old issue"
        }
      ]
    },
    "go-arch-lint": {
      "violations": [
        {
          "id": "deps|handler|/internal/handler/handler.go|github.com/twilight-labs/dataplatform/datainjector/worker/internal/sink|42",
          "file": "/internal/handler/handler.go",
          "line": 42,
          "column": 0,
          "rule": "dependency",
          "message": "历史架构违规"
        }
      ]
    }
  }
}
JSON

run_case() {
  local name="$1"
  local current_file="$2"
  local expected_exit="$3"

  local output_dir="${TMP_DIR}/${name}"
  mkdir -p "${output_dir}"

  set +e
  "${QUALITY_GATE}" \
    --mode check \
    --baseline-file "${BASELINE_FILE}" \
    --current-file "${current_file}" \
    --output-dir "${output_dir}" \
    --summary-file "${output_dir}/summary.json"
  local actual_exit=$?
  set -e

  if [[ "${actual_exit}" != "${expected_exit}" ]]; then
    echo "[regression] 用例 ${name} 失败：期望退出码=${expected_exit}，实际=${actual_exit}" >&2
    exit 1
  fi

  echo "[regression] 用例 ${name} 通过（exit=${actual_exit}）"
}

cat > "${TMP_DIR}/current_success.json" <<'JSON'
{
  "schema_version": 1,
  "generated_at": "2026-02-28T00:00:00Z",
  "tools": {
    "golangci-lint": {
      "exit_code": 1,
      "report_path": "mock/golangci.json",
      "violations": [
        {
          "id": "staticcheck|internal/role/role.go:10:2|old issue",
          "file": "internal/role/role.go",
          "line": 10,
          "column": 2,
          "rule": "staticcheck",
          "message": "old issue"
        }
      ]
    },
    "go-arch-lint": {
      "exit_code": 1,
      "report_path": "mock/go-arch-lint.json",
      "violations": [
        {
          "id": "deps|handler|/internal/handler/handler.go|github.com/twilight-labs/dataplatform/datainjector/worker/internal/sink|42",
          "file": "/internal/handler/handler.go",
          "line": 42,
          "column": 0,
          "rule": "dependency",
          "message": "历史架构违规"
        }
      ]
    }
  }
}
JSON

cat > "${TMP_DIR}/current_lint_new.json" <<'JSON'
{
  "schema_version": 1,
  "generated_at": "2026-02-28T00:00:00Z",
  "tools": {
    "golangci-lint": {
      "exit_code": 1,
      "report_path": "mock/golangci.json",
      "violations": [
        {
          "id": "staticcheck|internal/role/role.go:10:2|old issue",
          "file": "internal/role/role.go",
          "line": 10,
          "column": 2,
          "rule": "staticcheck",
          "message": "old issue"
        },
        {
          "id": "gocyclo|internal/role/manager.go:99:1|new issue",
          "file": "internal/role/manager.go",
          "line": 99,
          "column": 1,
          "rule": "gocyclo",
          "message": "new issue"
        }
      ]
    },
    "go-arch-lint": {
      "exit_code": 1,
      "report_path": "mock/go-arch-lint.json",
      "violations": [
        {
          "id": "deps|handler|/internal/handler/handler.go|github.com/twilight-labs/dataplatform/datainjector/worker/internal/sink|42",
          "file": "/internal/handler/handler.go",
          "line": 42,
          "column": 0,
          "rule": "dependency",
          "message": "历史架构违规"
        }
      ]
    }
  }
}
JSON

cat > "${TMP_DIR}/current_arch_new.json" <<'JSON'
{
  "schema_version": 1,
  "generated_at": "2026-02-28T00:00:00Z",
  "tools": {
    "golangci-lint": {
      "exit_code": 1,
      "report_path": "mock/golangci.json",
      "violations": [
        {
          "id": "staticcheck|internal/role/role.go:10:2|old issue",
          "file": "internal/role/role.go",
          "line": 10,
          "column": 2,
          "rule": "staticcheck",
          "message": "old issue"
        }
      ]
    },
    "go-arch-lint": {
      "exit_code": 1,
      "report_path": "mock/go-arch-lint.json",
      "violations": [
        {
          "id": "deps|handler|/internal/handler/handler.go|github.com/twilight-labs/dataplatform/datainjector/worker/internal/sink|42",
          "file": "/internal/handler/handler.go",
          "line": 42,
          "column": 0,
          "rule": "dependency",
          "message": "历史架构违规"
        },
        {
          "id": "deps|caller|/internal/caller/native_call_http.go|github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler|66",
          "file": "/internal/caller/native_call_http.go",
          "line": 66,
          "column": 0,
          "rule": "dependency",
          "message": "新增架构违规"
        }
      ]
    }
  }
}
JSON

run_case "success" "${TMP_DIR}/current_success.json" 0
run_case "lint_new_violation" "${TMP_DIR}/current_lint_new.json" 1
run_case "arch_new_violation" "${TMP_DIR}/current_arch_new.json" 1

echo "[regression] 最小回归验证全部通过"

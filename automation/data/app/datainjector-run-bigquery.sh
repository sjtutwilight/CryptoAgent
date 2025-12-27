#!/usr/bin/env bash
# BigQuery 数据拉取脚本
# 用途：通过 Worker 的 bigquery-results-batch role 拉取 BigQuery 查询结果

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
WORKER_DIR="$ROOT_DIR/datainjector/worker"
source "$ROOT_DIR/automation/infra/load-infra-env.sh"
source "$ROOT_DIR/automation/infra/app-deps.sh"
BASE_CONFIG_FILE="${BASE_CONFIG_FILE:-$WORKER_DIR/configs/base.yaml}"
ROLE_REGISTRY_FILE="${ROLE_REGISTRY_FILE:-$WORKER_DIR/configs/config.yaml}"

# 默认参数（可通过环境变量覆盖）
PROJECT_ID="${BIGQUERY_PROJECT_ID:-ethereal-cache-481306-e5}"
JOB_ID="${BIGQUERY_JOB_ID:-US.bquxjob_2a1e2f66_19b2be99427}"
TASK_ID="${BIGQUERY_TASK_ID:-bigquery-$(date +%s)}"
SERVICE_ACCOUNT_JSON="${GOOGLE_APPLICATION_CREDENTIALS:-}"
MANUAL_ACCESS_TOKEN="${GOOGLE_CLOUD_API_KEY:-}"

MODE="${1:-test}"  # test | run

SA_TMP_FILE=""
cleanup_sa() {
  if [[ -n "$SA_TMP_FILE" && -f "$SA_TMP_FILE" ]]; then
    rm -f "$SA_TMP_FILE"
  fi
}
trap cleanup_sa EXIT

# service account 路径优先级:
# 1) 显式 SERVICE_ACCOUNT_JSON
# 2) BIGQUERY_SERVICE_ACCOUNT_JSON
# 3) GOOGLE_APPLICATION_CREDENTIALS
# 4) BIGQUERY_SERVICE_ACCOUNT_JSON_B64 / BIGQUERY_SERVICE_ACCOUNT_JSON_CONTENT
# 5) config/infrastructure/credentials/bigquery-service-account.json
if [[ -z "${SERVICE_ACCOUNT_JSON:-}" && -n "${BIGQUERY_SERVICE_ACCOUNT_JSON:-}" ]]; then
  SERVICE_ACCOUNT_JSON="$BIGQUERY_SERVICE_ACCOUNT_JSON"
fi

DEFAULT_SA_PATH="$ROOT_DIR/config/infrastructure/credentials/bigquery-service-account.json"

write_inline_sa_file() {
  local content="$1"
  SA_TMP_FILE="$(mktemp /tmp/bigquery-sa.XXXXXX.json)"
  printf '%s' "$content" >"$SA_TMP_FILE"
  chmod 600 "$SA_TMP_FILE"
  SERVICE_ACCOUNT_JSON="$SA_TMP_FILE"
}

write_base64_sa_file() {
  local b64="$1"
  SA_TMP_FILE="$(mktemp /tmp/bigquery-sa.XXXXXX.json)"
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$SA_TMP_FILE" "$b64" <<'PY'
import base64, pathlib, sys
target, data = sys.argv[1], sys.argv[2]
pathlib.Path(target).write_bytes(base64.b64decode(data))
PY
  else
    printf '%s' "$b64" | base64 --decode >"$SA_TMP_FILE" 2>/dev/null || printf '%s' "$b64" | base64 -d >"$SA_TMP_FILE"
  fi
  chmod 600 "$SA_TMP_FILE"
  SERVICE_ACCOUNT_JSON="$SA_TMP_FILE"
}

if [[ -z "${SERVICE_ACCOUNT_JSON:-}" && -n "${BIGQUERY_SERVICE_ACCOUNT_JSON_B64:-}" ]]; then
  write_base64_sa_file "$BIGQUERY_SERVICE_ACCOUNT_JSON_B64"
fi

if [[ -z "${SERVICE_ACCOUNT_JSON:-}" && -n "${BIGQUERY_SERVICE_ACCOUNT_JSON_CONTENT:-}" ]]; then
  write_inline_sa_file "$BIGQUERY_SERVICE_ACCOUNT_JSON_CONTENT"
fi

if [[ -z "${SERVICE_ACCOUNT_JSON:-}" && -f "$DEFAULT_SA_PATH" ]]; then
  SERVICE_ACCOUNT_JSON="$DEFAULT_SA_PATH"
fi

# 从服务账号 JSON 生成 access token
generate_access_token() {
    local sa_file="$1"
    
    if [ ! -f "$sa_file" ]; then
        echo "❌ 服务账号文件不存在: $sa_file" >&2
        return 1
    fi
    
    # 使用 gcloud 或 Python 生成 token
    if command -v gcloud &> /dev/null; then
        gcloud auth activate-service-account --key-file="$sa_file" --quiet 2>/dev/null
        gcloud auth print-access-token 2>/dev/null
    elif command -v python3 &> /dev/null; then
        python3 -c "
import json
import time
import sys
from urllib.request import Request, urlopen
from urllib.parse import urlencode
import base64
import hashlib

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend
    import jwt
except ImportError:
    print('需要安装: pip install PyJWT cryptography', file=sys.stderr)
    sys.exit(1)

with open('$sa_file') as f:
    sa = json.load(f)

now = int(time.time())
payload = {
    'iss': sa['client_email'],
    'sub': sa['client_email'],
    'aud': 'https://oauth2.googleapis.com/token',
    'iat': now,
    'exp': now + 3600,
    'scope': 'https://www.googleapis.com/auth/bigquery.readonly'
}

token = jwt.encode(payload, sa['private_key'], algorithm='RS256')

data = urlencode({
    'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
    'assertion': token
}).encode()

req = Request('https://oauth2.googleapis.com/token', data=data)
resp = urlopen(req)
result = json.loads(resp.read())
print(result['access_token'])
" 2>/dev/null
    else
        echo "❌ 需要 gcloud 或 python3 (with PyJWT/cryptography) 来生成 token" >&2
        return 1
    fi
}

echo "============================================"
echo "BigQuery 数据拉取"
echo "============================================"
echo "Mode:       $MODE"
echo "Project:    $PROJECT_ID"
echo "Job ID:     $JOB_ID"
echo "Task ID:    $TASK_ID"
echo ""

# 1. 配置验证
echo "[1/4] 验证配置..."
if [ ! -f "$BASE_CONFIG_FILE" ]; then
    echo "❌ 配置文件不存在: $BASE_CONFIG_FILE"
    exit 1
fi

if [ ! -f "$ROLE_REGISTRY_FILE" ]; then
    echo "❌ Role 注册文件不存在: $ROLE_REGISTRY_FILE"
    exit 1
fi

# 检查 YAML 语法
if command -v python3 &> /dev/null; then
    python3 -c "import yaml; yaml.safe_load(open('$BASE_CONFIG_FILE'))" 2>&1
    python3 -c "import yaml; yaml.safe_load(open('$ROLE_REGISTRY_FILE'))" 2>&1
    if [ $? -ne 0 ]; then
        echo "❌ YAML 语法错误"
        exit 1
    fi
fi

# 检查 role 是否存在
if ! grep -q "role_id: \"bigquery-results-batch\"" "$ROLE_REGISTRY_FILE"; then
    echo "❌ 未找到 bigquery-results-batch role"
    exit 1
fi
echo "✅ 配置验证通过"
echo ""

# 2. 认证检查
echo "[2/4] 检查认证..."
if [ -n "$SERVICE_ACCOUNT_JSON" ]; then
    echo "使用服务账号 JSON: $SERVICE_ACCOUNT_JSON"
    if [ "$MODE" = "run" ]; then
        echo "生成 Access Token..."
        ACCESS_TOKEN=$(generate_access_token "$SERVICE_ACCOUNT_JSON")
        if [ -z "$ACCESS_TOKEN" ]; then
            echo "❌ 无法生成 Access Token"
            exit 1
        fi
        export GOOGLE_CLOUD_API_KEY="$ACCESS_TOKEN"
        export GOOGLE_APPLICATION_CREDENTIALS="$SERVICE_ACCOUNT_JSON"
        echo "✅ Access Token 已生成（有效期 1 小时）"
    fi
elif [ -n "$MANUAL_ACCESS_TOKEN" ]; then
    echo "✅ 使用预设的 GOOGLE_CLOUD_API_KEY"
else
    echo "⚠️  未设置认证信息"
    if [ "$MODE" = "run" ]; then
        echo "❌ 运行模式需要设置认证"
        echo "   方式1: export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account.json'"
        echo "   方式2: export GOOGLE_CLOUD_API_KEY='your-access-token'"
        exit 1
    fi
    echo "   在 test 模式下跳过"
fi
echo ""

if [ "$MODE" = "test" ]; then
    echo "[3/4] 测试模式 - 仅验证配置"
    echo "✅ 所有检查通过"
    echo ""
    echo "配置摘要："
    echo "  - Role ID:    bigquery-results-batch"
    echo "  - Endpoint:   https://bigquery.googleapis.com/bigquery/v2"
    echo "  - Path:       /projects/{project_id}/queries/{job_id}"
    echo "  - 分页参数:   pageToken (10000 条/页)"
    echo "  - 输出目录:   runtime/data/bigquery/results/{project_id}/{job_id}"
    echo ""
    echo "下一步："
    echo "  1. 设置 API Key: export GOOGLE_CLOUD_API_KEY='your-key'"
    echo "  2. 运行拉取: ./automation/data/data.sh datainjector:run:bigquery"
    exit 0
fi

# 3. 启动 Worker (run 模式)
echo "[3/4] 启动 Worker..."
ensure_service_group datainjector_worker

# 检查 worker 可执行文件
if [ ! -f "$WORKER_DIR/worker" ]; then
    echo "⚠️  Worker 可执行文件不存在，尝试编译..."
    cd "$WORKER_DIR"
    if [ -f "go.mod" ]; then
        go build -o worker cmd/main.go
        if [ $? -ne 0 ]; then
            echo "❌ Worker 编译失败"
            exit 1
        fi
        echo "✅ Worker 编译成功"
    else
        echo "❌ 找不到 go.mod，无法编译"
        exit 1
    fi
fi

# 后台启动 Worker
echo "启动 Worker 进程（后台）..."
cd "$WORKER_DIR"
./worker -config="$BASE_CONFIG_FILE" --roles="$ROLE_REGISTRY_FILE" > /tmp/bigquery-worker.log 2>&1 &
WORKER_PID=$!
echo "Worker PID: $WORKER_PID"

# 等待 Worker 启动
sleep 3

# 检查 Worker 是否还在运行
if ! kill -0 $WORKER_PID 2>/dev/null; then
    echo "❌ Worker 启动失败，查看日志:"
    tail -20 /tmp/bigquery-worker.log
    exit 1
fi
echo "✅ Worker 启动成功"
echo ""

# 4. 发送任务
echo "[4/4] 发送任务到 Kafka..."
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS_LOCAL:-localhost:9092}"
KAFKA_TOPIC="${KAFKA_BIGQUERY_TOPIC:-batch.tasks}"
if ! nc -z "${KAFKA_BOOTSTRAP%%:*}" "${KAFKA_BOOTSTRAP##*:}" 2>/dev/null; then
    echo "❌ Kafka 未运行 (${KAFKA_BOOTSTRAP})"
    kill $WORKER_PID 2>/dev/null
    exit 1
fi

# 构建任务 JSON（单行，避免被拆分成多条消息）
TASK_JSON='{"task_id":"'"$TASK_ID"'","project_id":"'"$PROJECT_ID"'","job_id":"'"$JOB_ID"'"}'

echo "任务内容:"
echo "$TASK_JSON"
echo ""

# 发送到 Kafka（使用 printf 确保单行）
printf '%s\n' "$TASK_JSON" | docker exec -i "${KAFKA_CONTAINER_NAME}" \
    kafka-console-producer \
    --broker-list "$KAFKA_BOOTSTRAP" \
    --topic "$KAFKA_TOPIC"

if [ $? -eq 0 ]; then
    echo "✅ 任务已发送到 Kafka topic: $KAFKA_TOPIC"
else
    echo "❌ 任务发送失败"
    kill $WORKER_PID 2>/dev/null
    exit 1
fi

echo ""
echo "============================================"
echo "数据拉取进行中..."
echo "============================================"
echo ""
echo "监控选项:"
echo "  - Worker 日志:   tail -f /tmp/bigquery-worker.log"
echo "  - 输出目录:      ls -lh runtime/data/bigquery/results/$PROJECT_ID/$JOB_ID/"
echo "  - 停止 Worker:   kill $WORKER_PID"
echo ""

# 等待任务完成（检查输出目录）
OUTPUT_DIR="runtime/data/bigquery/results/$PROJECT_ID/$JOB_ID"
MANIFEST_FILE="$OUTPUT_DIR/manifest.json"

echo "等待数据拉取完成（最多等待 5 分钟）..."
for i in {1..60}; do
    if [ -f "$MANIFEST_FILE" ]; then
        echo ""
        echo "✅ 数据拉取完成！"
        echo ""
        echo "输出文件:"
        ls -lh "$OUTPUT_DIR/"
        echo ""
        echo "Manifest 内容:"
        cat "$MANIFEST_FILE" | python3 -m json.tool 2>/dev/null || cat "$MANIFEST_FILE"
        
        # 停止 Worker
        kill $WORKER_PID 2>/dev/null
        exit 0
    fi
    
    if ! kill -0 $WORKER_PID 2>/dev/null; then
        echo ""
        echo "❌ Worker 进程已停止，可能发生错误"
        echo "查看日志: tail -20 /tmp/bigquery-worker.log"
        exit 1
    fi
    
    printf "."
    sleep 5
done

echo ""
echo "⚠️  超时：5 分钟内未完成"
echo "Worker 仍在运行，PID: $WORKER_PID"
echo "请手动检查进度或停止 Worker"
exit 1

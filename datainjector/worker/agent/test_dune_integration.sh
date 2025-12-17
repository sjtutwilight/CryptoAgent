#!/bin/bash

# Dune Token Holders 集成测试脚本
# 测试用例：以太坊 Chainlink Token (LINK)

set -e

echo "=========================================="
echo "Dune Token Holders 集成测试"
echo "=========================================="

# 配置
export DUNE_SIM_API_KEY="sim_ZmoRtMDsmW0WWNeTUpFr2hjU8pIHEaAY"
CHAIN_ID="1"
TOKEN_ADDRESS="0x514910771af9ca656af840dff83e8264ecf986ca"
TASK_ID="test-chainlink-$(date +%s)"

echo ""
echo "测试参数:"
echo "  Chain ID: $CHAIN_ID"
echo "  Token Address: $TOKEN_ADDRESS"
echo "  Task ID: $TASK_ID"
echo ""

# 1. 检查 Kafka 是否运行
echo "[1/5] 检查 Kafka 服务..."
if ! nc -z localhost 9092 2>/dev/null; then
    echo "错误: Kafka 未运行，请先启动 docker-compose"
    exit 1
fi
echo "✓ Kafka 服务正常"

# 2. 启动 Worker（后台运行）
echo ""
echo "[2/5] 启动 Worker..."
cd "$(dirname "$0")"

# 确保输出目录存在
OUTPUT_DIR="/tmp/dune/token-holders/${CHAIN_ID}/${TOKEN_ADDRESS}"
mkdir -p "$OUTPUT_DIR"

# 启动 Worker
go run ./cmd/worker --config ./configs/dune_token_holders.yaml > /tmp/dune_worker.log 2>&1 &
WORKER_PID=$!
echo "✓ Worker 已启动 (PID: $WORKER_PID)"

# 等待 Worker 初始化
sleep 3

# 3. 发送测试任务到 Kafka
echo ""
echo "[3/5] 发送测试任务到 Kafka..."

TASK_MESSAGE=$(cat <<EOF
{
  "taskId": "$TASK_ID",
  "taskType": "batch_file",
  "payload": {
    "task_id": "$TASK_ID",
    "chain_id": "$CHAIN_ID",
    "address": "$TOKEN_ADDRESS"
  },
  "metadata": {
    "datasourceId": "DuneSim",
    "test": true
  }
}
EOF
)

echo "$TASK_MESSAGE" | docker exec -i $(docker ps -qf "name=kafka") \
    kafka-console-producer.sh \
    --broker-list localhost:9092 \
    --topic batch.tasks

echo "✓ 任务已发送到 Kafka"

# 4. 监控任务执行
echo ""
echo "[4/5] 监控任务执行..."
echo "输出目录: $OUTPUT_DIR"
echo ""

# 等待任务完成（最多等待 5 分钟）
TIMEOUT=300
ELAPSED=0
INTERVAL=5

while [ $ELAPSED -lt $TIMEOUT ]; do
    # 检查是否生成了 manifest.json
    if [ -f "$OUTPUT_DIR/manifest.json" ]; then
        echo ""
        echo "✓ 任务完成！"
        break
    fi
    
    # 显示进度
    if [ -f "$OUTPUT_DIR/.cursor.json" ]; then
        CURSOR_INFO=$(cat "$OUTPUT_DIR/.cursor.json" | jq -r '.total_records // 0')
        echo -ne "\r  已拉取记录数: $CURSOR_INFO"
    else
        echo -ne "\r  等待任务开始... (${ELAPSED}s)"
    fi
    
    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

echo ""

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "错误: 任务超时"
    kill $WORKER_PID 2>/dev/null || true
    exit 1
fi

# 5. 验证结果
echo ""
echo "[5/5] 验证结果..."

# 检查 manifest.json
if [ ! -f "$OUTPUT_DIR/manifest.json" ]; then
    echo "错误: manifest.json 不存在"
    kill $WORKER_PID 2>/dev/null || true
    exit 1
fi

echo ""
echo "Manifest 内容:"
cat "$OUTPUT_DIR/manifest.json" | jq '.'

# 提取关键信息
TOTAL_RECORDS=$(cat "$OUTPUT_DIR/manifest.json" | jq -r '.total_records')
TOTAL_FILES=$(cat "$OUTPUT_DIR/manifest.json" | jq -r '.total_files')
STATUS=$(cat "$OUTPUT_DIR/manifest.json" | jq -r '.status')

echo ""
echo "=========================================="
echo "测试结果:"
echo "  状态: $STATUS"
echo "  总记录数: $TOTAL_RECORDS"
echo "  总文件数: $TOTAL_FILES"
echo "  输出目录: $OUTPUT_DIR"
echo "=========================================="

# 检查状态
if [ "$STATUS" != "completed" ]; then
    echo ""
    echo "错误: 任务状态非 completed"
    kill $WORKER_PID 2>/dev/null || true
    exit 1
fi

# 检查记录数
if [ "$TOTAL_RECORDS" -eq 0 ]; then
    echo ""
    echo "警告: 总记录数为 0"
fi

# 列出生成的文件
echo ""
echo "生成的文件:"
ls -lh "$OUTPUT_DIR"

# 显示第一个文件的前几行
FIRST_FILE=$(ls "$OUTPUT_DIR"/holders_*.json 2>/dev/null | head -1)
if [ -n "$FIRST_FILE" ]; then
    echo ""
    echo "第一个文件的前 3 条记录:"
    head -3 "$FIRST_FILE" | jq '.'
fi

# 清理
echo ""
echo "清理 Worker 进程..."
kill $WORKER_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true

echo ""
echo "✓ 集成测试完成！"
echo ""
echo "提示: 查看完整日志: tail -f /tmp/dune_worker.log"


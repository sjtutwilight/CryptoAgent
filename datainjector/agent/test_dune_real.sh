#!/bin/bash

# Dune Token Holders 真实接口测试脚本
# 使用真实的 Chainlink Token 数据

set -e

echo "=========================================="
echo "Dune Token Holders 真实接口测试"
echo "=========================================="

# 配置
export DUNE_SIM_API_KEY="sim_ZmoRtMDsmW0WWNeTUpFr2hjU8pIHEaAY"
CHAIN_ID="1"
TOKEN_ADDRESS="0x514910771af9ca656af840dff83e8264ecf986ca"
TASK_ID="dune-chainlink-$(date +%s)"

echo ""
echo "测试参数:"
echo "  Chain ID: $CHAIN_ID (Ethereum Mainnet)"
echo "  Token: Chainlink (LINK)"
echo "  Address: $TOKEN_ADDRESS"
echo "  Task ID: $TASK_ID"
echo "  API Key: ${DUNE_SIM_API_KEY:0:20}..."
echo ""

# 1. 检查 Docker 服务
echo "[1/6] 检查 Docker 服务..."
if ! docker ps | grep -q kafka; then
    echo "错误: Kafka 未运行，请先启动: docker-compose up -d"
    exit 1
fi
echo "✓ Kafka 服务正常"

# 2. 确保输出目录存在
OUTPUT_DIR="/tmp/dune/token-holders/${CHAIN_ID}/${TOKEN_ADDRESS}"
echo ""
echo "[2/6] 准备输出目录..."
mkdir -p "$OUTPUT_DIR"
# 清理旧数据
rm -f "$OUTPUT_DIR"/*.json 2>/dev/null || true
echo "✓ 输出目录: $OUTPUT_DIR"

# 3. 启动 Worker（使用独立配置）
echo ""
echo "[3/6] 启动 Worker..."
cd "$(cd "$(dirname "$0")/.." && pwd)/worker"

# 后台启动 Worker
nohup go run ./cmd/worker --config ./configs/dune_token_holders.yaml > /tmp/dune_worker.log 2>&1 &
WORKER_PID=$!
echo "✓ Worker 已启动 (PID: $WORKER_PID)"

# 等待 Worker 初始化
echo "  等待 Worker 初始化..."
sleep 5

# 检查 Worker 是否还在运行
if ! kill -0 $WORKER_PID 2>/dev/null; then
    echo "错误: Worker 启动失败"
    echo "查看日志: tail -50 /tmp/dune_worker.log"
    tail -50 /tmp/dune_worker.log
    exit 1
fi
echo "✓ Worker 初始化完成"

# 4. 发送测试任务到 Kafka
echo ""
echo "[4/6] 发送测试任务到 Kafka..."

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
    "test": true,
    "token_name": "Chainlink",
    "token_symbol": "LINK"
  }
}
EOF
)

echo "$TASK_MESSAGE" | docker exec -i $(docker ps -qf "name=kafka") \
    kafka-console-producer.sh \
    --broker-list localhost:9092 \
    --topic batch.tasks

echo "✓ 任务已发送到 Kafka (topic: batch.tasks)"

# 5. 监控任务执行
echo ""
echo "[5/6] 监控任务执行..."
echo "输出目录: $OUTPUT_DIR"
echo ""

# 等待任务完成（最多等待 10 分钟）
TIMEOUT=600
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
        CURSOR_INFO=$(cat "$OUTPUT_DIR/.cursor.json" 2>/dev/null | jq -r '.total_records // 0' 2>/dev/null || echo "0")
        NEXT_OFFSET=$(cat "$OUTPUT_DIR/.cursor.json" 2>/dev/null | jq -r '.next_offset // "null"' 2>/dev/null || echo "null")
        FILES_COUNT=$(ls "$OUTPUT_DIR"/holders_*.json 2>/dev/null | wc -l | tr -d ' ')
        echo -ne "\r  进度: ${CURSOR_INFO} 条记录, ${FILES_COUNT} 个文件, offset: ${NEXT_OFFSET:0:20}... (${ELAPSED}s)"
    else
        echo -ne "\r  等待任务开始... (${ELAPSED}s)"
    fi
    
    # 检查 Worker 是否还在运行
    if ! kill -0 $WORKER_PID 2>/dev/null; then
        echo ""
        echo "警告: Worker 进程已退出"
        echo "查看日志: tail -50 /tmp/dune_worker.log"
        tail -50 /tmp/dune_worker.log
        exit 1
    fi
    
    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

echo ""

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "错误: 任务超时 (${TIMEOUT}s)"
    echo "查看日志: tail -100 /tmp/dune_worker.log"
    kill $WORKER_PID 2>/dev/null || true
    exit 1
fi

# 6. 验证结果
echo ""
echo "[6/6] 验证结果..."

# 检查 manifest.json
if [ ! -f "$OUTPUT_DIR/manifest.json" ]; then
    echo "错误: manifest.json 不存在"
    kill $WORKER_PID 2>/dev/null || true
    exit 1
fi

echo ""
echo "=========================================="
echo "Manifest 内容:"
echo "=========================================="
cat "$OUTPUT_DIR/manifest.json" | jq '.'

# 提取关键信息
TOTAL_RECORDS=$(cat "$OUTPUT_DIR/manifest.json" | jq -r '.total_records')
TOTAL_FILES=$(cat "$OUTPUT_DIR/manifest.json" | jq -r '.total_files')
STATUS=$(cat "$OUTPUT_DIR/manifest.json" | jq -r '.status')
TOKEN_ADDRESS_FROM_MANIFEST=$(cat "$OUTPUT_DIR/manifest.json" | jq -r '.custom_fields.token_address // "N/A"')
FIRST_HOLDER=$(cat "$OUTPUT_DIR/manifest.json" | jq -r '.custom_fields.first_holder_address // "N/A"')

echo ""
echo "=========================================="
echo "测试结果汇总:"
echo "=========================================="
echo "  状态: $STATUS"
echo "  Token 地址: $TOKEN_ADDRESS_FROM_MANIFEST"
echo "  总记录数: $TOTAL_RECORDS"
echo "  总文件数: $TOTAL_FILES"
echo "  第一个持有者: ${FIRST_HOLDER:0:20}..."
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
echo "生成的文件列表:"
ls -lh "$OUTPUT_DIR"

# 显示第一个文件的前几行
FIRST_FILE=$(ls "$OUTPUT_DIR"/holders_*.json 2>/dev/null | head -1)
if [ -n "$FIRST_FILE" ]; then
    echo ""
    echo "=========================================="
    echo "数据样例 (前 3 条记录):"
    echo "=========================================="
    head -3 "$FIRST_FILE" | jq '.'
fi

# 统计信息
echo ""
echo "=========================================="
echo "数据统计:"
echo "=========================================="
TOTAL_LINES=$(cat "$OUTPUT_DIR"/holders_*.json 2>/dev/null | wc -l | tr -d ' ')
TOTAL_SIZE=$(du -sh "$OUTPUT_DIR" | cut -f1)
echo "  总行数: $TOTAL_LINES"
echo "  总大小: $TOTAL_SIZE"

# 清理
echo ""
echo "清理 Worker 进程..."
kill $WORKER_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true

echo ""
echo "=========================================="
echo "✓ 真实接口测试完成！"
echo "=========================================="
echo ""
echo "提示:"
echo "  - 查看完整日志: tail -f /tmp/dune_worker.log"
echo "  - 查看数据文件: ls -lh $OUTPUT_DIR"
echo "  - 查看 Manifest: cat $OUTPUT_DIR/manifest.json | jq '.'"
echo ""

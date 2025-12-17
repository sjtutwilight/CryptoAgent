#!/usr/bin/env bash
# 快速检查 dwd_dex_swap topic 的消息数量和最新消息
# 用于快速验证 DexSwapDwdJob 的输出

set -euo pipefail

KAFKA_BOOTSTRAP="localhost:9092"
TOPIC="dwd_dex_swap"

echo "=========================================="
echo "检查 Kafka Topic: $TOPIC"
echo "=========================================="
echo ""

# 检查 Kafka 是否可用
if ! nc -z localhost 9092 2>/dev/null; then
    echo "❌ Kafka 服务未运行 (localhost:9092)"
    exit 1
fi

echo "✓ Kafka 服务运行中"
echo ""

# 检查 topic 是否存在
if docker exec crypto-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "^${TOPIC}$"; then
    echo "✓ Topic '${TOPIC}' 存在"
    echo ""
    
    # 获取 topic 详情
    echo "Topic 详情:"
    docker exec crypto-kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --describe \
        --topic "${TOPIC}" 2>/dev/null
    echo ""
    
    # 统计消息数量
    echo "正在统计消息数量..."
    MESSAGE_COUNT=$(docker exec crypto-kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic "${TOPIC}" \
        --time -1 2>/dev/null | \
        awk -F':' '{sum += $3} END {print sum}')
    
    echo "📊 消息总数: ${MESSAGE_COUNT:-0}"
    echo ""
    
    # 显示最新的几条消息
    if [ "${MESSAGE_COUNT:-0}" -gt 0 ]; then
        echo "最新 5 条消息:"
        echo "----------------------------------------"
        docker exec crypto-kafka kafka-console-consumer \
            --bootstrap-server localhost:9092 \
            --topic "${TOPIC}" \
            --from-beginning \
            --max-messages 5 \
            --property print.timestamp=true \
            --property print.key=true \
            --property key.separator=" => " \
            --timeout-ms 5000 2>/dev/null | jq -c '.' 2>/dev/null || cat
        echo "----------------------------------------"
    else
        echo "⚠️  暂无消息"
    fi
else
    echo "❌ Topic '${TOPIC}' 不存在"
    echo ""
    echo "提示: 可能 Job 还未创建 topic,或者 Job 未启动"
fi

echo ""
echo "=========================================="
echo "DexSwapDwdJob 进程状态:"
echo "=========================================="
if ps aux | grep -v grep | grep -q "DexSwapDwdJob"; then
    echo "✓ DexSwapDwdJob 正在运行"
    ps aux | grep -v grep | grep "DexSwapDwdJob" | awk '{print "PID:", $2, "  内存:", $4"%", "  运行时间:", $10}'
else
    echo "❌ DexSwapDwdJob 未运行"
fi
echo ""


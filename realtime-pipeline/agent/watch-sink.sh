#!/usr/bin/env bash
# 观测 dwd_dex_swap topic 的输出
# 用于验证 DexSwapDwdJob 的 sink 是否正常工作

set -euo pipefail

KAFKA_BOOTSTRAP="localhost:9092"
TOPIC="dwd_dex_swap"

echo "=========================================="
echo "观测 Kafka Topic: $TOPIC"
echo "Kafka Broker: $KAFKA_BOOTSTRAP"
echo "=========================================="
echo ""

# 检查 Kafka 是否可用
if ! nc -z localhost 9092 2>/dev/null; then
    echo "错误: Kafka 服务未运行 (localhost:9092)"
    echo "请先启动 Kafka 服务"
    exit 1
fi

# 检查 topic 是否存在
if ! docker exec crypto-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "^${TOPIC}$"; then
    echo "警告: Topic '${TOPIC}' 不存在"
    echo "创建 topic..."
    docker exec crypto-kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --create \
        --topic "${TOPIC}" \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
    echo "Topic 创建完成"
    echo ""
fi

echo "开始消费 topic (按 Ctrl+C 停止)..."
echo "=========================================="
echo ""

# 使用 kafka-console-consumer 消费消息
# --from-beginning: 从头开始消费
# --property print.timestamp=true: 打印时间戳
# --property print.key=true: 打印 key
docker exec -it crypto-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "${TOPIC}" \
    --from-beginning \
    --property print.timestamp=true \
    --property print.key=true \
    --property key.separator=" | " \
    2>/dev/null || true

echo ""
echo "消费结束"


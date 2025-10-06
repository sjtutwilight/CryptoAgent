#!/bin/bash

# WebSocket订阅测试脚本
# 测试场景：
# 1. WebSocket长连接订阅newHeads
# 2. 序列号缓冲和乱序处理
# 3. 数据持续流入Kafka

set -e

echo "=== Unified Worker WebSocket测试 ==="
echo ""

# 颜色定义
GREEN='\033[0.32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Kafka配置
KAFKA_BROKER="localhost:9092"
DATA_TOPIC="chain.ethereum.blocks"
FAILURE_TOPIC="worker.failures"

echo "步骤1: 检查MockDataProvider是否运行"
if ! curl -s http://localhost:8090/health > /dev/null 2>&1; then
    echo -e "${RED}✗ MockDataProvider未运行${NC}"
    echo "  请先启动: cd datasource/MockDataProvider && go run cmd/server/main.go"
    exit 1
fi
echo -e "${GREEN}✓ MockDataProvider运行中${NC}"
echo ""

echo "步骤2: 创建Kafka Topic"
docker exec -it crypto-kafka kafka-topics.sh --create --topic $DATA_TOPIC --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || echo "  Topic已存在"
docker exec -it crypto-kafka kafka-topics.sh --create --topic $FAILURE_TOPIC --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || echo "  Topic已存在"
echo -e "${GREEN}✓ Kafka Topic已准备${NC}"
echo ""

echo "步骤3: 下发WebSocket订阅任务到Kafka"
cat <<EOF | docker exec -i crypto-kafka kafka-console-producer.sh --broker-list localhost:9092 --topic worker.tasks
{
  "task_id": "test-ws-001",
  "task_type": "long_connection",
  "data_source_id": "mock-ethereum",
  "data_source": {
    "id": "mock-ethereum",
    "name": "Mock Ethereum Node",
    "type": "ethereum",
    "protocol": "websocket",
    "endpoint": {
      "url": "ws://localhost:8090/ws",
      "headers": {"User-Agent": "TwilightWorker/Test"},
      "timeout": 30
    },
    "rate_limit": {
      "requests_per_minute": 1200,
      "requests_per_second": 20,
      "burst_size": 50
    },
    "subscription": {
      "supported": true,
      "subscribe_method": "eth_subscribe",
      "topics": ["newHeads"],
      "params": ["newHeads"]
    }
  },
  "task_specific_config": {
    "subscription": {
      "topics": ["newHeads"]
    }
  },
  "sequence_field": "number",
  "output_topic": "$DATA_TOPIC",
  "report_to_control_plane": true,
  "retry_config": {
    "max_retries": 3,
    "backoff_base": 2,
    "backoff_max": 30
  }
}
EOF

echo -e "${GREEN}✓ 任务已下发${NC}"
echo ""

echo "步骤4: 启动Unified Worker"
echo -e "${YELLOW}提示: Worker将在前台运行，按Ctrl+C停止${NC}"
echo -e "${YELLOW}请在另一个终端运行以下命令查看Kafka输出:${NC}"
echo ""
echo -e "  ${YELLOW}# 查看数据输出${NC}"
echo -e "  docker exec -it crypto-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $DATA_TOPIC --from-beginning"
echo ""
echo -e "  ${YELLOW}# 查看失败报告${NC}"
echo -e "  docker exec -it crypto-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $FAILURE_TOPIC --from-beginning"
echo ""
echo "按Enter键启动Worker..."
read

./unified-worker -config configs/config.yaml

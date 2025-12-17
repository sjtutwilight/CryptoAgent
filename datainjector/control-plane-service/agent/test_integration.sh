#!/bin/bash

# 控制平面与HTTP Worker集成测试脚本
# 验证完整的闭环功能

echo "🚀 开始控制平面与HTTP Worker闭环测试"

# 配置
CONTROL_PLANE_URL="http://localhost:8083"
API_BASE="${CONTROL_PLANE_URL}/api/v1"
SYSTEM_BASE="${CONTROL_PLANE_URL}/system"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查服务状态
echo -e "\n${YELLOW}📊 检查系统状态${NC}"
curl -s "${SYSTEM_BASE}/health" | jq '.' || echo -e "${RED}❌ 控制平面服务未启动${NC}"

# 创建测试任务
echo -e "\n${YELLOW}📝 创建测试任务${NC}"
TASK_RESPONSE=$(curl -s -X POST "${API_BASE}/tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "dataSourceId": "ethereum-mainnet",
    "method": "GET",
    "params": {
      "limit": 10,
      "offset": 0
    },
    "cost": 1,
    "priority": 5,
    "headers": {
      "User-Agent": "CryptoDataIngestion/1.0"
    }
  }')

echo "任务创建响应:"
echo "$TASK_RESPONSE" | jq '.'

# 提取任务ID
TASK_ID=$(echo "$TASK_RESPONSE" | jq -r '.data.taskId')

if [ "$TASK_ID" = "null" ] || [ -z "$TASK_ID" ]; then
  echo -e "${RED}❌ 任务创建失败${NC}"
  exit 1
fi

echo -e "${GREEN}✅ 任务创建成功: $TASK_ID${NC}"

# 查询任务状态
echo -e "\n${YELLOW}🔍 查询任务状态${NC}"
sleep 2
curl -s "${API_BASE}/tasks/${TASK_ID}" | jq '.'

# 查看系统统计
echo -e "\n${YELLOW}📈 系统统计信息${NC}"
curl -s "${SYSTEM_BASE}/stats" | jq '.'

# 检查限流状态
echo -e "\n${YELLOW}🚦 检查限流状态${NC}"
curl -s "${SYSTEM_BASE}/rate-limit/ethereum-mainnet" | jq '.'

# 任务统计
echo -e "\n${YELLOW}📊 任务统计${NC}"
curl -s "${API_BASE}/tasks/statistics" | jq '.'

echo -e "\n${GREEN}🎉 集成测试完成！${NC}"

# 创建高频测试（验证限流）
echo -e "\n${YELLOW}⚡ 高频请求测试（验证限流机制）${NC}"
for i in {1..5}; do
  echo "发送第 $i 个请求..."
  curl -s -X POST "${API_BASE}/tasks" \
    -H "Content-Type: application/json" \
    -d '{
      "dataSourceId": "ethereum-mainnet",
      "method": "GET",
      "params": {"limit": 1},
      "cost": 10
    }' | jq '.message'
  sleep 1
done

echo -e "\n${GREEN}✅ 集成测试完成！检查日志以验证完整的闭环流程。${NC}"
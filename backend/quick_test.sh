#!/bin/bash
# Twilight Backend API 快速验证脚本

BASE_URL="http://localhost:8088/api"

echo "=== Twilight Backend API 快速验证 ==="
echo "服务地址: $BASE_URL"
echo

# 1. 检查服务状态
echo "1. 检查服务健康状态..."
health_response=$(curl -s "$BASE_URL/health" 2>/dev/null)
if [[ $? -eq 0 ]] && [[ -n "$health_response" ]]; then
  echo "✓ 服务正常运行"
  echo "  响应: $health_response"
else
  echo "✗ 服务连接失败 - 请确认应用已启动在端口8088"
  exit 1
fi
echo

# 2. 测试代币概览接口
echo "2. 测试代币概览接口..."
overview_response=$(curl -s "$BASE_URL/v1/tokens/1/overview" 2>/dev/null)
if [[ $? -eq 0 ]] && echo "$overview_response" | grep -q '"code"'; then
  echo "✓ 代币概览接口正常"
  # 尝试解析响应
  if command -v jq >/dev/null 2>&1; then
    echo "  代币符号: $(echo "$overview_response" | jq -r '.data.symbol // "N/A"')"
    echo "  响应码: $(echo "$overview_response" | jq -r '.code // "N/A"')"
  else
    echo "  原始响应: ${overview_response:0:200}..."
  fi
else
  echo "✗ 代币概览接口异常"
  echo "  响应: $overview_response"
fi
echo

# 3. 测试其他核心接口
endpoints=("price-chart" "trade-flow" "pnl" "distribution")
for endpoint in "${endpoints[@]}"; do
  echo "3. 测试 $endpoint 接口..."
  response=$(curl -s "$BASE_URL/v1/tokens/1/$endpoint" 2>/dev/null)
  if [[ $? -eq 0 ]] && echo "$response" | grep -q '"code"'; then
    if command -v jq >/dev/null 2>&1; then
      status_code=$(echo "$response" | jq -r '.code // "N/A"')
      echo "✓ $endpoint 接口响应正常 (状态码: $status_code)"
    else
      echo "✓ $endpoint 接口响应正常"
    fi
  else
    echo "✗ $endpoint 接口异常"
    echo "  响应: ${response:0:100}..."
  fi
done
echo

# 4. 测试账户接口
echo "4. 测试账户接口..."
account_response=$(curl -s "$BASE_URL/v1/accounts/1" 2>/dev/null)
if [[ $? -eq 0 ]] && echo "$account_response" | grep -q '"code"'; then
  echo "✓ 账户接口响应正常"
else
  echo "✗ 账户接口异常"
  echo "  响应: ${account_response:0:100}..."
fi
echo

# 5. 检查API文档
echo "5. 检查API文档可用性..."
swagger_response=$(curl -s "$BASE_URL/swagger-ui.html" 2>/dev/null)
if [[ $? -eq 0 ]]; then
  echo "✓ Swagger UI 可访问"
  echo "  文档地址: $BASE_URL/swagger-ui.html"
else
  echo "? Swagger UI 可能不可用"
fi
echo

# 6. 性能简单测试
echo "6. 简单性能测试..."
start_time=$(date +%s.%N 2>/dev/null || date +%s)
for i in {1..5}; do
  curl -s "$BASE_URL/v1/tokens/1/overview" >/dev/null 2>&1
done
end_time=$(date +%s.%N 2>/dev/null || date +%s)

if command -v bc >/dev/null 2>&1; then
  duration=$(echo "$end_time - $start_time" | bc 2>/dev/null)
  avg_time=$(echo "scale=3; $duration / 5" | bc 2>/dev/null)
  echo "✓ 5次请求完成，平均响应时间: ${avg_time}s"
else
  echo "✓ 5次请求完成"
fi
echo

echo "=== 验证完成 ==="
echo
echo "如果所有测试都通过，说明后端API运行正常！"
echo "可以使用以下命令进行更详细的测试："
echo "  curl -s '$BASE_URL/v1/tokens/1/overview' | jq ."
echo "  curl -s '$BASE_URL/swagger-ui.html'"
echo



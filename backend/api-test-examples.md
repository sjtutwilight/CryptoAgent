# Twilight Backend API 调用示例

## 服务基本信息

- **服务地址**: http://localhost:8088
- **API前缀**: `/api`
- **WebSocket端点**: `/ws`

## REST API 接口测试

### 1. 健康检查

```bash
# 检查服务状态
curl -X GET "http://localhost:8088/api/health"
```

### 2. 代币数据接口

#### 2.1 获取代币概览
```bash
# 获取代币ID为1的概览信息
curl -X GET "http://localhost:8088/api/v1/tokens/1/overview" \
  -H "Content-Type: application/json"
```

**响应示例**:
```json
{
  "code": 200,
  "message": "成功",
  "data": {
    "tokenId": 1,
    "symbol": "USDC",
    "name": "USDC",
    "chainName": "Ethereum",
    "age": 1250,
    "securityScore": 95,
    "description": "USDC是一个稳定币，在区块链生态中发挥重要作用。",
    "metrics": {
      "currentPrice": 1.0001,
      "fdv": 32000000000,
      "mcap": 31500000000,
      "liquidity": 850000000,
      "fdvMcapRatio": 1.02
    }
  }
}
```

#### 2.2 获取价格历史图表
```bash
# 获取24小时价格历史
curl -X GET "http://localhost:8088/api/v1/tokens/1/price-chart?timeRange=24h" \
  -H "Content-Type: application/json"

# 获取7天价格历史  
curl -X GET "http://localhost:8088/api/v1/tokens/1/price-chart?timeRange=7d" \
  -H "Content-Type: application/json"
```

#### 2.3 获取交易流数据
```bash
# 获取1小时交易流数据
curl -X GET "http://localhost:8088/api/v1/tokens/1/trade-flow?timeRange=1h" \
  -H "Content-Type: application/json"
```

**响应示例**:
```json
{
  "code": 200,
  "message": "成功", 
  "data": {
    "tokenId": 1,
    "timeRange": "1h",
    "tradeVolume": [
      {
        "tag": "all",
        "volumeUsd": 2500000,
        "buyVolumeUsd": 1300000,
        "sellVolumeUsd": 1200000,
        "txCount": 450
      }
    ],
    "netFlow": [
      {
        "tag": "smart_money",
        "tagName": "聪明钱",
        "netFlowUsd": 50000,
        "inflowUsd": 150000,
        "outflowUsd": 100000
      }
    ]
  }
}
```

#### 2.4 获取PnL数据
```bash
# 获取Top 50 PnL数据
curl -X GET "http://localhost:8088/api/v1/tokens/1/pnl?topLimit=50" \
  -H "Content-Type: application/json"
```

#### 2.5 获取代币分布数据
```bash
# 获取代币分布和Top 100持有者
curl -X GET "http://localhost:8088/api/v1/tokens/1/distribution?topHolderLimit=100" \
  -H "Content-Type: application/json"
```

#### 2.6 获取账户数据
```bash
# 获取账户ID为123的数据
curl -X GET "http://localhost:8088/api/v1/accounts/123" \
  -H "Content-Type: application/json"
```

### 3. 批量测试不同代币

```bash
# 测试多个代币ID
for token_id in 1 2 3 4 5; do
  echo "Testing token ID: $token_id"
  curl -s "http://localhost:8088/api/v1/tokens/$token_id/overview" | jq '.data.symbol // "Not found"'
done
```

## WebSocket 连接测试

### 使用JavaScript测试WebSocket

```javascript
// 连接WebSocket
const socket = new SockJS('http://localhost:8088/api/ws');
const stompClient = Stomp.over(socket);

stompClient.connect({}, function (frame) {
    console.log('Connected: ' + frame);
    
    // 订阅代币1的实时数据
    stompClient.subscribe('/topic/token/1/realtime', function (message) {
        console.log('Real-time data:', JSON.parse(message.body));
    });
    
    // 订阅价格更新
    stompClient.subscribe('/topic/token/1/price', function (message) {
        console.log('Price update:', JSON.parse(message.body));
    });
    
    // 发送订阅消息
    stompClient.send("/app/subscribe/token/1", {}, JSON.stringify({}));
});
```

### 使用curl测试WebSocket (需要websocat工具)

```bash
# 安装websocat
# brew install websocat  # macOS
# apt install websocat   # Ubuntu

# 连接WebSocket并监听实时数据
echo '["CONNECT\nAccept-version:1.1,1.0\nheart-beat:10000,10000\n\n\x00"]' | \
websocat ws://localhost:8088/api/ws/websocket
```

## 性能测试

### 并发测试

```bash
# 使用Apache Bench进行并发测试
ab -n 1000 -c 10 "http://localhost:8088/api/v1/tokens/1/overview"

# 使用curl进行简单的并发测试
for i in {1..10}; do
  curl -s "http://localhost:8088/api/v1/tokens/1/overview" &
done
wait
```

### 负载测试脚本

```bash
#!/bin/bash
# load_test.sh

BASE_URL="http://localhost:8088/api"
TOKENS=(1 2 3 4 5)
ENDPOINTS=("overview" "price-chart" "trade-flow" "pnl" "distribution")

echo "开始负载测试..."

for token in "${TOKENS[@]}"; do
  for endpoint in "${ENDPOINTS[@]}"; do
    echo "测试 /v1/tokens/$token/$endpoint"
    
    start_time=$(date +%s.%N)
    response=$(curl -s "$BASE_URL/v1/tokens/$token/$endpoint")
    end_time=$(date +%s.%N)
    
    duration=$(echo "$end_time - $start_time" | bc)
    status_code=$(echo "$response" | jq -r '.code // "error"')
    
    echo "  响应时间: ${duration}s, 状态码: $status_code"
  done
done

echo "负载测试完成"
```

## 错误处理测试

```bash
# 测试不存在的代币ID
curl -X GET "http://localhost:8088/api/v1/tokens/99999/overview"

# 测试无效的参数
curl -X GET "http://localhost:8088/api/v1/tokens/abc/overview"

# 测试超大的limit参数  
curl -X GET "http://localhost:8088/api/v1/tokens/1/pnl?topLimit=999999"
```

## 数据库连接测试

如果需要测试数据库连接：

```bash
# 检查应用日志中的数据库连接状态
tail -f logs/twilight-backend.log | grep -i "database\|connection\|sql"

# 或者查看actuator健康检查
curl -X GET "http://localhost:8088/api/actuator/health"
```

## API文档访问

```bash
# 访问Swagger UI文档
open "http://localhost:8088/api/swagger-ui.html"

# 获取OpenAPI JSON
curl -X GET "http://localhost:8088/api/api-docs"
```

## 监控指标

```bash
# 获取应用指标
curl -X GET "http://localhost:8088/api/actuator/metrics"

# 获取特定指标
curl -X GET "http://localhost:8088/api/actuator/metrics/http.server.requests"
```

## 常见问题排查

1. **连接被拒绝**: 检查应用是否启动在8088端口
2. **404错误**: 检查API路径是否正确，注意`/api`前缀
3. **500错误**: 检查数据库连接和日志文件
4. **WebSocket连接失败**: 确认SockJS和STOMP协议设置正确

## 快速验证脚本

```bash
#!/bin/bash
# quick_test.sh

echo "=== Twilight Backend API 快速验证 ==="

# 1. 检查服务状态
echo "1. 检查服务健康状态..."
health_response=$(curl -s "http://localhost:8088/api/health")
if [[ $? -eq 0 ]]; then
  echo "✓ 服务正常运行"
else
  echo "✗ 服务连接失败"
  exit 1
fi

# 2. 测试代币概览接口
echo "2. 测试代币概览接口..."
overview_response=$(curl -s "http://localhost:8088/api/v1/tokens/1/overview")
if echo "$overview_response" | jq -e '.code == 200' > /dev/null; then
  echo "✓ 代币概览接口正常"
else
  echo "✗ 代币概览接口异常"
fi

# 3. 测试其他核心接口
endpoints=("price-chart" "trade-flow" "pnl" "distribution")
for endpoint in "${endpoints[@]}"; do
  echo "3. 测试 $endpoint 接口..."
  response=$(curl -s "http://localhost:8088/api/v1/tokens/1/$endpoint")
  if echo "$response" | jq -e '.code' > /dev/null; then
    echo "✓ $endpoint 接口响应正常"
  else
    echo "✗ $endpoint 接口异常"
  fi
done

echo "=== 验证完成 ==="
```

使用方法：
```bash
chmod +x quick_test.sh
./quick_test.sh
```



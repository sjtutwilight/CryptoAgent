# HTTP Worker

## 概述

HTTP Worker是加密货币数据接入系统的核心组件，负责从Kafka消费HTTP任务，调用外部API获取数据，并将结果发送到相应的Kafka topic。

## 功能特性

### 核心功能
- **Kafka消费**: 从`http.tasks` topic消费HTTP任务
- **HTTP请求执行**: 支持GET/POST请求，自动处理JSON-RPC协议
- **限流控制**: 基于令牌桶的本地限流机制
- **连接池管理**: 按host:port隔离的HTTP连接池
- **结果输出**: 将成功的数据发送到数据topic，任务状态发送到状态topic
- **错误处理**: 支持可重试错误的识别和处理

### 技术特性
- **Go 1.20**: 使用现代Go语言特性
- **高并发**: 支持多协程并发处理任务
- **可配置**: 丰富的配置选项，支持运行时调整
- **可观测**: 详细的日志记录和状态监控

## 架构设计

### 模块结构
```
http-worker/
├── cmd/server/          # 服务器启动入口
├── internal/
│   ├── config/         # 配置管理
│   ├── consumer/       # Kafka消费者
│   ├── handler/        # 任务处理器
│   ├── client/         # HTTP客户端
│   ├── ratelimit/      # 限流器
│   ├── model/          # 数据模型
│   └── producer/       # Kafka生产者
├── configs/            # 配置文件
├── go.mod
├── go.sum
├── main.go
└── README.md
```

### 工作流程
1. **消费任务**: 从Kafka `http.tasks` topic消费HTTP任务
2. **限流检查**: 使用令牌桶检查是否可以执行请求
3. **HTTP请求**: 通过连接池执行HTTP请求
4. **结果处理**: 
   - 成功：发送数据到数据topic（如`data.ethereum`）
   - 失败：根据错误类型决定重试或失败
5. **状态上报**: 发送任务执行状态到`tasks.status` topic

## 配置说明

### 主要配置项

```yaml
kafka:
  brokers: ["localhost:9092"]
  consumer:
    group_id: "http-worker-group"
    topics:
      tasks: "http.tasks"

http:
  client:
    timeout: 10                    # 请求超时（秒）
    connection_timeout: 5          # 连接超时（秒）
    max_idle_conns: 100           # 最大空闲连接数
    max_conns_per_host: 20        # 每个主机最大连接数

rate_limit:
  refill_interval_ms: 200         # 令牌补充间隔（毫秒）

datasources:
  mock-ethereum:
    url: "http://localhost:8080"
    rate_limit:
      interval: 60                # 限流时间窗口（秒）
      weight: 1200               # 时间窗口内的权重限制

topic_mapping:
  data_topic_prefix: "data."      # 数据输出topic前缀
  status_topic: "tasks.status"    # 任务状态topic
```

## 使用方法

### 1. 启动服务

```bash
# 使用默认配置
go run main.go

# 使用自定义配置
go run main.go /path/to/config.yaml

# 编译后运行
go build -o http-worker main.go
./http-worker
```

### 2. 发送测试任务

向Kafka `http.tasks` topic发送JSON格式的任务：

```json
{
  "taskId": "task-001",
  "payload": {
    "dataSourceUrl": "http://localhost:8080",
    "method": "POST",
    "params": {
      "method": "eth_getBlockByNumber",
      "params": ["latest", false],
      "id": 1
    },
    "dataSourceId": "mock-ethereum"
  }
}
```

### 3. 监控输出

**数据输出** (topic: `data.ethereum`):
```json
{
  "taskId": "task-001",
  "dataSourceId": "mock-ethereum",
  "timestamp": "2024-01-13T10:30:00Z",
  "data": {
    "id": 1,
    "result": {
      "number": "0xf4241",
      "hash": "0x1234...",
      // ... 区块数据
    },
    "jsonrpc": "2.0"
  },
  "metadata": {
    "statusCode": 200,
    "duration": 50,
    "size": 1024
  }
}
```

**状态输出** (topic: `tasks.status`):
```json
{
  "taskId": "task-001",
  "status": "SUCCESS",
  "message": "Request completed successfully",
  "timestamp": "2024-01-13T10:30:00Z",
  "duration": 50,
  "statusCode": 200,
  "dataSize": 1024,
  "retryCount": 0
}
```

## 限流机制

### 令牌桶算法
- **容量**: 根据数据源配置的权重限制
- **补充速率**: 平滑分配到200ms间隔
- **成本计算**: 根据请求类型计算令牌消耗

### 示例配置
```yaml
datasources:
  coinmarketcap:
    rate_limit:
      interval: 60      # 60秒时间窗口
      weight: 333       # 333个请求限制
      # 计算：每200ms补充 333 * 200 / (60 * 1000) ≈ 1个令牌
```

## HTTP客户端特性

### 连接池管理
- 按`host:port`隔离连接池
- 支持Keep-Alive
- 自动管理空闲连接

### 协议支持
- **标准HTTP**: GET/POST请求
- **JSON-RPC**: 自动识别以太坊类型接口
- **认证**: 支持API Key认证

### 错误处理
- **可重试错误**: 5xx、429、408状态码
- **不可重试错误**: 4xx状态码（除429外）
- **网络错误**: 连接超时、DNS解析失败等

## 监控和调试

### 日志配置
```yaml
logging:
  level: "debug"        # debug, info, warn, error
  format: "json"        # json, text
```

### 关键日志
- 任务接收和处理状态
- HTTP请求执行详情
- 限流检查结果
- Kafka消息发送状态

### 性能指标
- 任务处理延迟
- HTTP请求响应时间
- 令牌桶使用率
- Kafka消息吞吐量

## 故障排查

### 常见问题

1. **Kafka连接失败**
   - 检查broker地址配置
   - 确认网络连通性
   - 验证topic是否存在

2. **HTTP请求超时**
   - 调整超时配置
   - 检查目标服务状态
   - 验证网络延迟

3. **限流触发**
   - 检查令牌桶配置
   - 调整请求频率
   - 监控令牌使用情况

4. **任务处理失败**
   - 查看详细错误日志
   - 验证任务格式
   - 检查数据源配置

### 调试技巧
```bash
# 启用详细日志
export LOG_LEVEL=debug

# 监控Kafka消息
kafka-console-consumer --bootstrap-server localhost:9092 --topic http.tasks

# 查看状态输出
kafka-console-consumer --bootstrap-server localhost:9092 --topic tasks.status

# 查看数据输出
kafka-console-consumer --bootstrap-server localhost:9092 --topic data.ethereum
```

## 扩展开发

### 添加新数据源
1. 在配置文件中添加数据源配置
2. 实现特定的认证逻辑（如需要）
3. 配置topic映射
4. 添加特定的错误处理逻辑

### 自定义限流规则
1. 扩展`calculateCost`方法
2. 添加新的成本计算规则
3. 更新配置文件格式

### 添加新协议支持
1. 扩展`buildRequest`方法
2. 添加协议识别逻辑
3. 实现协议特定的请求构建

## 测试

### 单元测试
```bash
go test ./internal/...
```

### 集成测试
1. 启动Kafka和Mock服务
2. 运行HTTP Worker
3. 发送测试任务
4. 验证输出结果

### 性能测试
```bash
# 使用Kafka工具发送大量任务
kafka-console-producer --bootstrap-server localhost:9092 --topic http.tasks < test-tasks.json
```

## 许可证

本项目仅供学习和测试使用。
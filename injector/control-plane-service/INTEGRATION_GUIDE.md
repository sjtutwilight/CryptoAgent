# 控制平面与HTTP Worker集成指南

## 📋 系统概述

控制平面服务现已完成，可以与HTTP Worker形成完整的闭环系统。本指南详细说明如何启动和测试整个系统。

## 🏗️ 系统架构

```
用户 → 控制平面API → 限流器 → 任务调度器 → PostgreSQL → 定时生产器 → Kafka → HTTP Worker → 状态上报 → 控制平面监听器
```

## 🚀 启动步骤

### 1. 启动基础服务
```bash
# 启动 Kafka, PostgreSQL, Redis
cd /Users/yangguang/data-ingestion-1
docker-compose up -d
```

### 2. 启动控制平面服务
```bash
cd control-plane-service
mvn spring-boot:run
```

### 3. 启动HTTP Worker
```bash
cd ../worker/http-worker
go run .
```

## 📬 消息格式兼容性

### HTTP任务消息 (http.tasks topic)

控制平面发送的消息格式完全兼容HTTP Worker：

```json
{
  "taskId": "task-abc123def456",
  "payload": {
    "dataSourceUrl": "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
    "method": "get",
    "params": {
      "start": 1,
      "limit": 100,
      "convert": "USD"
    },
    "apikey": "your-api-key",
    "headers": {
      "Content-Type": "application/json"
    }
  }
}
```

### 任务状态更新 (tasks.status topic)

HTTP Worker上报的状态格式：

```json
{
  "taskId": "task-abc123def456",
  "status": "SUCCESS",
  "statusCode": 200,
  "message": "请求成功",
  "durationMs": 1234,
  "dataSize": 5678,
  "retryable": false,
  "timestamp": 1642678901234
}
```

## 🔄 完整测试流程

### 1. 创建任务
```bash
curl -X POST http://localhost:8083/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "dataSourceId": "ethereum-mainnet",
    "method": "GET", 
    "params": {
      "limit": 10
    },
    "cost": 1,
    "priority": 5
  }'
```

### 2. 查询任务状态
```bash
curl http://localhost:8083/api/v1/tasks/{taskId}
```

### 3. 监控系统统计
```bash
curl http://localhost:8083/system/stats
```

## 🎯 关键特性

### 限流控制
- Redis滑动窗口限流
- 数据源级别的精确控制
- 自动延迟调度

### 任务重试机制
- 基于HTTP状态码的智能重试
- 指数退避算法
- 最大重试次数控制

### 状态监控
- 实时任务统计
- 执行历史记录
- 系统健康检查

## 📊 系统监控端点

- **健康检查**: `GET /system/health`
- **系统统计**: `GET /system/stats`
- **限流状态**: `GET /system/rate-limit/{dataSourceId}`
- **任务统计**: `GET /api/v1/tasks/statistics`

## 🔧 配置说明

### 限流配置
```yaml
app:
  rate-limit:
    window-size: 60          # 滑动窗口大小（秒）
    redis-key-prefix: "rate-limit:"
```

### 定时器配置
```yaml
app:
  timer:
    scan-interval: 1000      # 任务扫描间隔（毫秒）
    max-tasks-per-scan: 1000 # 每次扫描最大任务数
    advance-schedule-time: 5  # 提前调度时间（秒）
```

### Kafka配置
```yaml
app:
  kafka:
    topics:
      http-tasks: "http.tasks"
      task-status: "tasks.status"
```

## 🎉 完整闭环验证

1. **任务提交** → 用户通过REST API提交任务
2. **限流检查** → 系统检查数据源限流配置
3. **任务持久化** → 任务保存到PostgreSQL
4. **定时调度** → TimerProducer发送任务到Kafka
5. **任务执行** → HTTP Worker处理任务
6. **状态上报** → HTTP Worker发送状态到Kafka
7. **状态更新** → 控制平面更新任务状态
8. **重试处理** → 自动处理失败任务重试

## 🚨 故障排查

### 常见问题
1. **任务一直PENDING** → 检查TimerProducerService是否启动
2. **限流不生效** → 检查Redis连接和配置
3. **状态不更新** → 检查Kafka连接和topics配置

### 日志级别
```yaml
logging:
  level:
    com.crypto: DEBUG
    org.springframework.kafka: INFO
```

## 📈 性能建议

- 根据数据源QPS配置合适的限流参数
- 调整Kafka batch配置优化吞吐量
- 监控PostgreSQL连接池使用情况
- 定期清理执行历史记录

系统现已完全就绪，可以处理生产级的任务调度和执行！
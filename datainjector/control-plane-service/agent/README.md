# 控制平面服务 (Control Plane Service)

## 概述

控制平面服务是加密货币数据接入系统的核心调度组件，负责任务的创建、调度、限流和状态管理。

## 功能特性

### 🎯 核心功能
- **任务调度**: 支持立即执行和延时调度
- **智能限流**: 基于Redis滑动窗口的分布式限流
- **自动重试**: 可配置的任务重试机制
- **状态监控**: 实时任务状态跟踪和统计

### 🏗️ 架构组件
- **MainProcessor**: 主处理器，协调整个任务流程
- **TaskScheduler**: 任务调度器，负责任务持久化
- **RateLimiter**: 限流器，基于Redis实现
- **TimerProducer**: 定时生产者，将任务发送到Kafka
- **StatusListener**: 状态监听器，处理HTTP Worker反馈

## 快速开始

### 1. 环境要求
- Java 17+
- PostgreSQL 15+
- Redis 7+
- Kafka 2.8+

### 2. 启动基础设施
```bash
# 启动Docker服务
docker-compose up -d

# 等待服务启动完成
sleep 30
```

### 3. 启动控制平面服务
```bash
cd control-plane-service
mvn spring-boot:run
```

### 4. 验证服务状态
```bash
# 健康检查
curl http://localhost:8083/api/v1/system/health

# 查看任务统计
curl http://localhost:8083/api/v1/tasks/statistics
```

## API接口

### 创建任务
```bash
POST /api/v1/tasks
Content-Type: application/json

{
  "dataSourceId": "MockEthereum",
  "taskType": "http_jsonrpc",
  "payload": {
    "url": "/eth",
    "method": "eth_getBlockByNumber",
    "params": ["latest", false],
    "chain_id": "31337",
    "source": "manual"
  },
  "metadata": {
    "origin": "api",
    "notes": "manual trigger"
  },
  "priority": 5,
  "cost": 1
}
```

### 查询任务
```bash
# 根据ID查询
GET /api/v1/tasks/{taskId}

# 任务统计
GET /api/v1/tasks/statistics
```

## 与HTTP Worker集成

### 消息流程
```
控制平面 --[http.tasks]--> HTTP Worker
HTTP Worker --[tasks.status]--> 控制平面
```

### 任务消息格式
```json
{
  "taskId": "task-abc123",
  "taskType": "http_jsonrpc",
  "payload": {
    "url": "/eth",
    "method": "eth_getBlockRange",
    "params": ["0xf45c9", "0xf45ca"],
    "chain_id": "31337",
    "source": "sequence-gap-detector",
    "dataSourceId": "MockEthereum",
    "task_id": "dex_transaction-1000-1024"
  },
  "metadata": {
    "gapStart": 1000,
    "gapEnd": 1024,
    "sequenceType": "dex_transaction"
  }
}

## 缺失检测与自动回补

- `SequenceExtractJob` 将多源序列写入 `data.sequence` 主题。
- 控制面内置 `SequenceGapDetectorService`，实时消费 `data.sequence`，维护 Redis 中的序列状态并识别缺口。
- 一旦发现缺失区间，会按配置切片生成补数任务，直接调用 `MainProcessorService.processTask`，形成闭环。
- 关键配置位于 `sequence-detector` 节点，可针对不同 `type` 配置数据源、方法、批量大小、限流成本等。
```

## 完整闭环测试

启动系统后，可以进行端到端测试：

1. **启动所有服务**：`docker-compose up -d`
2. **启动HTTP Worker**：按照HTTP Worker文档启动
3. **启动控制平面**：`mvn spring-boot:run`
4. **创建测试任务**：使用上述API创建任务
5. **验证执行**：查看Kafka消息和任务状态变化

系统现在支持完整的任务生命周期管理！🚀

# Unified Worker 架构与设计文档

## 一、架构概览

### 1.1 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                    Worker Manager                        │
│              (统一生命周期管理)                            │
└──────────────┬──────────────────────────┬────────────────┘
               │                          │
        ┌──────▼───────┐         ┌───────▼────────┐
        │ Role Instance │         │ Role Instance  │
        │   (角色1)     │         │   (角色2)      │
        └──────┬───────┘         └───────┬────────┘
               │                          │
    ┌──────────┴──────────┐    ┌─────────┴─────────┐
    │  Protocol Layer     │    │   Task Layer      │
    │  ├─ HTTP Handler   │    │   ├─ Polling      │
    │  └─ WS Handler     │    │   ├─ LongConn     │
    └──────────┬──────────┘    │   └─ OneTime      │
               │                └─────────┬─────────┘
    ┌──────────▼──────────────────────────▼─────────┐
    │           Runtime Layer (能力层)                │
    │  ├─ RateLimiter (限流)                         │
    │  ├─ ReconnectMgr (重连)                        │
    │  ├─ HeartbeatMgr (心跳)                        │
    │  ├─ ConnectionPool (连接池)                     │
    │  └─ SequenceBuffer (序列号缓冲)                 │
    └────────────────┬───────────────────────────────┘
                     │
    ┌────────────────▼───────────────────────────────┐
    │           Parser Chain (解析器链)               │
    │  DexParser → BlockParser → BalanceParser → GenericParser   │
    └────────────────┬───────────────────────────────┘
                     │
    ┌────────────────▼───────────────────────────────┐
    │              Kafka Layer                        │
    │  ├─ Producer (数据/失败/序列号)                 │
    │  └─ Consumer (命令式任务)                       │
    └────────────────────────────────────────────────┘
```

### 1.2 Pipeline责任链数据流

```
原始数据 (rawData)
    ↓
┌─────────────────────┐
│   ParseStage        │  ← Parser责任链: BlockParser → BalanceParser → GenericParser
│   解析数据           │     输出: ParsedData (map[string]interface{})
└─────────────────────┘
    ↓
┌─────────────────────┐
│  SequenceStage      │  ← 提取序列号字段 (通用化)
│  序列号提取          │     支持: "number", "block_id", "result.number"
└─────────────────────┘
    ↓
┌─────────────────────┐
│   BufferStage       │  ← 乱序缓冲 (SequenceBuffer)
│   缓冲检查           │     连续 → 继续, 乱序 → 缓冲 (ShouldOutput=false)
└─────────────────────┘
    ↓
┌─────────────────────┐
│   OutputStage       │  ← Kafka Producer
│   输出到Kafka        │     发送DataMessage到output_topic
└─────────────────────┘
```

**扩展示例**: 新增ValidationStage (数据验证)
```go
type ValidationStage struct {
    BasePipeline
}

func (vs *ValidationStage) Process(ctx context.Context, data *PipelineData) error {
    // 验证逻辑
    if !isValid(data.ParsedData) {
        data.ShouldOutput = false
        log.Printf("数据验证失败")
        return nil
    }
    return vs.ProcessNext(ctx, data)
}

// 注册到链中
parseStage.SetNext(validationStage)
validationStage.SetNext(seqStage)
```

### 1.3 目录结构

```
unified-worker/
├── cmd/
│   └── main.go                 # 主程序入口
├── configs/
│   └── config.yaml            # 配置文件示例
├── internal/
│   ├── config/                # 配置管理
│   │   ├── config.go
│   │   ├── yaml_types.go
│   │   └── loader.go
│   ├── protocol/              # Protocol层（协议插件）
│   │   ├── http_handler.go
│   │   ├── http_handler_impl.go
│   │   ├── http_handler_meta.go
│   │   ├── websocket_handler.go
│   │   ├── websocket_connect.go
│   │   ├── websocket_reconnect.go
│   │   ├── websocket_heartbeat.go
│   │   ├── websocket_handler_impl.go
│   │   └── websocket_meta.go
│   ├── task/                  # Task层（任务编排）
│   │   ├── task_executor.go
│   │   ├── long_connection.go
│   │   ├── polling.go
│   │   ├── one_time.go
│   │   ├── data_processor.go
│   │   └── data_processor_impl.go
│   ├── runtime/               # Runtime层（通用能力）
│   │   ├── ratelimit.go
│   │   ├── ratelimit_impl.go
│   │   ├── reconnect.go
│   │   ├── reconnect_impl.go
│   │   ├── heartbeat.go
│   │   ├── heartbeat_impl.go
│   │   └── connection_pool.go
│   ├── parser/                # Parser层（解析器链）
│   │   ├── parser.go
│   │   ├── block_parser.go
│   │   ├── balance_parser.go
│   │   └── generic_parser.go
│   ├── buffer/                # 序列号缓冲
│   │   ├── sequence_buffer.go
│   │   ├── sequence_buffer_impl.go
│   │   └── sequence_buffer_utils.go
│   ├── kafka/                 # Kafka集成
│   │   ├── producer.go
│   │   ├── producer_impl.go
│   │   ├── consumer.go
│   │   └── consumer_handler.go
│   └── worker/                # Worker管理
│       ├── manager.go
│       ├── manager_impl.go
│       └── role_instance.go
└── pkg/
    └── types/                 # 类型定义
        ├── types.go
        ├── task.go
        ├── config.go
        └── message.go
```

---

## 二、核心设计机制

### 2.1 Protocol层 - 协议插件化

**设计目标**: 将协议细节与业务逻辑解耦，支持快速扩展新协议。

**关键接口**:
```go
type ProtocolHandler interface {
    Type() ProtocolType
    Initialize(ctx, config) error
    Send(ctx, message) ([]byte, error)
    Receive(ctx) (<-chan []byte, <-chan error)
    HealthCheck(ctx) error
    Close() error
    Metadata() ProtocolMetadata
}
```

**实现细节**:

| 协议 | 类型 | 双向通信 | 需要重连 | 需要心跳 | 连接池 | 典型使用 |
|-----|-----|---------|---------|---------|-------|---------|
| HTTP | 原生 | ❌ | ❌ | ❌ | ✅ | 轮询、命令式调用 |
| WebSocket | 原生 | ✅ | ✅ | ✅ | ❌ | 长连接订阅 |
| Ethereum-SDK | SDK | ❌ | 内置✅ | 内置✅ | 内置✅ | 本地/远程节点轮询 |

**扩展方式**: 实现`ProtocolHandler`接口，注册到Manager即可。

#### 2.1.1 SDK协议层设计

**设计动机**:
- 原生协议（HTTP/WebSocket）需要手动管理重连、心跳、连接池等Runtime能力
- 许多SDK（如go-ethereum）已内置这些能力，重复创建造成资源浪费
- 需要一套能力协商机制，让责任链自动识别并跳过已内置的能力

**能力协商机制**:

```go
// ProtocolMetadata 协议元数据（用于能力协商）
type ProtocolMetadata struct {
    // 协议需求
    RequiresHeartbeat      bool
    RequiresReconnect      bool
    RequiresConnectionPool bool
    RequiresRateLimit      bool
    
    // SDK内置能力声明
    HasBuiltInReconnect bool
    HasBuiltInRateLimit bool
    HasBuiltInHeartbeat bool
}
```

**责任链动态能力选择**:

```go
// RateLimitHandler根据Metadata判断
func (h *RateLimitHandler) shouldCreateRateLimiter(req *Request) bool {
    if protocolHandler, ok := req.Data["protocol"].(types.ProtocolHandler); ok {
        metadata := protocolHandler.Metadata()
        
        // 如果SDK内置了限流，跳过创建
        if metadata.HasBuiltInRateLimit {
            return false
        }
    }
    // 兜底逻辑...
}
```

**Ethereum SDK实现亮点**:
- 完整封装go-ethereum的`ethclient`
- 支持`eth_getBlockByNumber`、`eth_blockNumber`、`eth_getTransactionReceipt`
- 自动获取交易receipts和logs（参考listener实现）
- 内置重连、心跳、连接池，无需额外Runtime组件
- 区块数据完整转JSON，包含交易、logs、decoded args

**配置示例**:

```yaml
- role_id: "hardhat-local-polling"
  protocol: "ethereum-sdk"  # 使用SDK协议
  
  protocol_config:
    endpoint: "http://localhost:8545"
  
  runtime:
    reconnect:
      enabled: false  # SDK内置，无需创建
    heartbeat:
      enabled: false  # SDK内置，无需创建
    connection_pool:
      enabled: false  # SDK内置，无需创建
    rate_limit:
      enabled: true   # 业务层限流仍需要
```

---

### 2.2 Task层 - 任务类型抽象

**设计目标**: 统一不同任务类型的执行逻辑，通过TaskExecutor统一调度。

**任务类型**:
1. **长连接订阅** (LongConnection)
   - WebSocket订阅MockDataProvider的newHeads
   - 持续运行，接收数据流
   - 自动重连和订阅恢复

2. **定期轮询** (Polling)
   - HTTP轮询`eth_getBlockByNumber`
   - 定时器触发，周期执行
   - 限流保护

3. **命令式调用** (OneTime)
   - Kafka触发的单次HTTP请求
   - 用于补数据场景
   - 本地重试3次

**执行流程**:
```
TaskExecutor.Execute()
    ↓
根据TaskType选择执行方法
    ↓ (长连接)              ↓ (轮询)              ↓ (命令式)
subscribe()          Ticker触发           Kafka触发
    ↓                      ↓                   ↓
Receive() 循环         限流检查             限流检查
    ↓                      ↓                   ↓
processData()         Send()              Send()
    ↓                      ↓                   ↓
解析→缓冲→输出        processData()       processData()
```

---

### 2.3 Parser层 - 责任链模式

**设计目标**: 通用化数据解析，支持不同数据源的定制化处理。

**责任链结构**:
```
BlockParser → BalanceParser → GenericParser
    ↓              ↓                ↓
处理区块数据    处理余额快照    兜底通用解析
```

**解析流程**:
```go
// 1. 判断是否能处理
if parser.CanHandle(dataSourceID, taskType) {
    // 2. 解析数据
    parsedData := parser.Parse(rawData, config)
    // 3. 提取序列号
    sequence := parser.GetSequence(parsedData)
} else {
    // 4. 传递给下一个解析器
    return parser.Next().Parse(...)
}
```

**扩展方式**:
```go
// 新增ChainEventParser处理链上事件
type ChainEventParser struct {
    BaseParser
}

func (cep *ChainEventParser) CanHandle(...) bool {
    return contains(dataSourceID, "chain") && 
           contains(dataSourceID, "event")
}

// 注册到链中
blockParser.SetNext(chainEventParser)
chainEventParser.SetNext(balanceParser)
```

---

### 2.4 Runtime层 - 通用能力组合

**设计目标**: 提供可组合的运行时能力，按需装配。

**能力组件**:

1. **限流器 (RateLimiter)**
   ```
   令牌桶算法
   - 容量: 按数据源配置
   - 补充速率: 折算到秒级 (如1200次/分 = 20次/秒)
   - 粒度: 按数据源维度隔离
   ```

2. **重连管理器 (ReconnectManager)**
   ```
   指数退避策略
   - 初始: 2秒
   - 指数增长: 2^n
   - 最大: 60秒
   - 重试次数: 无限(-1) 或 有限(N次)
   ```

3. **心跳管理器 (HeartbeatManager)**
   ```
   WebSocket心跳
   - Ping间隔: 30秒
   - Pong超时: 10秒
   - 超时触发: 重连
   ```

4. **序列号缓冲 (SequenceBuffer)**
   ```
   乱序处理
   - 数据结构: 有序链表
   - 最大容量: 1000
   - 比较函数: 可定制 (int64/string/hex)
   ```

**组装规则**:
```yaml
runtime:
  reconnect:
    enabled: true    # WebSocket需要
  heartbeat:
    enabled: true    # WebSocket需要
  rate_limit:
    enabled: true    # 所有协议都需要
  connection_pool:
    enabled: true    # HTTP需要，WebSocket不需要
```

---

### 2.5 序列号管理与缺失检测

**设计目标**: 通用化序列号提取和连续性检测。

**机制**:

1. **通用化提取**
   ```yaml
   sequence_field: "number"  # 区块数据使用block.number
   sequence_field: "block_id" # 余额数据使用block_id
   sequence_field: "result.number" # 支持嵌套路径
   ```

2. **连续性检测**
   ```
   收到数据 → 提取序列号 → 比较lastSequence
       ↓ (连续)              ↓ (乱序)
   直接输出Kafka        加入SequenceBuffer
                              ↓
                       尝试从Buffer取连续数据
                              ↓
                       输出到Kafka
   ```

3. **缺失上报**
   ```
   Worker定期批量上报已接收序列号
       ↓
   控制面检测连续性
       ↓ (发现缺失12347)
   下发OneTime任务: eth_getBlockByNumber(0x303b)
       ↓
   Worker执行补数据
   ```

---

### 2.6 与控制面交互

**交互模式**:

1. **任务接收** (Kafka: `worker.tasks`)
   ```json
   {
     "task_id": "task-补数据-12347",
     "task_type": "one_time",
     "protocol": "http",
     "data_source_id": "mock-ethereum",
     "task_specific_config": {
       "one_time": {
         "method": "eth_getBlockByNumber",
         "params": {"block": "0x303b", "full_tx": false}
       }
     },
     "sequence_field": "number",
     "output_topic": "chain.ethereum.blocks",
     "report_to_control_plane": false
   }
   ```

2. **数据输出** (Kafka: 按data_source配置的topic)
   ```json
   {
     "worker_id": "worker-1",
     "task_id": "task-001",
     "data_source_id": "mock-ethereum",
     "timestamp": 1696512345,
     "sequence": 12345,
     "data": {...},
     "metadata": {"task_type": "long_connection"}
   }
   ```

3. **失败上报** (Kafka: `worker.failures`)
   ```json
   {
     "worker_id": "worker-1",
     "task_id": "task-001",
     "data_source_id": "mock-ethereum",
     "timestamp": 1696512345,
     "error_type": "retry_exhausted",
     "error_message": "HTTP请求失败",
     "retry_count": 3,
     "last_sequence": 12344
   }
   ```

4. **序列号上报** (Kafka: `worker.sequences`)
   ```json
   {
     "worker_id": "worker-1",
     "task_id": "task-001",
     "data_source_id": "mock-ethereum",
     "timestamp": 1696512345,
     "sequences": [12345, 12346, 12348, 12349]
   }
   ```

---

## 三、已完成功能

### 3.1 核心模块 ✅

- [x] **配置管理**: YAML配置加载、类型转换
- [x] **Protocol层**: HTTP Handler、WebSocket Handler
- [x] **Task层**: 三种任务类型执行逻辑
- [x] **Runtime层**: 限流、重连、心跳、连接池、缓冲
- [x] **Parser层**: 责任链模式，三种解析器
- [x] **Kafka集成**: 生产者、消费者
- [x] **Worker Manager**: 角色管理、生命周期管理

### 3.2 关键特性 ✅

- [x] 插件化协议扩展
- [x] 责任链解析器
- [x] 通用化序列号提取
- [x] 乱序数据缓冲
- [x] 自动重连恢复
- [x] 本地限流保护
- [x] 优雅停机

---

## 四、重大设计改进 ✅

### 4.1 Pipeline责任链模式（新增）

**问题**: 原设计只有Parser是责任链，整个数据处理流程缺乏统一抽象。

**改进**: 引入Pipeline责任链，覆盖完整数据流：
```
接收数据 → ParseStage → SequenceStage → BufferStage → OutputStage
```

**优势**:
- ✅ 每个阶段职责单一，易于测试
- ✅ 支持动态插拔处理器
- ✅ 统一错误处理和日志
- ✅ 便于扩展新的处理阶段

**实现**:
```go
// 构建管道链
parseStage := pipeline.NewParseStage(parserConfig)
seqStage := pipeline.NewSequenceStage(sequenceField)
bufferStage := pipeline.NewBufferStage()
outputStage := pipeline.NewOutputStage(producer, workerID, taskID, dataSourceID, outputTopic)

parseStage.SetNext(seqStage)
seqStage.SetNext(bufferStage)
bufferStage.SetNext(outputStage)

// 执行管道
chain := pipeline.NewPipelineChain(parseStage)
chain.Execute(ctx, rawData)
```

### 4.2 数据源元数据标准化（新增）

**问题**: 控制面下发任务时，数据源信息散落在多个字段，难以维护。

**改进**: 引入DataSourceMetadata，统一管理数据源元信息：

```json
{
  "task_id": "task-001",
  "data_source": {
    "id": "mock-ethereum",
    "name": "Mock Ethereum Node",
    "type": "ethereum",
    "protocol": "websocket",
    "endpoint": {
      "url": "ws://localhost:8090/ws",
      "headers": {"User-Agent": "TwilightWorker"},
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
  }
}
```

**优势**:
- ✅ 数据源信息集中管理
- ✅ 控制面可动态更新限流配置
- ✅ 支持多种数据源类型
- ✅ 便于数据源版本管理

### 4.3 通用化订阅逻辑（新增）

**问题**: 原代码硬编码`eth_subscribe`，不够通用。

**改进**: 从数据源元数据读取订阅方法：
```go
// 以太坊数据源
"subscription": {
  "subscribe_method": "eth_subscribe",
  "params": ["newHeads"]
}

// Binance数据源
"subscription": {
  "subscribe_method": "SUBSCRIBE",
  "params": ["btcusdt@trade"]
}
```

**优势**:
- ✅ 主链路不包含具体业务逻辑
- ✅ 支持任意协议的订阅
- ✅ 配置驱动，易于扩展

---

## 五、当前遗留问题

### 5.1 序列号批量上报未实现

**问题**: `reportSequence()`函数为空实现

**修复方案**: 
```go
type SequenceCollector struct {
    sequences []interface{}
    mu        sync.Mutex
    ticker    *time.Ticker
}
```

### 5.2 配置解析增强

**问题**: YAML嵌套map解析可能失败

**修复方案**: 增强`manager_impl.go`的类型转换逻辑

---

## 六、待做事项

### 6.1 高优先级 🔴

1. **修复编译错误**
   - [ ] 补充所有缺失的import
   - [ ] 修复类型转换问题
   - [ ] 运行`go mod tidy`

2. **完善配置解析**
   - [ ] 增强`parseTaskSpecificConfig`的类型处理
   - [ ] 添加配置验证逻辑
   - [ ] 支持环境变量替换

3. **实现序列号批量上报**
   - [ ] 创建SequenceCollector
   - [ ] 定时刷新机制
   - [ ] 集成到TaskExecutor

4. **集成listener逻辑**
   - [ ] 创建ChainEventParser解析链上事件
   - [ ] 创建BalanceSnapshotTask支持余额快照轮询
   - [ ] 配置示例更新

### 6.2 中优先级 🟡

5. **错误处理优化**
   - [ ] 统一错误码定义
   - [ ] 错误分类（可重试/不可重试）
   - [ ] 详细错误日志

6. **WebSocket订阅恢复**
   - [ ] 重连后重新发送subscribe请求
   - [ ] 订阅ID管理
   - [ ] 状态持久化（可选）

7. **测试验证**
   - [ ] 单元测试（Parser、Buffer、Runtime）
   - [ ] 集成测试（与MockDataProvider对接）
   - [ ] 压力测试（限流、重连）

### 6.3 低优先级 🟢

8. **可观测性**
   - [ ] Prometheus指标暴露
   - [ ] 链路追踪（OpenTelemetry）
   - [ ] 结构化日志

9. **性能优化**
   - [ ] 批量Kafka发送
   - [ ] 零拷贝数据传输
   - [ ] 并发控制优化

10. **文档完善**
    - [ ] API文档
    - [ ] 配置参数说明
    - [ ] 故障排查指南

---

## 七、控制面改进建议

### 7.1 任务下发格式标准化（已更新）

**新格式** (整合数据源元数据):
```json
{
  "task_id": "uuid",
  "task_type": "one_time|polling|long_connection",
  "data_source_id": "mock-ethereum",
  "data_source": {
    "id": "mock-ethereum",
    "name": "Mock Ethereum Node",
    "type": "ethereum",
    "protocol": "websocket",
    "endpoint": {
      "url": "ws://localhost:8090/ws",
      "headers": {"User-Agent": "TwilightWorker"},
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
    },
    "custom_config": {}
  },
  "task_specific_config": {
    "subscription": {...},
    "polling": {...},
    "one_time": {...}
  },
  "sequence_field": "number",
  "output_topic": "chain.ethereum.blocks",
  "report_to_control_plane": true,
  "retry_config": {
    "max_retries": 3,
    "backoff_base": 2,
    "backoff_max": 30
  }
}
```

**优势**:
- ✅ 数据源元信息集中管理
- ✅ 控制面可动态调整限流、端点配置
- ✅ 支持任意数据源类型扩展
- ✅ 订阅方法通用化（不再硬编码）

### 7.2 序列号缺失检测

**流程**:
```
1. Worker每分钟批量上报: [12345, 12346, 12348, 12349]
2. 控制面检测: 缺失12347
3. 控制面下发补数据任务: 
   {
     "task_type": "one_time",
     "method": "eth_getBlockByNumber",
     "params": {"block": "0x303b"}
   }
4. Worker执行补数据
5. 控制面验证完整性
```

### 7.3 分布式限流协调

**当前**: Worker本地限流
**建议**: 控制面分布式限流
- 控制面维护全局限流状态（Redis）
- 下发任务时计算`next_available_time`
- Worker按时间调度，本地限流作为保护

---

## 八、MockDataProvider改进建议

**当前已满足**: 提供block数据和WebSocket推送

**建议保持简单**: 
1. ✅ WebSocket推送包含`number`字段
2. ✅ HTTP接口支持按`block_number`查询
3. ✅ 故障注入功能完善

**无需改动**: MockDataProvider设计已足够灵活。

---

## 九、快速启动指南

### 9.1 编译

```bash
cd /Users/yangguang/DataPlatform/injector/unified-worker
go mod tidy
go build -o unified-worker cmd/main.go
```

### 9.2 运行

```bash
# 确保依赖服务运行
docker-compose up -d  # Kafka, Redis

# 启动MockDataProvider
cd ../datasource/MockDataProvider
go run main.go

# 启动unified-worker
cd ../injector/unified-worker
./unified-worker -config configs/config.yaml
```

### 9.3 验证

```bash
# 检查Kafka输出
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic chain.ethereum.blocks --from-beginning

# 检查失败上报
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic worker.failures --from-beginning
```

---

## 十、总结

**当前状态**: ✅ MVP版本完成，责任链全面落地

**已完成** - 第一阶段（基础架构）:
- ✅ Pipeline责任链完整实现（Parse → Sequence → Buffer → Output）
- ✅ MockDataProvider增加WebSocket补数据支持（eth_getBlockByNumber + eth_getBlockRange）
- ✅ 数据源元数据标准化（通过control-plane-task-example.json下发）
- ✅ 通用化订阅逻辑（支持eth_subscribe、SUBSCRIBE等）
- ✅ WebSocket心跳问题修复（MockDataProvider正确响应Pong）

**已完成** - 第二阶段（责任链深化）:
- ✅ **角色创建责任链**: Protocol → RateLimit → Executor（WebSocket自动跳过限流）
- ✅ **智能限流判断**: WebSocket长连接不创建限流器，HTTP才创建
- ✅ **DEX事件解析器**: 替代listener硬编码逻辑，支持所有DEX事件
- ✅ **Parser链优化**: DexParser → BlockParser → BalanceParser → GenericParser
- ✅ **配置增强**: 新增hardhat-local-polling角色，输出到dex_transaction
- ✅ **批量区块查询**: MockDataProvider支持eth_getBlockRange(start, end)

**已完成** - 第三阶段（SDK协议层）:
- ✅ **SDK协议抽象**: 新增ProtocolEthereumSDK类型
- ✅ **能力协商机制**: ProtocolMetadata增加HasBuiltInReconnect等字段
- ✅ **Ethereum SDK实现**: 完整封装go-ethereum，内置重连/心跳/连接池
- ✅ **动态能力选择**: 责任链根据Metadata自动跳过SDK内置能力
- ✅ **智能Runtime创建**: SDK协议不创建重连/心跳/连接池管理器
- ✅ **完整区块转换**: 支持交易、logs、receipt完整转JSON（参考listener）

**核心优势**:
- ✅ 插件化设计，易于扩展
- ✅ 完整Pipeline责任链，职责分离
- ✅ 通用化序列号管理
- ✅ SDK协议层，复用第三方库内置能力
- ✅ 责任链动态能力选择，避免资源浪费

**数据格式规范**:

DexParser输出格式（与listener保持一致）：
```json
{
  "transaction": {
    "blockNumber": 177,
    "blockHash": "0x...",
    "timestamp": 1759461410211,
    "transactionHash": "0x...",
    "transactionIndex": 0,
    "transactionStatus": "success",
    "gasUsed": 103272,
    "gasPrice": "1000000008",
    "nonce": 126,
    "fromAddress": "0x...",
    "toAddress": "0x...",
    "transactionValue": "0",
    "inputData": "0x...",
    "chainID": "31337"
  },
  "events": [
    {
      "eventName": "Transfer",
      "contractAddress": "0x...",
      "logIndex": 0,
      "blockNumber": 177,
      "topics": ["0x..."],
      "eventData": "0x...",
      "decodedArgs": {
        "from": "0x...",
        "to": "0x...",
        "value": "3901711100000000000000"
      }
    }
  ]
}
```

**输出策略**:
- 每个包含DEX事件的交易输出一条Kafka消息
- 无交易或无DEX事件的区块不输出
- 保持与原listener完全一致的数据格式
- ✅ 完整的容错机制
- ✅ 配置驱动，无业务硬编码

**待验证**:
1. WebSocket长连接订阅 + 数据流入Kafka
2. SequenceBuffer乱序处理
3. 自动重连机制
4. 补数据机制（WebSocket eth_getBlockByNumber）

**下一步**: 端到端测试 → 验证所有机制正常运行 → 集成listener逻辑 → 生产部署

---

## 十一、架构重构v2.0（进行中）

### 重构背景

**问题诊断**:
1. 责任链职责混乱：资源管理（限流、连接池）和数据处理（解析、输出）混在一起
2. 组件硬编码：Parser、Task逻辑写死，缺乏配置驱动的灵活性
3. 补数据机制缺失：无法处理序列号缺失场景

**改进方向**:
1. **职责分离**: 资源层（Resource）、任务层（Fetcher）、处理层（Handler）三层解耦
2. **配置驱动**: 通过配置动态选择Parser、Fetcher、Handler组件
3. **补数据机制**: 增加MissingDetector和Refiller处理缺失数据

### 新架构设计

```
┌─────────────────────────────────────────┐
│           Worker Manager                 │
└──────────────┬──────────────────────────┘
               │
        ┌──────▼────────┐
        │  Role Instance │
        │  ┌──────────┐ │
        │  │Resources │ │ ← 资源层：限流、连接池、重连、心跳（按需创建）
        │  └──────────┘ │
        │  ┌──────────┐ │
        │  │ Fetcher  │ │ ← 任务层：BalanceFetcher、BlockFetcher（配置驱动）
        │  └──────────┘ │
        │  ┌──────────┐ │
        │  │ Handlers │ │ ← 处理层：Parser→Sequence→MissingDetector→Refiller→Sink
        │  └──────────┘ │
        └───────────────┘
```

### ✅ 已完成组件

#### 1. ResourceManager（资源管理器）

**核心特性**:
- **配置驱动创建**: 只创建配置中明确声明的资源
- **SDK能力协商**: 根据ProtocolMetadata自动跳过SDK内置能力
- **统一资源接口**: 提供AcquireRateLimit、GetConnectionPool等方法

**实现位置**: `/internal/resource/manager.go`

```go
type ResourceManager struct {
    roleID string
    rateLimiter   runtime.RateLimiter      // 可选
    connPool      runtime.ConnectionPool    // 可选
    reconnectMgr  runtime.ReconnectManager  // 可选
    heartbeatMgr  runtime.HeartbeatManager  // 可选
}

// 创建逻辑：配置驱动 + SDK能力协商
if config.RateLimit.Enabled && !protocolMeta.HasBuiltInRateLimit {
    rm.rateLimiter = runtime.NewTokenBucketRateLimiter(config.RateLimit)
}
```

#### 2. DataFetcher（数据获取器）

**核心特性**:
- **Fetcher接口**: 统一数据获取抽象
- **工厂模式**: 通过`polling_task`配置动态创建
- **内置实现**: BalanceFetcher、BlockFetcher

**实现位置**: `/internal/fetcher/`

```go
type DataFetcher interface {
    Fetch(ctx context.Context, config map[string]interface{}) ([]byte, error)
    Name() string
}

// 工厂注册
factory.Register("balance", NewBalanceFetcher)
factory.Register("block", NewBlockFetcher)
```

**BalanceFetcher特性**:
- 参考listener的BalanceSnapshotGenerator实现
- 支持多账户、多token余额批量获取
- 使用`balanceOf` (0x70a08231) 调用合约
- 输出JSON数组格式

**BlockFetcher特性**:
- 支持Ethereum SDK获取区块
- 支持`latest`或指定区块号
- 支持完整交易信息（含logs）

### 📋 待完成组件

#### 3. Handler Chain（处理链重构）

**目标结构**:
```
Parser → SequenceExtractor → MissingDetector → Refiller → KafkaSink
```

**设计要点**:
- 每个Handler独立组件，实现统一接口
- 通过HandlerFactory根据配置动态构建链
- Parser不再内部嵌套责任链（每角色一个Parser）

#### 4. 配置结构重构

**新配置格式**:
```yaml
roles:
  - role_id: "localnode-balance"
    protocol: "ethereum-sdk"
    task_type: "polling"
    
    task_config:
      polling_task: "balance"  # 使用BalanceFetcher
      interval_seconds: 60
      accounts: ["0x..."]
      tokens:
        - address: "0x..."
          symbol: "USDC"
    
    resources:  # 仅创建配置的资源
      rate_limit:
        enabled: true
        capacity: 100
    
    handlers:  # 配置驱动的处理链
      - type: "parser"
        name: "BalanceParser"
      - type: "sequence"
        field: "timestamp"
      - type: "missing_detector"
        threshold: 5
      - type: "refiller"
        method: "websocket"
      - type: "kafka_sink"
        topic: "account_balance_snapshot"
```

#### 5. 补数据机制

**触发逻辑**:
- MissingDetector检测序列号缺失
- 缺失小于阈值：通过现有WS连接单次请求
- 缺失超过阈值：记录告警，不补

### 下一步计划

1. Handler独立组件化
2. HandlerFactory实现
3. 配置解析器更新
4. RoleInstance重构
5. 端到端测试

**检查点**: 资源管理和数据获取层已完成 ✅

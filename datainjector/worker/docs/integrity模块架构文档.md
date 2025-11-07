# Integrity 模块完整架构文档

## 1. 概述

Integrity 模块是 DataInjector Worker 的核心数据完整性保障系统，负责确保数据流的顺序性、完整性和一致性。该模块采用模块化设计，通过职责分离实现了高内聚、低耦合的架构。

### 1.1 核心职责

- **序列控制**：保证消息按序列号顺序处理
- **缺失检测**：识别序列间隙并触发补数
- **去重处理**：防止重复消息进入下游
- **门控机制**：根据业务场景控制消息下发时机
- **缓冲管理**：临时存储乱序消息，等待序列连续
- **补数调度**：协调多种补数策略和通道

### 1.2 设计原则

1. **职责单一**：每个子模块只负责一个明确的功能
2. **依赖倒置**：通过接口解耦，核心逻辑不依赖具体实现
3. **配置驱动**：支持灵活的策略配置和运行时调整
4. **可观测性**：详细日志和状态跟踪

---

## 2. 整体架构

### 2.1 模块层次结构

```
handler/                          # Handler 框架层
├── handler.go                    # 核心接口定义
│   ├── Handler                   # 消息处理接口
│   ├── BackfillCommandAware      # 回补命令注入接口
│   └── SnapshotListenerAware     # 快照监听接口
├── integrity_handler.go          # Integrity 适配器（简化）
│   └── integrityAdapter          # 将 integrity 包装成 Handler
├── registry.go                   # Handler 工厂注册表
├── parser/                       # 解析器模块
│   └── binance_parser.go         # Binance 消息解析器
├── orderbook_handlers.go         # 订单簿处理器
├── binance_handlers.go           # Binance 业务处理器
└── integrity/                    # 完整性保障核心模块 ⭐
    ├── handler.go                # IntegrityHandler 主入口
    ├── config_parser.go          # 配置解析（新增）
    ├── types.go                  # 核心类型定义
    ├── sequence_engine.go        # 序列引擎
    ├── buffer.go                 # 消息缓冲区
    ├── dedupe.go                 # 去重引擎
    ├── gate.go                   # 门控策略
    └── scheduler.go              # 补数调度器
```

### 2.2 数据流向

```
WebSocket/HTTP 数据源
      ↓
┌─────────────────────────────────────────────────────┐
│  Parser Handler (binance_parser)                    │
│  - 解析原始 payload                                  │
│  - 提取序列号、symbol 等字段                          │
│  - 将解析结果放入 msg.Metadata                       │
└─────────────────────────────────────────────────────┘
      ↓
┌─────────────────────────────────────────────────────┐
│  Integrity Handler (integrityAdapter)               │
│  ┌───────────────────────────────────────────────┐  │
│  │  IntegrityHandler                             │  │
│  │  ┌─────────────────────────────────────────┐ │  │
│  │  │  SequenceEngine (序列引擎)              │ │  │
│  │  │  - 接收 Event                           │ │  │
│  │  │  - 检测序列连续性                       │ │  │
│  │  │  - 调用 Buffer/Dedupe/Gate              │ │  │
│  │  │  - 触发 Scheduler 补数                  │ │  │
│  │  └─────────────────────────────────────────┘ │  │
│  │        ↓           ↓           ↓              │  │
│  │   Buffer      Dedupe       Gate               │  │
│  │   (缓冲)      (去重)      (门控)              │  │
│  │        ↓           ↓           ↓              │  │
│  │         Scheduler (补数调度器)                │  │
│  │              ↓                                │  │
│  │         BackfillCmd → Role.backfillCh        │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
      ↓
┌─────────────────────────────────────────────────────┐
│  Business Handler (orderbook_diff/trade_normalizer) │
│  - 依赖 parser 解析好的 metadata                     │
│  - 执行业务逻辑                                      │
└─────────────────────────────────────────────────────┘
      ↓
   Kafka/ClickHouse
```

---

## 3. 核心组件详解

### 3.1 IntegrityHandler (handler.go)

**职责**：Integrity 模块的统一入口，串联各个子模块。

```go
type IntegrityHandler struct {
    cfg       Config            // 完整配置
    engine    *SequenceEngine   // 序列引擎
    scheduler Scheduler         // 补数调度器
}
```

**核心方法**：

- `Handle(msg *types.Message) ([]*types.Message, error)`
  - 将消息转换为 Event
  - 调用 SequenceEngine 处理
  - 返回允许下发的消息列表

- `OnSnapshotApplied(lastSeq uint64) []*types.Message`
  - 接收快照应用完成通知
  - 释放缓冲区中大于 lastSeq 的消息
  - 由 orderbook handler 在应用快照后调用

- `SetBackfillTarget(name string, ch chan<- types.BackfillCmd)`
  - 注册补数通道（snapshot/diff/default）
  - 由 Role 框架在初始化时注入

**事件模型**：

```go
type Event struct {
    Seq        uint64           // 序列号（必须）
    RangeStart uint64           // 范围起始（可选，用于 binance depth）
    HasRange   bool             // 是否包含范围字段
    StreamKey  string           // 流分区键（多流场景）
    MessageID  string           // 消息唯一标识（去重用）
    Message    *types.Message   // 原始消息
    Arrival    time.Time        // 到达时间
}
```

---

### 3.2 SequenceEngine (sequence_engine.go)

**职责**：序列控制的核心逻辑，维护序列状态、检测间隙、触发补数。

```go
type SequenceEngine struct {
    cfg         Config
    streams     map[string]*streamState  // 支持多流分区
    rangeEval   RangeEvaluator           // 序列匹配策略
    scheduler   Scheduler                // 补数调度器
    gate        Gate                     // 门控
    deduper     *deduper                 // 去重器
    buffer      *buffer                  // 缓冲区
}
```

**核心流程**：

1. **消息到达**：
   ```go
   func (e *SequenceEngine) Handle(evt *Event) Decision
   ```
   - 根据 StreamKey 分流（多交易对场景）
   - 检查是否重复（调用 deduper）
   - 判断序列关系：连续、乱序、缺失
   
2. **序列判断逻辑**：
   ```
   if evt.Seq == expected:
       → 立即下发，尝试释放缓冲区
   
   elif evt.Seq < expected:
       → 过期消息，丢弃
   
   elif evt.Seq > expected:
       → 检测间隙大小
       → 小间隙（<= EagerGap）：立即触发补数，缓冲当前消息
       → 大间隙（> EagerGap）：缓冲消息，等待延迟触发
   ```

3. **补数触发策略**：
   - **Eager（急切）**：间隙 ≤ EagerGap 时立即补
   - **Delayed（延迟）**：等待 MaxDelay 后仍未连续则补
   - **Hard Timeout（硬超时）**：超过 HardTimeout 强制下发缓冲消息，跳过间隙

4. **Profile 配置**：
   - `generic`：标准序列匹配 `evt.Seq == expected`
   - `binance_depth`：范围匹配 `evt.RangeStart <= expected && evt.Seq >= expected`

---

### 3.3 Buffer (buffer.go)

**职责**：缓存乱序到达的消息，按序列号索引，支持范围查询和过期清理。

```go
type buffer struct {
    mu         sync.RWMutex
    buckets    map[uint64]*bucket  // seq → bucket
    maxBuckets int
    ttl        time.Duration
    sweepEvery time.Duration
}

type bucket struct {
    seq     uint64
    msgs    []*Event
    birth   time.Time
}
```

**核心方法**：

- `Store(evt *Event)`：按序列号存储消息
- `RangeReady(start, end uint64) []*Event`：提取 [start, end] 范围内的消息
- `sweepExpired()`：定期清理过期桶

**设计要点**：

- 每个序列号对应一个 bucket，支持同序列多消息
- 自动 GC 过期消息，防止内存泄漏
- 读写锁保护并发访问

---

### 3.4 Dedupe (dedupe.go)

**职责**：基于 MessageID 的滑动窗口去重。

```go
type deduper struct {
    mu   sync.RWMutex
    seen map[string]time.Time  // messageID → 首次见到的时间
    ttl  time.Duration
}
```

**去重策略**：

1. 从 Event.MessageID 提取唯一标识
2. 检查是否在 TTL 窗口内见过
3. 过期条目自动清理

**使用场景**：

- 补数重试可能导致重复消息
- WebSocket 重连后可能收到重叠数据

---

### 3.5 Gate (gate.go)

**职责**：根据业务场景控制消息下发时机。

```go
type Gate interface {
    ShouldPass(evt *Event) bool
    OnSnapshotApplied(lastSeq uint64) []*Event
    Buffer(evt *Event)
}
```

**门控策略**：

1. **noopGate**（默认）：
   - 所有消息直接放行
   - 无缓冲，无阻塞

2. **snapshotHoldGate**：
   - 在快照加载前阻塞所有增量消息
   - 快照应用后一次性释放
   - 适用于订单簿场景

3. **finalityGate**：
   - 区块链场景，等待 N 个确认块
   - 防止链重组导致数据不一致

---

### 3.6 Scheduler (scheduler.go)

**职责**：补数命令调度和多通道路由。

```go
type Scheduler interface {
    Schedule(cmd types.BackfillCmd) bool
    RegisterTarget(name string, target Target)
}

type simpleScheduler struct {
    targets map[string]Target  // snapshot/diff/default
}
```

**路由策略**：

```go
func preferredOrder(cmd types.BackfillCmd) []string {
    switch cmd.Type {
    case types.BackfillTypeSnapshot:
        return []string{"snapshot"}
    case types.BackfillTypeRange:
        return []string{"diff", "queue"}
    default:
        return nil
    }
}
```

**补数类型**：

- `BackfillTypeSnapshot`：请求快照数据
- `BackfillTypeRange`：请求 [start, end] 范围数据
- `BackfillTypeSingle`：请求单个序列号

---

### 3.7 Config & ConfigParser (types.go, config_parser.go)

**职责**：集中管理所有配置项，支持灵活的策略组合。

```go
type Config struct {
    Profile  string    // generic / binance_depth
    Keys     KeysConfig
    Sequence SequenceConfig
    Buffer   BufferConfig
    Dedupe   DedupeConfig
    Gate     GateConfig
    Backfill BackfillConfig
}
```

**配置解析**（已提取到独立文件 `config_parser.go`）：

```go
// ParseConfig 从 map[string]any 解析配置
func ParseConfig(cfg map[string]any) (Config, error)
```

**关键配置项**：

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `sequence_field` | 序列号字段名 | - |
| `eager_gap` | 立即补数的间隙阈值 | 3 |
| `max_delay_ms` | 延迟补数等待时间 | 800ms |
| `hard_timeout_ms` | 强制下发超时 | 3000ms |
| `bucket_ttl_ms` | 缓冲区过期时间 | 3000ms |
| `dedupe_enabled` | 是否启用去重 | false |
| `gate_mode` | 门控模式 | "" (noop) |

---

## 4. 接口与协议

### 4.1 Handler 框架接口

```go
// Handler 责任链节点接口
type Handler interface {
    Handle(msg *types.Message) ([]*types.Message, error)
}

// BackfillCommandAware 标记 handler 支持回补命令通道注入
type BackfillCommandAware interface {
    SetBackfillChannel(ch chan<- types.BackfillCmd)
}

// SnapshotListener 接收快照应用完成的通知
type SnapshotListener interface {
    OnSnapshotApplied(lastSeq uint64) []*types.Message
}

// SnapshotListenerAware 表示 handler 能够感知快照完成事件
type SnapshotListenerAware interface {
    SetSnapshotListener(listener SnapshotListener)
}
```

**接口设计原则**：

1. `Handler` 是基础接口，所有 handler 必须实现
2. `BackfillCommandAware` 是可选接口，由 Role 框架检测并注入
3. `SnapshotListenerAware` 用于 orderbook 和 integrity 的协作

---

### 4.2 IntegrityAdapter 适配器

```go
// integrityAdapter 将 integrity.IntegrityHandler 包装成 handler.Handler
type integrityAdapter struct {
    handler *integrity.IntegrityHandler
}

// 实现 Handler 接口
func (a *integrityAdapter) Handle(msg *types.Message) ([]*types.Message, error)

// 实现 BackfillCommandAware 接口
func (a *integrityAdapter) SetBackfillChannel(ch chan<- types.BackfillCmd)

// 实现 SnapshotListener 接口
func (a *integrityAdapter) OnSnapshotApplied(lastSeq uint64) []*types.Message
```

**适配器优势**：

- integrity 包内部不依赖 handler 框架
- 清晰的依赖边界
- 方便单元测试

---

## 5. 典型使用场景

### 5.1 Binance 订单簿（带快照恢复）

**配置示例**：

```yaml
handlers:
  - type: binance_parser
    kind: depth
  
  - type: integrity
    profile: binance_depth
    sequence_field: final_update_id
    range_start_field: first_update_id
    eager_gap: 2
    max_delay_ms: 500
    gate_mode: snapshot_hold
    dedupe_enabled: false
  
  - type: orderbook_diff
    symbol: BTCUSDT
    max_depth: 100
```

**处理流程**：

1. Parser 提取 `firstUpdateId`, `finalUpdateId`, `prevFinalUpdateId`
2. Integrity 检测序列连续性（范围匹配）
3. 发现间隙 → 触发 snapshot 补数
4. Gate 阻塞所有消息直到快照应用
5. Orderbook 应用快照后调用 `OnSnapshotApplied(lastSeq)`
6. Integrity 释放缓冲区中的增量消息

---

### 5.2 区块链数据（带确认机制）

**配置示例**：

```yaml
handlers:
  - type: integrity
    profile: generic
    sequence_field: block_number
    eager_gap: 1
    gate_mode: finality
    finality_blocks: 12
    dedupe_enabled: true
    message_id_fields: [block_hash, tx_index]
```

**处理流程**：

1. 区块到达 → 检测连续性
2. 发现间隙 → 立即补数
3. Gate 缓冲区块，等待 12 个确认块
4. 确认后释放，防止链重组

---

### 5.3 交易数据（简单序列）

**配置示例**：

```yaml
handlers:
  - type: binance_parser
    kind: trade
  
  - type: integrity
    profile: generic
    sequence_field: trade_id
    eager_gap: 5
    max_delay_ms: 1000
    dedupe_enabled: false
  
  - type: trade_normalizer
    symbol: ETHUSDT
```

**处理流程**：

1. Parser 提取 `trade_id`
2. Integrity 检测间隙
3. 间隙 ≤ 5 立即补数
4. 无门控，连续消息直接下发

---

## 6. 架构优化总结

### 6.1 已完成的优化

1. **✅ 接口文件合并**
   - 删除 `backfill.go` 和 `snapshot_listener.go`
   - 统一到 `handler.go`，作为框架核心接口

2. **✅ Parser 职责分离**
   - `binance_parser.go` 已存在且完善
   - 业务 handler 不再直接调用解码函数
   - orderbook/binance_handlers 依赖 metadata 传递

3. **✅ 配置解析提取**
   - 创建 `integrity/config_parser.go`
   - `integrity_handler.go` 瘦身至 ~60 行
   - 清晰的适配器模式

4. **✅ 代码职责清晰**
   ```
   parser/    → 解析原始数据，填充 metadata
   integrity/ → 保障数据完整性
   business/  → 执行业务逻辑
   ```

### 6.2 架构优势

1. **高内聚低耦合**
   - integrity 包独立，无外部依赖
   - 通过接口与框架解耦

2. **易测试**
   - 每个组件可独立单测
   - 适配器模式便于 Mock

3. **可扩展**
   - 新增门控策略：实现 `Gate` 接口
   - 新增补数方式：实现 `Target` 接口

4. **配置灵活**
   - Profile 支持多种序列匹配策略
   - 参数可按场景调优

---

## 7. 配置责任链示例

### 7.1 正确配置顺序

```yaml
handlers:
  # 1. Parser 必须在最前面
  - type: binance_parser
    kind: depth
  
  # 2. Integrity 在 Parser 后、业务 Handler 前
  - type: integrity
    profile: binance_depth
    sequence_field: final_update_id
    range_start_field: first_update_id
    eager_gap: 2
  
  # 3. 业务 Handler 依赖前面的处理结果
  - type: orderbook_diff
    symbol: BTCUSDT
    max_depth: 100
```

### 7.2 错误配置（会报错）

```yaml
handlers:
  # ❌ 缺少 parser，业务 handler 会报错：
  # "binance_depth 未找到，请确保上游 binance_parser 已配置"
  - type: orderbook_diff
    symbol: BTCUSDT
```

---

## 8. 监控与可观测性

### 8.1 日志输出点

1. **SequenceEngine**：
   - 序列连续/乱序/缺失
   - 补数触发原因（eager/delayed/hard_timeout）
   - 缓冲区释放情况

2. **Buffer**：
   - 当前缓冲消息数
   - 过期桶清理

3. **Scheduler**：
   - 补数命令调度成功/失败
   - 路由目标选择

### 8.2 关键指标

- `sequence_gap_detected`：间隙检测次数
- `backfill_triggered`：补数触发次数
- `messages_buffered`：缓冲区消息数
- `messages_delivered`：下发消息数
- `duplicates_dropped`：去重丢弃数

---

## 9. 未来扩展方向

### 9.1 短期优化

1. **持久化缓冲区**
   - 当前缓冲区在内存，重启丢失
   - 可考虑 Redis/RocksDB 持久化

2. **补数重试策略**
   - 当前失败立即丢弃
   - 可增加指数退避重试

### 9.2 长期演进

1. **分布式协调**
   - 多 worker 场景需要协调序列状态
   - 引入 etcd/consul 分布式锁

2. **动态配置热更新**
   - 当前需要重启生效
   - 支持运行时调整参数

3. **智能补数**
   - 根据间隙大小选择批量补数
   - 降低补数开销

---

## 10. 总结

Integrity 模块通过精心的架构设计，实现了数据完整性保障的核心功能：

- ✅ **职责清晰**：每个组件单一职责
- ✅ **接口解耦**：通过接口隔离变化
- ✅ **配置灵活**：支持多种场景
- ✅ **易于扩展**：开闭原则
- ✅ **可维护性高**：代码结构清晰

该架构已在 Binance 订单簿、交易数据等场景验证，能够有效保证数据流的顺序性和完整性。

---

**文档版本**：v2.0  
**更新日期**：2025-11-02  
**维护人**：DataPlatform Team





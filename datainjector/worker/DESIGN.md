# Worker 设计文档（最小可用版）

本设计实现一个与“Input → Queue → Handlers → Sink”思想一致的最小可用 Worker，当前内置示例 role：

- role_id: `localnode-block`
  - emitter: `polling`（基于定时器）
  - caller: `sdk_call(LocalGetBlock)` → 拉取区块与交易
  - handler: `dex_parser` → 组装 `transaction + events[]`
  - sink: `kafka`(`dex_transaction`)
- role_id: `account-balance-snapshot`
  - emitter: `polling`
  - caller: `balance_snapshot` → 批量查询账户 Token/LP 余额（单次 batch 返回）
  - handler: `balance_parser` → Redis 价格补全 + 数值归一化
  - sink: `kafka`(`account_balance_snapshot`)
- role_id: `mockprovider-websocket`
  - emitter: `single` → 维护订阅链路并周期拉取缓存
  - caller: `native_call(websocket)` → 长连订阅 + 支持本地/HTTP 补数
  - handler: `missing_detector` → 管理乱序/缺失、自动触发补数
  - sink: `kafka`(`chain.ethereum.blocks`)

目标：以最少概念实现“配置驱动 + 可扩展”的骨架，使后续能平滑扩展 Source/Handler/Sink，而本次仅实现 Pull（Trigger+Caller）路径中的 `polling` + `sdk_call` 调用。

## 核心抽象

- Message：内部统一消息结构，承载 caller 返回的 0~N 条数据（支持 JSON/bytes）。
- Emitter：输入端（Push 或 Pull）。当前支持 `polling` 与 `single`（一次订阅 + 周期拉取）。
- Caller：一次性调用（HTTP/SDK/WebSocket）。当前支持 `sdk_call`、`balance_snapshot`、`native_call(websocket)`。
- Queue：有界通道（隔离取数与处理）。
- Handler：责任链（可选），当前提供 `dex_parser`（交易事件解析）、`balance_parser`（余额快照归一化）。
- Sink：数据下沉接口，支持 `kafka`（默认写入 `dex_transaction` topic），保留 `console` 作为调试选项。
- Role：一个独立任务单元，持有各组件并管理生命周期。

## 运行流程

1) Emitter（polling）使用 `time.Ticker` 每隔 `polling_interval` 触发一次 `Caller.CallOnce(ctx, args)`。
2) Caller 返回 0~N 条 Message，写入有界 Queue（丢弃策略：阻塞等待；后续可配置 drop/timeout）。
3) Worker 从 Queue 消费，依次经过 Handler 链（本次空实现），最终交给 Sink（console 打印）。
4) Role 可平滑停机（context 取消、goroutine 退出）。

## 扩展点与约束

- Emitter：后续可新增 `source`（websocket/webhook）实现 `Start(ctx, out)`，与 `polling` 并行存在。
- Caller：注册表模式（name → factory），当前支持 `sdk_call(LocalGetBlock)`、`balance_snapshot`（批量余额查询），后续可加 `http`（模板化请求）。
- Handler：保持无状态/幂等，可扩展 `sequence_detector`、`validator_schema` 等，本次提供 `dex_parser`。
- Sink：注册表模式，本次提供 `kafka` 实现，后续扩展 Kafka 配置（acks、DLQ 等）。
- 配置：最小 YAML，与上层更完整模板兼容（可扩展 input/handlers/sink 结构，支持多 Role 并行）。

## 配置（本次最小版）

```yaml
roles:
  - role_id: "localnode-block"
    emitter: "polling"
    polling_interval: 2   # 秒
    caller: "sdk_call"
    caller_class: "LocalGetBlock"
    caller_params:
      rpc_endpoint: "http://localhost:8545"
      chain_id: "local"
      confirmations: 0
      max_blocks_per_poll: 5
    handlers:
      - type: "dex_parser"
        with:
          chain_id: "local"
    sink:
      type: "kafka"
      with:
        brokers: ["localhost:9092"]
        topic: "dex_transaction"
        key_from: ["chain_id", "tx_hash"]
    queue: { size: 5000 }

  - role_id: "account-balance-snapshot"
    emitter: "polling"
    polling_interval: 60
    caller: "balance_snapshot"
    caller_params:
      rpc_endpoint: "http://localhost:8545"
      chain_id: "31337"
      deployment_path: "./datainjector/deployment.json"
    handlers:
      - type: "balance_parser"
        with:
          redis_addr: "localhost:6379"
    sink:
      type: "kafka"
      with:
        brokers: ["localhost:9092"]
        topic: "account_balance_snapshot"
        key_from: ["chain_id", "account_id", "asset_type", "biz_id"]
    queue: { size: 5000 }

  - role_id: "mockprovider-websocket"
    emitter: "single"
    caller: "native_call"
    caller_config:
      protocol: "websocket"
      url: "ws://localhost:8090/ws"
    caller_params:
      subscribe: "newHeads"
      heartbeat_ms: 30000
      reconnect:
        backoff_base_seconds: 2
        backoff_max_seconds: 60
      poll_interval_ms: 500
    sink:
      type: "kafka"
      with:
        brokers: ["localhost:9092"]
        topic: "chain.ethereum.blocks"
        key_from: ["chain_id", "block_number"]
    queue: { size: 5000 }

  - role_id: "mockprovider-http-backfill"
    emitter: "kafka_command"
    emitter_config:
      brokers: ["localhost:9092"]
      topic: "command.mockprovider.block"
      group_id: "worker.mockprovider.http"
    caller: "native_call"
    caller_config:
      protocol: "http"
      datasource_id: "mockprovider"
      timeout_ms: 5000
    sink:
      type: "kafka"
      with:
        brokers: ["localhost:9092"]
        topic: "chain.ethereum.blocks"
        key_from: ["chain_id", "block_number"]
    queue: { size: 5000 }
```

## 数据模型（简化）

- Message
  - Metadata：map[string]any（例如 `chain_id`、`block_number`、`tx_hash` 或 `account_id`）。
  - Payload：
    - 交易链路：与历史 listener 对齐（`transaction + events[]`）。
    - 余额链路：`{account_id, observed_time, block_id, asset_type, biz_id, amount, price_usd, value_usd, ...}`。

```json
{
  "transaction": {
    "blockNumber": 151,
    "blockHash": "0x...",
    "timestamp": 1759461369211,
    "transactionHash": "0x...",
    "transactionIndex": 0,
    "transactionStatus": "success",
    "gasUsed": 103260,
    "gasPrice": "1000000010",
    "nonce": 4,
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
      "blockNumber": 151,
      "topics": ["0x..."],
      "eventData": "0x...",
      "decodedArgs": {"from": "0x...", "to": "0x...", "value": "..."}
    }
  ]
}
```

## 目录结构

```
datainjector/worker/
  cmd/worker/main.go        # 入口
  internal/
    config/config.go        # YAML 解析 + 校验
    emitter/polling.go      # 定时触发器
    caller/
      caller.go             # 接口 + 注册表
      sdk_local_get_block.go# LocalGetBlock 示例实现
      balance_snapshot.go   # 批量余额查询 caller
      native_call.go        # WebSocket/HTTP 原生协议 caller
    protocol/jsonrpc.go     # JSON-RPC 通用结构体
    protocol/websocket.go   # WebSocket 连接/心跳/重连管理
    protocol/http_client.go # HTTP JSON-RPC 客户端 + 连接池
    queue/queue.go          # 有界队列（基于 chan）
    handler/handler.go      # 责任链接口 + registry
    handler/dex_parser.go   # DEX 交易解析
    handler/balance_parser.go # 余额补价解析
    sink/console.go         # 控制台下沉
    sink/kafka.go           # Kafka sink
    role/role.go            # Role 装配与生命周期管理
  configs/config.yaml       # 示例配置（多 role 并行）
```

## 缺失检测与补数据设计（唯一共识）

本节定义在 datainjector/worker 内部落地的“数据缺失检测 + 补数据”方案，不依赖 unified-worker 代码，实现目标是在仅考虑严格递增的单序列场景（如区块 `block_number` 连续 +1）下，实现自动检测缺口、触发补拉、与主数据流闭环合并，确保下游只看到严格有序的数据。

### 范围与约束
- 实现范围：仅在 datainjector/worker 模块内实现，不依赖 unified-worker。
- 序列假设：严格递增的单序列，下一条为 `last+1`。
- 补数据通道：
  - WebSocket（快速补数）：用于本地快速检测到的小范围缺口，采用多次 `eth_getBlockByNumber` 请求补齐。
  - HTTP（控制面下发）：用于较大范围的批量补数任务，同样采用多次 `eth_getBlockByNumber`。
- 不做分区：不按 `chain_id` 等字段分区，整个 handler 维护单一序列与缓冲。
- 去重策略：临时以 `block_number` 为唯一键进行简单去重。
- 最大 gap：配置最大缺口跨度 `max_gap`，超过阈值直接报错（防止无限补数）。
- 标记策略：快速补数（WS）路径下，sink 不依赖 `is_backfill` 标记；是否带标记不影响快速补数正确性与下游逻辑。

### 组件与数据契约
- 关键字段：
  - `sequence_field`: 通过 role 配置显式指定（现阶段示例设置为 `block_number`，类型为 int64）。
  - `id_field`: 可选，用于未来增强 dedupe，目前不使用。
- 数据封装：
  - 实时 WS 订阅：JSON-RPC `eth_subscription`，区块头放在 `params.result`。
  - HTTP/WS 补数：仍使用 `eth_getBlockByNumber`，多次单 block 拉取，封装对齐实时格式（`params.result`）。
  - 标记建议：HTTP 路径可打 `is_backfill=true` 用于观测；WS 快速补数对 sink 不要求依赖该标记。

### Handler：MissingDetector（缺失检测 + 缓冲）
- 核心状态：
  - `expectedNext`：当前最小可接受序号；每次成功下游后自增。
  - `buffer[seq][]`：乱序暂存桶，保留同高度的多变体。
  - `firstSeen[seq]`：序号首见时间，用于软/硬超时判断。
- 核心规则：
  1. **等于** `seq == expectedNext`：立即下游，`expectedNext++`，之后持续冲刷 `buffer[expectedNext]` 直至缺口消失。
  2. **更大** `seq > expectedNext`：追加到对应桶并记录 `firstSeen`；若 `seq - expectedNext > eagerGap` 则触发本地回补（范围不超过 `maxRange`）。
  3. **更小** `seq < expectedNext`：认为是重复或旧变体，直接丢弃（如需保留，可调整策略）。
  4. **软超时** `now - firstSeen[expectedNext] > maxDelay`：对 `[expectedNext, expectedNext+maxRange]` 触发补数（优先 WS，失败可降级 HTTP）。
  5. **实时预算**：若 `seenMax - expectedNext > maxGap` 或 `now - firstSeen[expectedNext] > hardTimeout`，提升 `expectedNext` 至 `max(expectedNext+1, seenMax - maxGap)`，同时丢弃被跨越区间（交由控制面后补）。
- 周期清理（每 `sweepInterval`）：
  - 删除 `now - firstSeen[seq] > bucketTTL` 的过期桶。
  - 若 `len(buffer) > maxBuckets`，从最小序号起依次尝试单点回补，再清理多余桶。
- BackfillCmd 会携带有序的 `Options`（WS → HTTP），确保角色侧可按照优先级尝试。

### Role 集成：命令通道与调用
- 构建时注入 `cmdCh chan BackfillCmd` 至 MissingDetector。
- Role 监听命令后，按 `Options` 顺序依次尝试回补：
  - WebSocket 本地快速补数：逐块调用 `eth_getBlockByNumber`，维持与订阅一致的 envelope。
  - HTTP 回补：同样逐块 JSON-RPC 请求，与 WS 输出结构完全对齐。
- 任一方案成功后回写主队列；失败则自动尝试下一传输层，全部失败会记录日志并等待外部控制面补齐。

### Caller 要求与封装对齐
- WebSocket 订阅与 pull：保持 `eth_subscription` envelope，补数消息与实时流一致。
- HTTP 回补：重用 `eth_getBlockByNumber`，输出封装与 WS 完全一致；补数元数据通过 `is_backfill`、`source` 等字段标识。

### 观测与保护
- 通过日志/指标记录缺口检测、触发窗口、补数成功率、跳过区间（hardTimeout）等关键事件。
- 乱序桶超限/老化会主动清理并尝试补数，避免内存与延迟基线被拖垮。

### 配置示例（草案）
```yaml
roles:
  - role_id: "mockprovider-websocket"
    emitter: "single"
    caller: "native_call"
    caller_config:
      protocol: "websocket"
      url: "ws://localhost:8090/ws"
    caller_params:
      subscribe: "newHeads"
      heartbeat_ms: 30000
      poll_interval_ms: 500
    handlers:
      - type: "missing_detector"
        with:
          sequence_field: "block_number"
          eager_gap: 3                # 乱序跨度超过 3 立即触发快速补数
          max_range: 20               # 单次本地补数的最大跨度
          max_delay_ms: 800           # 软超时，超过则主动补数
          hard_timeout_ms: 3000       # 硬超时允许跳过缺口
          max_gap: 8                  # 实时预算：最大未确认序列差
          sweep_interval_ms: 200      # 定期清理 / 老化周期
          bucket_ttl_ms: 3000         # 缓存桶过期时间
          max_buckets: 2000           # 缓存桶最大数量
          backfill:
            ws:
              enabled: true
              rpc_method: "eth_getBlockByNumber"
              include_full_tx: false
            http:
              enabled: true
              endpoint: "http://localhost:8545"
              rpc_method: "eth_getBlockByNumber"
    sink:
      type: "kafka"
      with:
        brokers: ["localhost:9092"]
        topic: "chain.ethereum.blocks"
        key_from: ["block_number"]
    queue: { size: 5000 }
```

### 时序闭环
1) 实时数据通过 WS 到达 → MissingDetector → 若连续则转下游。
2) 发现缺口 → MissingDetector 推送 BackfillCmd(start,end) 到 `cmdCh`。
3) Role 监听到 cmd → 选择 WS 或 HTTP 逐块补数（`eth_getBlockByNumber`）。
4) Caller 返回补数消息，封装与实时一致 → 回写 Role 队列。
5) MissingDetector 从 buffer 中与新到补数一起拼接连续段 → 出队。
6) 下游只看到严格有序的输出；快速补数路径对 sink 不要求依赖 `is_backfill`。

以上设计即为当前唯一共识，后续改动请先更新本节文档再推进实现。

## 关闭语义

- 使用 `context.Context` 贯穿：ticker stop、调用超时、消费者退出。
- main 捕获 `SIGINT/SIGTERM`，触发优雅关闭。

## 误差与边界

- 首版不包含乱序/缺失检测与回补；后续以 Handler 形式补充。
- Caller 失败重试策略：本版简单日志 + 下轮重试；后续加入退避与熔断。
- 队列满：先阻塞等待（避免丢数），后续可引入 backpressure/metrics。

## 运行方式

```
go run ./datainjector/worker/cmd/worker --config ./datainjector/worker/configs/config.yaml
```

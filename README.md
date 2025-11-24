# **System Overview**

## **Project Introduction**

This project is a **real-time cryptocurrency data platform**.

The system integrates multiple heterogeneous data sources—including on-chain transactions (DEX), centralized exchanges (Binance, Hyperliquid), and market data providers (CMC, QuickNode)—to deliver an end-to-end solution covering data ingestion, real-time processing, and intelligent applications.

### Partial chart display

kline and perp (data from binance stream and hyperliquid), on-chain Kanban (data from local trading simulator), ai investment assistant

<img width="1080" height="509" alt="截屏2025-11-24 12 35 30" src="https://github.com/user-attachments/assets/a19d62e0-f443-4512-9d2f-a752e5516bc9" />
<img width="1116" height="470" alt="截屏2025-11-24 12 35 40" src="https://github.com/user-attachments/assets/1bf06aca-c0a5-4b21-a634-2b6725c34a5b" />

<img width="389" height="554" alt="截屏2025-11-24 12 37 00" src="https://github.com/user-attachments/assets/842f8544-3f92-401b-a448-6f906018e403" />

## **System Architecture**

```mermaid
graph TB
    subgraph "Data Source Layer"
        DS1[On-chain Data<br/>DEX/ERC20]
        DS2[Centralized Exchanges<br/>Binance/Hyperliquid]
        DS3[Market Data<br/>CMC/QuickNode]
        DS4[Simulated Sources<br/>MockProvider/LocalNode]
    end

    subgraph "Data Ingestion Layer - Injector"
        C[Control Plane<br/>Task Scheduling / Rate Limiting / State Management]
        W[Worker Cluster<br/>Config-driven / Data Integrity Assurance]
        C -.Task Dispatch / Status Reporting.-> W
    end

    subgraph "Message Queue"
        K[Kafka Topics]
    end

    subgraph "Data Processing Layer - Aggregator"
        F1[On-chain Data Processing<br/>PnL / Token Metrics / Account Balances]
        F2[Kline Analysis<br/>Technical Indicators / Signal Generation]
        F3[Perpetual Contracts Analysis<br/>Execution / Context / Market Congestion]
    end

    subgraph "Data Storage Layer"
        clickhouse
        postgresql
        redis
        paimon
    end

    subgraph "Data Application Layer"
        API[RESTful API]
        FE[Visualization Frontend]
        AG[AI Agent<br/>LangGraph-based]
    end

    DS1 --> W
    DS2 --> W
    DS3 --> W
    DS4 --> W
    W --> K
    K --> F1
    K --> F2
    K --> F3
    F1 --> 数据存储层
    F2 --> 数据存储层
    F3 --> 数据存储层
    数据存储层 --> API
    API --> FE
    API --> AG

    style C fill:#fff4e1
    style W fill:#e1f5ff
    style K fill:#ffe0b2
    style F1 fill:#f3e5f5
    style F2 fill:#f3e5f5
    style F3 fill:#f3e5f5
    style AG fill:#fce4ec

```

# Data Ingestion Layer

### Highlights

- **Separation of control plane and data plane:**
    - The control plane is responsible for task lifecycle management (dispatch, retry, status tracking), global rate limiting, and data quality checks.
    - The data plane pulls data from sources in a **configuration-driven** way.
- **Configuration-driven unified worker architecture:** Ingestion tasks can be switched on/off independently. New requirements usually do not require modifying existing code. Worker instances are not tightly bound to any specific communication protocol.
- **High reliability — four key mechanisms:** Data integrity guard module, timestamp-based delayed scheduling, task state management with multi-level retries, and multi-level rate limiting.

## Overall Architecture

```mermaid
graph TD
  subgraph source
      mockprovider
      simulator
      simulator--send trades-->dex
      subgraph localnode
        dex
      end
      subgraph real_nodes[Real Nodes]
        binance[binance: spot, perp]
        other[other: cmc, quicknode]
        hyperliquid
      end
  end

  task_dispatch[Task Dispatch]-->control_plane[Control Plane]
  task_dispatch--config-driven dispatch-->worker
  control_plane<--task dispatch / status report-->worker
  worker-->topic[(Downstream Topic)]
  topic-->job[Sequence Generator Job]
  control_plane--subscribe-->job
  topic-->stream_proc[Stream Processing]
  source--data ingestion-->worker

```

## Real Data Sources

- Binance spot stream
- Binance perp stream
- Hyperliquid
- CoinMarketCap
- QuickNode

## Simulated Data Sources

Besides real nodes, the project introduces simulated data sources to better reproduce rare events that occur in production.

### `localnode`

- **Self-hosted DEX:**
    
    A local DEX built on a Hardhat node, modeled after Uniswap V2 and implemented with Solidity 0.8.x.
    
- **Trade simulator:**
    - Simulates common patterns on real DEXes: minting tokens, adding liquidity to pools, executing trades, etc.
    - Accounts are tagged as CEX, smart money, public figure, fresh wallet, etc. Per-tag behavioral patterns are not yet differentiated.
    

### `MockDataProvider`

Used to validate **non-functional behavior** of the ingestion layer. Core modules:

- **dataGenerator:** mock data generator
- **faultInjector:** fault injector
    - **HTTP fault injection:** request failures (retryable and non-retryable)
    - **WebSocket fault injection:** connection drops, missing messages, heartbeat anomalies
    - **Other faults:** e.g., chain reorg

---

## Worker Architecture

```mermaid
graph LR
    A[Emitter<br/>Trigger] --trigger--> R[Role Instance]
    F[Resource] --acquire resource--> R
    R --trigger--> B[Caller<br/>Source Invocation]
    B -->|messages| C[Queue<br/>Buffering Queue]
    C -->|dequeue| D[Handler Chain<br/>Processing Pipeline]
    D -->|processed| E[Sink<br/>Data Sink]

    style A fill:#e8f5e9
    style B fill:#fff9c4
    style C fill:#ffe0b2
    style D fill:#f3e5f5
    style E fill:#fce4ec

```

### Component Descriptions

- **Role:** The execution unit of a task. Tasks can be elastically assigned to worker instances, in line with cloud-native principles.
- **Emitter:**
    
    Controls when tasks are triggered, including:
    
    - `Polling` — time-based / periodic
    - `Single` — triggered by configuration change
    - `KafkaCommand` — event-driven
- **Resource:** Unified abstraction for resources. Before a role invokes a caller, it must acquire the required resources, such as HTTP connection pools, WebSocket connection slots, rate-limit tokens, etc.
- **Protocol:** Low-level protocol management, responsible for managing individual connections (e.g., WebSocket reconnection, heartbeats, etc.).
- **Caller：** Business adaptation layer that uses the protocol layer to fetch data.
    - **NativeCall (HTTP/WS):** raw protocol calls with simple parameter assembly.
    - **SDKCall (e.g., go-ethereum):** custom caller classes that invoke SDK APIs.
- **Queue:** Bounded channel that decouples data collection from downstream processing.
- **Handler:** Chain-of-responsibility data processing pipeline. Handlers are business-agnostic.

### Example Configuration

```yaml
roles:
  - role_id: "binance-perp-btc-orderbook"
    emitter: "single"
    caller: "native_call"
    caller_config:
      protocol: "websocket"
      url: "wss://fstream.binance.com/ws"  # Live data source (default)
      # url: "ws://localhost:8090/ws/binance/btcusdt@depth@100ms"  # Comment out the line above and enable this when using mock-data-provider
      datasource_id: "binance.perp.depth"
      rate_limit:
        capacity: 80        # allow short bursts but stay under 2400 req/min (~40 rps)
        refill_rate: 30     # conservative vs Binance 40 rps limit
    caller_params:
      message_format: "binance"
      streams:
        - "btcusdt@depth@100ms"
      metadata_fields:
        symbol: "data.s"
        event_time: "data.E"
    handlers:
      - type: "binance"
        with:
          kind: "depth"
      - type: "integrity"
        with:
          profile: "binance_depth"
          sequence_field: "final_update_id"
          range_start_field: "first_update_id"
          stream_key_field: "binance_symbol"
          gate_mode: "snapshot_hold"
          eager_gap: 20
          max_range: 5
          max_delay_ms: 1500
          hard_timeout_ms: 4000
          max_gap: 200
          sweep_interval_ms: 400
          bucket_ttl_ms: 6000
          max_buckets: 500
          backfill_cooldown_ms: 15000
          backfill:
            http:
              enabled: true
              endpoint: "https://fapi.binance.com/fapi/v1/depth"  # Live backfill
              # endpoint: "http://localhost:8090/fapi/v1/depth"   # Mock backfill
              method: "GET"
              query:
                symbol: "BTCUSDT"
                limit: "500"
      - type: "orderbook_diff"
        with:
          symbol: "BTCUSDT"
          max_depth: 200
      - type: "orderbook_validator"
    sink:
      type: "kafka"
      with:
        brokers:
          - "localhost:9092"
        topic: "perp.orderbook"
        key_from: ["symbol", "exchange"]
    queue: { size: 5000 }

```

## Data Integrity Module

**Core objective:**

Ensure **integrity**, **ordering**, and **idempotency** for streaming data, handling common production issues like network jitter, out-of-order messages, and missing data.

### Architecture

```mermaid
graph TB
    subgraph "Data Input"
        IN[Raw Message Stream<br/>WebSocket / HTTP]
    end

    subgraph "IntegrityHandler - Unified Entry"
        IN --> IH[IntegrityHandler<br/>Message parsing + event construction]
    end

    subgraph "SequenceEngine - Core Engine"
        IH --> SE[SequenceEngine<br/>Ordering control]

        SE --> C1{Message state}
        C1 -->|seq == expected| EQ[onEqual<br/>direct dispatch + drain buffer]
        C1 -->|seq < expected| DROP[Drop<br/>already processed old message]
        C1 -->|range covers expected| COV[onCover<br/>range coverage + drain buffer]
        C1 -->|gap detected| GAP[onGap<br/>buffer + trigger backfill]
    end

    subgraph "Buffer - Out-of-order Cache"
        GAP --> BUF[ReorderBuffer<br/>cache out-of-order messages by seq]
        BUF --> SW[Sweep Cleaner<br/>TTL + capacity constraints]
    end

    subgraph "Backfill - Recovery Scheduler"
        GAP --> BF[BackfillScheduler<br/>backfill decision]
        BF --> BF1{Backfill type}
        BF1 -->|Snapshot| SNAP[Snapshot backfill<br/>e.g. full Binance orderbook]
        BF1 -->|Range| RANGE[Range backfill]
        SNAP --> HTTP[HTTP / WebSocket<br/>backfill request]
        RANGE --> HTTP
        HTTP --> BACK[Backfill messages reinjected]
        BACK --> SE
    end

    subgraph "Gate - Release Control"
        EQ --> GATE[Gate<br/>release valve]
        COV --> GATE
        GATE --> G1{Gate mode}
        G1 -->|snapshot_hold| SH[Wait for snapshot<br/>then release diffs]
        G1 -->|finality| FIN[Wait N blocks<br/>to avoid reorg issues]
        G1 -->|none| PASS[Direct pass-through]
    end

    subgraph "Dedupe - Idempotency Filter"
        SH --> DD[Deduper<br/>MessageID-based dedupe]
        FIN --> DD
        PASS --> DD
        DD --> OUT[Downstream output]
    end

    style SE fill:#ffcccc
    style BUF fill:#fff4e1
    style BF fill:#e1f5ff
    style GATE fill:#ffe1f5
    style DD fill:#e8f5e9

```

### Design Highlights

### 1. Three-dimensional integrity guarantees

- **Ordering (Sequence):** Strict sequence-number based ordering, with support for range coverage checks (e.g., Binance depth’s `first_update_id` → `final_update_id`).
- **Integrity (Completeness):** Detect gaps and automatically trigger backfill. Supports:
    - Snapshot backfill (e.g., orderbooks)
    - Range backfill (e.g., blockchain blocks)
- **Idempotency (Dedupe):** Dedup based on `MessageID` or `StreamKey + Seq` within a TTL window to automatically filter duplicates.

### 2. Smart backfill strategy

**Trigger conditions:**

- **EagerGap:**
    
    When the gap exceeds a threshold (e.g., 20), immediately trigger backfill.
    
- **MaxDelay:**
    
    Soft timeout (e.g., 1.5s). If the expected message isn’t received during the waiting window, trigger backfill.
    
- **HardTimeout:**
    
    Hard timeout (e.g., 4s). Allows skipping over gaps and continuing processing.
    

**Backfill types:**

- **Snapshot backfill:**
    
    For orderbooks like Binance depth, request a full snapshot. With `snapshot_hold` gate, diffs are buffered until the snapshot is applied, then released.
    
- **Range backfill:**
    
    For blockchain data, request the missing [start, end] block range. Supports multi-channel recovery (HTTP / WebSocket / RPC).
    

**Cooldown mechanism:** The same gap/range will not be backfilled repeatedly within a cooldown window (e.g., 15s), preventing backfill storms.

### 3. Gate-based release control

Three modes:

- **`snapshot_hold`:** For orderbooks. Block all diff messages until a snapshot is applied, then release the buffered diffs in order.
- **`finality`:** For blockchain data. Wait for N blocks (e.g., 12) of confirmation before releasing messages to avoid inconsistencies from chain reorgs.
- **`none`:** No special gating; messages pass through after sequence checks.

### 4. Out-of-order cache (`ReorderBuffer`)

**Core mechanisms:**

- Cache out-of-order messages in buckets keyed by sequence number.
- `drain` operation: starting from `expected`, continuously take all dispatchable messages.
- Dual constraints:
    - TTL (e.g., 3s)
    - Maximum capacity (e.g., 2000 buckets)
- Periodic cleanup:
    - `sweep_interval` (e.g., 400ms) to clean expired / over-capacity buckets.

### 5. Configuration-driven profile mechanism

**Built-in profiles:**

- **`generic`:** default, generic parameters
- **`binance_depth`:** for Binance orderbook, enabling range coverage checks, `snapshot_hold` gate, snapshot backfill
- **`chain_blocks`:** for blockchain data, enabling `finality` gate (e.g., 12 confirmations)

**Flexible configuration example:**

```yaml
handlers:
  - type: "integrity"
    with:
      profile: "binance_depth"
      sequence_field: "final_update_id"
      range_start_field: "first_update_id"
      stream_key_field: "binance_symbol"
      gate_mode: "snapshot_hold"
      eager_gap: 20              # trigger immediate backfill if gap > 20
      max_range: 5               # at most 5 records per backfill
      max_delay_ms: 1500         # 1.5s soft timeout
      hard_timeout_ms: 4000      # 4s hard timeout
      max_gap: 200               # max tolerable gap during streaming
      sweep_interval_ms: 400     # 400ms sweep interval
      bucket_ttl_ms: 6000        # 6s TTL
      max_buckets: 500           # up to 500 buckets
      backfill_cooldown_ms: 15000 # 15s backfill cooldown

```

### 6. Performance & reliability

- **Bounded memory usage:**
    
    Buffer has both capacity and TTL constraints to prevent leaks.
    
- **Lock-free core path:**
    
    Core logic can run single-threaded to avoid lock contention.
    
- **Backfill resilience:**
    
    Supports multiple backfill channels (HTTP / WebSocket / RPC) with automatic fallback.
    
- **State isolation:**
    
    Each stream maintains its own independent state.
    

## Layered Rate Limiting

### Strategy: Two-level rate limiting (Control Plane + Worker)

- **Background:** After reviewing rate-limit documentation for Binance, CMC, etc., I found that APIs differ widely: weighted limits, per-endpoint limits, different time windows, and implicit throttling (e.g., short-term bursts).
- **Strategy:** configuration-driven and hierarchical rate limiting
    - **Configuration-driven:** Flexible configuration of scope, weights, and time granularity.
    - **Hierarchical:**
        - Monthly granularity: periodic checks and alerting are sufficient.
        - Finer granularities:
            - Control plane enforces **global** limits.
            - Worker-local rate limiting smooths local bursts.

## Delayed Task Dispatch Based on Persisted Timestamps

### Core Idea

**Approach:**

Persist tasks into PostgreSQL, and use a periodic scanner (`TimerProducer`) to fetch tasks whose `scheduled_time` has arrived and push them to Kafka. This provides **delayed scheduling** and **reliable delivery**.

- **Highlights**
    - **No task loss:**
        
        Either the task is persisted, or the request fails fast to the caller.
        
    - **Delayed scheduling:**
        
        Implemented via timer + batched DB scan + Kafka dispatch.
        
    - **Rate-limit coupling:**
        
        When rate limiting is hit, the scheduled time can be computed and set directly to the next allowable time.
        

### End-to-end Flow

```mermaid
sequenceDiagram
    autonumber
    participant API as REST API
    participant MP as MainProcessor
    participant RL as RateLimiter<br/>(Redis)
    participant TS as TaskScheduler
    participant DB as PostgreSQL
    participant TP as TimerProducer<br/>(Scheduled Scan)
    participant K as Kafka
    participant W as Worker

    Note over API,W: Phase 1: Task creation & persistence
    API->>MP: POST /tasks<br/>{dataSourceId, payload}
    MP->>RL: checkRateLimit(dataSourceId, cost)

    alt Rate limit allowed
        RL-->>MP: allowed = true
        MP->>MP: scheduledTime = now()
    else Rate limit rejected
        RL-->>MP: allowed = false, resetTime
        MP->>MP: scheduledTime = resetTime
        Note over MP: Delay scheduling to the end of the rate-limit window
    end

    MP->>TS: createTask(request)
    TS->>DB: INSERT INTO tasks<br/>SET scheduled_time = scheduledTime<br/>status = PENDING
    DB-->>TS: task saved
    TS-->>MP: TaskResponse{taskId, scheduledTime}
    MP-->>API: 202 Accepted

    Note over API,W: Phase 2: Periodic scan & dispatch
    loop Every 1000ms
        TP->>TP: @Scheduled(fixedDelay = 1000)
        TP->>DB: SELECT * FROM tasks<br/>WHERE status = PENDING<br/>AND scheduled_time <= now() + 5s<br/>ORDER BY priority DESC<br/>LIMIT 1000
        DB-->>TP: List<Task>

        loop For each task
            TP->>TP: if scheduledTime <= now()
            TP->>DB: UPDATE tasks<br/>SET status = PROCESSING<br/>WHERE task_id = ?
            TP->>K: send(http.tasks, taskId, payload)
            K-->>TP: ack
            Note over TP: Task dispatched, waiting for Worker execution
        end
    end

    Note over API,W: Phase 3: Worker execution
    K->>W: consume(http.tasks)
    W->>W: Execute HTTP request

```

## Task State Management & Retry Mechanism

**Approach:** Two-level retry architecture:

1. **Fast local retry in the worker** — handles transient network issues.
2. **Exponential backoff retry in the control plane** — driven by worker status reports.

```mermaid
graph LR
    subgraph "Worker Local Retry (Fast)"
        W1[Receive Task] --> W2{HTTP Request}
        W2 -->|Success 200| W3[Report SUCCESS]
        W2 -->|429 / 503| W4{retryCount < localMax?}
        W4 -->|Yes| W5[Wait 500ms]
        W5 --> W2
        W4 -->|No| W6[Report RETRY<br/>retryable = true]
    end

    subgraph "Control Plane Retry (Exponential Backoff)"
        C1[StatusListener<br/>Receive Status] --> C2{status?}
        C2 -->|SUCCESS| C3[Mark SUCCESS]
        C2 -->|FAILED<br/>retryable = false| C4[Mark FAILED<br/>no further retry]
        C2 -->|RETRY<br/>retryable = true| C5{retryCount < maxRetry?}
        C5 -->|Yes| C6[retryCount++<br/>status = PENDING<br/>scheduledTime = now + delay]
        C5 -->|No| C7[Mark FAILED<br/>exceeded max retries]
        C6 --> C8[Wait for TimerProducer<br/>to redispatch]
    end

    W3 --> C1
    W6 --> C1
    C8 --> W1

    style W1 fill:#e8f5e9
    style C1 fill:#fff9c4
    style C6 fill:#ffebee
    style W4 fill:#e3f2fd
    style C5 fill:#e3f2fd

```

# Streaming Layer

- Main pipeline: **Kafka → Flink → ClickHouse**.
    
    StarRocks + Paimon are also wired in as streaming lakehouse sinks, but currently used only as storage (no production consumers yet).
    
- Currently three main business domains:
    - On-chain data processing
    - Kline (candlestick) analytics
    - Perpetual futures analytics

## On-chain Data Processing

### Architecture

```mermaid
graph TB
    subgraph "Source Layer"
        K1[Kafka: dex_transaction<br/>DEX trade events]
        K2[Kafka: account_balance_snapshot<br/>Account balance snapshots]
        R[Redis<br/>Metadata + token metrics]
    end

    subgraph "Standardized Operator Layer"
        F1[UnifiedFilterOperator<br/>Event filtering]
        F2[EventEnrichmentMap<br/>Metadata enrichment]
        F3[RedisTokenMetricsBroadcaster<br/>Realtime token metrics broadcast]
    end

    subgraph "Business Processing Layer"
        J1[TradeFactJob<br/>Trade fact table]
        J2[PnLAggregatorJob<br/>Account PnL aggregation]
        J3[TokenMetricAggregatorJob<br/>Token-level metrics]
        J4[AccountBalanceJob<br/>Account balance tracking]
    end

    subgraph "Storage Layer"
        C1[ClickHouse<br/>ch_account_trade_fact]
        C2[ClickHouse<br/>ch_account_pnl_current_ma<br/>ch_pnl_realized_event]
        C3[ClickHouse<br/>token_recent_metric_ch]
        C4[ClickHouse<br/>ch_account_balance_snapshot]
    end

    K1 --> F1
    K2 --> J4
    R --> F2
    R --> F3

    F1 --> F2
    F2 --> F3
    F3 --> J1
    F3 --> J2
    F3 --> J3
    F3 --> J4

    J1 --> C1
    J2 --> C2
    J3 --> C3
    J4 --> C4

    style F1 fill:#fff4e1
    style F2 fill:#ffe1f5
    style F3 fill:#e1ffe1
    style J1 fill:#ffcccc
    style J2 fill:#ccffcc
    style J3 fill:#ccccff
    style J4 fill:#ffccff

```

### Design Highlights

- **Standardized pre-processing pipeline:**
    
    All jobs share a unified set of upstream operators (filtering → enrichment → token metrics broadcast).
    
- **Minimal state:**
    
    The PnL operator keeps only 6 fields per account–token pair, resulting in very small state size and high throughput.
    
- **Hierarchical windowing:**
    
    Uses incremental aggregation across window levels, avoiding repeated computation.
    
- **Dual-stream alignment:**
    
    Snapshot + delta streams work together to ensure correctness and up-to-date data.
    

## Standardized Operator Layer

- **UnifiedFilterOperator**
    
    Unified event extraction based on configurable filter strategies.
    
- **EventEnrichmentMap**
    
    Injects metadata into raw messages (account, token, pair, etc.).
    
- **RedisTokenMetricsBroadcaster**
    
    Uses Flink broadcast state to enrich events with token metrics (price, mcap, etc.) from Redis.
    

## `PnLAggregatorJob` – Account PnL Analytics

**Core algorithm:** Moving Average Cost

```mermaid
graph LR
    RB[Upstream processing]-->ATE
    subgraph "PnL Computation Layer"
        ATE[AccountTradeExtractor<br/>Extract account trade events]
        ATE --> KB[KeyBy<br/>account_id + token_id]
        KB --> PP[PnLProcessor<br/>Moving average cost]
    end

    subgraph "State Management"
        PP --> ST[ValueState<br/>position / avg_cost / realized_pnl<br/>and other 6 fields]
        ST --> PP
    end

    subgraph "Output Layer"
        PP --> MS[Main output<br/>AccountPnLSnapshot]
        PP --> SS[Side output<br/>PnLRealizedEvent]
        MS --> CH1[(ClickHouse<br/>ch_account_pnl_current_ma)]
        SS --> CH2[(ClickHouse<br/>ch_pnl_realized_event)]
    end

    subgraph "Application Views"
        CH1 --> V1[v_token_macro_latest<br/>NUPL / MVRV / SOPR]
        CH2 --> V1
    end

    style PP fill:#ffcccc
    style ST fill:#fff4e1
    style V1 fill:#e8f5e9

```

### Design Highlights

- **Minimal state design**
    
    For each `(account_id, token_id)` key, only 6 fields are maintained:
    
    `position, avg_cost, realized_cost_usd, realized_proceeds_usd, realized_pnl_usd, last_price_usd`.
    
    This keeps memory usage extremely low and supports real-time computation for millions of accounts.
    
- **Dual-output stream architecture**
    - **Main output:** current position snapshot, used for unrealized PnL and current holdings.
    - **Side output:** realized PnL events on every sell, used for macro indicators like SOPR.
- **Accurate cost tracking**
    - **Buy:** update moving average cost
        
        `avg_cost = (position * avg_cost + buy_qty * buy_price) / (position + buy_qty)`
        
    - **Sell:** compute realized PnL
        
        `realized_pnl = sell_qty * (sell_price - avg_cost)`
        
- **Macro indicator support**
    
    Based on PnL data, compute core on-chain macro metrics (NUPL, MVRV, SOPR) in a way aligned with Nansen / Glassnode style indicators.
    

## `TokenMetricAggregatorJob` – Token-level Metrics

**Core architecture:** Hierarchical windowed aggregation

```mermaid
graph LR
    RB[Upstream processing]-->TE
    subgraph "Event Extraction Layer"
        TE[TokenEventExtractor<br/>Extract token trade events]
        TE --> KB[KeyBy token_id]
    end

    subgraph "Hierarchical Window Aggregation"
        KB --> W1[20s sliding window<br/>base aggregation]
        W1 --> M1[TokenRecentMetric<br/>tag = all / cex / smart / whale / fresh]

        W1 --> W2[1min window<br/>from 20s aggregates]
        W2 --> M2[TokenRecentMetric<br/>1min]

        W2 --> W3[5min window<br/>from 1min aggregates]
        W3 --> M3[TokenRecentMetric<br/>5min]

        W3 --> W4[1h window<br/>from 5min aggregates]
        W4 --> M4[TokenRecentMetric<br/>1h]
    end

    subgraph "Storage Layer"
        M1 --> CH[(ClickHouse<br/>token_recent_metric_ch)]
        M2 --> CH
        M3 --> CH
        M4 --> CH
    end

    subgraph "Query Optimization"
        CH --> P1[Projection: by_tag<br/>tag-based queries]
        CH --> P2[Projection: by_time_range<br/>time range queries]
    end

    style W1 fill:#e1f5ff
    style W2 fill:#fff4e1
    style W3 fill:#ffe1f5
    style W4 fill:#e8f5e9
    style CH fill:#ffcccc

```

### Design Highlights

- **Hierarchical windowing**
    
    Incremental aggregation across 20s → 1min → 5min → 1h windows.
    
    Avoids recomputing from raw events at each level, improving performance by ~3–5x.
    
- **Segmented by participant tags**
    
    For each window, metrics are computed across five segments:
    
    - `all` (all addresses)
    - `cex` (exchange)
    - `smart` (smart money)
    - `whale`
    - `fresh` (fresh wallets)
        
        This supports user-base segmentation and behavior analysis.
        
- **Rich metric set**
    - **Trading metrics:** `txcnt, buy_count, sell_count, volume_usd, buy_pressure_usd`
    - **Price metrics:** `token_price_usd, mcap_usd, fdv_usd, liquidity_usd`
    - **Behavior metrics:** buy/sell ratio, active address count, trading frequency
- **Query performance optimization**
    
    ClickHouse projections are used for:
    
    - tag-first queries
    - time-range-first queries
        
        enabling millisecond-level query latency.
        

---

## `AccountBalanceJob` – Account Balance Tracking

**Core architecture:** Snapshot + delta dual-stream alignment

```mermaid
graph LR
    subgraph "Snapshot Stream - Minute-level"
        S1[Go Service<br/>Full scan per minute]
        S1 --> S2[Kafka: account_balance_snapshot]
        S2 --> S3[Snapshot stream<br/>AccountBalance]
    end

    subgraph "Delta Stream - Realtime"
        D1[Kafka: dex_transaction<br/>Swap / Mint / Burn]
        D1 --> UF[UnifiedFilterOperator<br/>Balance-related events]
        UF --> EM[EventEnrichmentMap<br/>Metadata enrichment]
        EM --> RB[RedisTokenMetricsBroadcaster<br/>Price injection]
        RB --> BD[BalanceDeltaExtractor<br/>Extract balance changes]
        BD --> D2[Delta stream<br/>BalanceDelta]
    end

    subgraph "Dual-stream Alignment Layer"
        S3 --> KB1[KeyBy<br/>account + asset + biz]
        D2 --> KB2[KeyBy<br/>account + asset + biz]
        KB1 --> DSA[DualStreamAligner<br/>CoProcessFunction]
        KB2 --> DSA
        DSA --> ST[ValueState<br/>Snapshot state + delta accumulation]
        ST --> DSA
    end

    subgraph "Storage Layer"
        DSA --> CH1[(ClickHouse<br/>ch_account_balance_snapshot<br/>ReplacingMergeTree)]
    end

    subgraph "Materialized View Layer"
        CH1 --> MV1[mv_holder_balance_latest<br/>Latest two snapshots]
        MV1 --> CH2[(ch_token_holder_balance_latest)]
    end

    subgraph "Application Views"
        CH2 --> V1[v_token_top_holders_latest<br/>Top holders]
        CH2 --> V2[v_token_distribution_minute<br/>Token distribution stats]
        CH2 --> V3[v_token_holder_tag_minute<br/>Tag distribution + delta]
    end

    style DSA fill:#ffcccc
    style ST fill:#fff4e1
    style CH1 fill:#e1f5ff
    style MV1 fill:#ffe1f5

```

### Design Highlights

- **Dual-stream alignment**
    - **Snapshot stream:**
        
        A Go service scans contract state once per minute and emits full account balance snapshots, providing a strong correctness baseline.
        
    - **Delta stream:**
        
        Processes on-chain events in real time, extracting balance deltas to fill the gaps between snapshots.
        
    - **Alignment strategy:**
        
        Keyed by `(account_id, asset_type, biz_id)` and joined via a `CoProcessFunction`. Snapshot takes precedence; deltas are layered on top between snapshots.
        
- **Data consistency guarantees**
    - `ReplacingMergeTree` deduplicates by `block_id`, ensuring only the latest version for each point in time.
    - Projections on `(token, time)` dimensions optimize both token-first and time-first queries.
    - TTL (e.g., 30 days) automatically cleans up historical snapshots to control storage cost.
- **Chained materialized views**
    - `mv_holder_balance_latest`:
        
        Maintains the latest two snapshots of holder balances.
        
    - `v_token_top_holders_latest`:
        
        Top-N holders (e.g., top 100), sorted by balance ratio.
        
    - `v_token_distribution_minute`:
        
        Distribution metrics (holder count, median, top-2 share, fresh-wallet share).
        
    - `v_token_holder_tag_minute`:
        
        Tag distribution and 1-minute change rate for tags like `exchange / smart_money / whale / fresh_wallet`.
        
- **Price enrichment**
    
    Each balance snapshot is enriched with token prices via broadcast state to compute `value_usd`, enabling USD-denominated position analytics.
    
- **RocksDB state backend**
    
    Enables scaling to millions of accounts with controlled memory usage.
    

---

## Kline Analytics

### Architecture

```mermaid
graph TB
    subgraph "Source Layer"
        K1[Kafka: binance.kline<br/>Raw kline data]
    end

    subgraph "Indicator Computation Layer"
        K1 --> KB[KeyBy symbol + interval]
        KB --> T1[Trend indicators<br/>MA / MACD / EMA]
        KB --> T2[Oscillator indicators<br/>RSI / KDJ]
        KB --> T3[Volatility indicators<br/>Bollinger / ATR]
    end

    subgraph "Signal Generation Layer"
        T1 --> SG[SignalGenerator<br/>crossovers / overbought / oversold]
        T2 --> SG
        T3 --> SG
    end

    subgraph "Output Layer"
        SG --> O1[Kafka: kline.signal<br/>Realtime signals]
        SG --> O2[ClickHouse<br/>kline_metrics]
        SG --> O3[ClickHouse<br/>kline_indicator_metrics]
    end

    style T1 fill:#e1f5ff
    style T2 fill:#fff4e1
    style T3 fill:#ffe1f5
    style SG fill:#e8f5e9

```

### Design Highlights

- **Parallel computation of multiple indicator families**
    - Trend: MA / MACD / EMA
    - Oscillator: RSI / KDJ
    - Volatility: Bollinger / ATR
- **Stateful streaming computation**
    
    Maintains price history windows per `(symbol, interval)` key to support sliding-window indicators.
    
- **Realtime signal generation**
    
    Generates buy/sell signals based on indicator thresholds and crossovers (e.g., golden cross / death cross, overbought / oversold).
    
- **Full metric persistence**
    
    All raw indicator values and signals are stored in ClickHouse to support backtesting, strategy evaluation, and research.
    

---

## Perpetual Futures Analytics

### Architecture

```mermaid
graph TB
    subgraph "Source Layer"
        OB[Kafka: perp.orderbook<br/>Orderbook updates]
        TR[Kafka: perp.trades<br/>Trades]
        MI[Kafka: perp.mark_index<br/>Mark price / index price]
        FR[Kafka: perp.funding_rate<br/>Funding rate]
        OI[Kafka: perp.open_interest<br/>Open interest]
    end

    subgraph "Job1: Execution-side Metrics (1s)"
        OB --> OBP[OrderBookProcessor<br/>Rebuild orderbook]
        OBP --> OBS[1s window<br/>spread / depth / imbalance]
        TR --> TRA[1s window<br/>volume / vwap / buy-sell imbalance]
        OBS --> J1[CoProcess join<br/>Compute OFI / impact cost]
        TRA --> J1
        J1 --> E1[ExecutionMetrics<br/>1s-level execution metrics]
    end

    subgraph "Job2: Context-side Metrics (1m)"
        MI --> CST[ContextSnapshotTimer<br/>Maintain latest state]
        FR --> CST
        OI --> CST
        CST --> C1[ContextMetrics<br/>basis / funding_ema / oi_delta]
    end

    subgraph "Job3: Panel Fusion (1m)"
        E1 --> R1[1min rollup<br/>avg / max / sum]
        R1 --> PJ[PanelJoiner<br/>Align fast & slow streams]
        C1 --> PJ
        PJ --> LC[LiquidityRegimeClassifier<br/>THICK / NORMAL / THIN]
        LC --> CS[CrowdingScoreCalculator<br/>T-Digest Z-score]
        CS --> SD[TrendSignalDetector<br/>Crowding / liquidation risk signals]
    end

    subgraph "Storage Layer"
        E1 --> CH1[(ClickHouse<br/>dws_exec_1s)]
        C1 --> CH2[(ClickHouse<br/>dws_perps_ctx_1m)]
        SD --> CH3[(ClickHouse<br/>dws_perps_panel_1m)]
        SD --> CH4[(ClickHouse<br/>perp_signals)]
    end

    style J1 fill:#ffcccc
    style CST fill:#ccffcc
    style CS fill:#ccccff
    style SD fill:#ffe1f5

```

### Design Highlights

- **Fast vs. slow separation**
    - **Execution-side (fast, 1s):**
        
        Focuses on microstructure: liquidity, spread, order book depth, impact cost.
        
    - **Context-side (slow, 1m):**
        
        Focuses on market regime: basis, funding, open interest, crowding.
        
- **Three-stage job pipeline**
    - **Job1:** High-frequency orderbook + trade metrics.
    - **Job2:** Low-frequency context metrics (funding, OI, mark/index).
    - **Job3:** Panel fusion and signal generation across both dimensions.
- **Rich metric system**
    - **Execution-side:** spread, depth, order flow imbalance (OFI), impact cost.
    - **Context-side:** spot–perp basis, funding rate EMA, open interest delta.
    - **Panel layer:** liquidity regime classification, crowding Z-score, liquidation risk signals.
- **Time alignment & state management**
    - `CoProcessFunction` aligns fast and slow streams into a consistent panel.
    - T-Digest maintains rolling 24h distributions for crowding / regime scoring.

# Data Application

## Query Layer

*(Omitted here – this section is a placeholder for API / SQL query patterns over ClickHouse / StarRocks / Paimon.)*

## Frontend

*(Omitted here – placeholder for dashboard / UI usage of the above metrics.)*

## Agent Layer

**Positioning:**The current agent is an MVP. Its purpose is to:

- Demonstrate **end-to-end capability** (ingestion → streaming → storage → application).
- Prove the **practical usability** of the data platform.

It is **not** designed yet for maximum robustness or complexity on the agent side.

### Tech Stack

- **Framework:** LangGraph (prebuilt ReAct-style agent) on top of LangChain.
- **LLM:** DeepSeek.
- **Tooling:** Currently only wrapped backend APIs; **no NL2SQL** yet.
- **Context management:**Short-term memory only. Conversations are keyed by `sessionId`.
    
    When history exceeds a threshold, older messages are summarized into a compressed context.
    

### Typical Scenarios

- **Market microstructure analysis** “Compare the BTCUSDT spread and impact cost on OKX vs Hyperliquid over the last 30 minutes.”
- **Risk monitoring** “List the latest 50 perpetual anomaly signals with `signalLevel ≥ WARNING`.”
- **Cross-market relationship analysis** “Analyze the relationship between ETHUSDT spot returns and perp funding rate / crowding score.”
- **Asset screening** “Find tokens where spot volume is expanding while perp `crowdingScore < 0.4`.”

### Future Improvements

- **Observation + execution feedback loop** Use tools like LangSmith to build evaluation environments, anomaly alerts, and iterative improvement for the agent.
- **Text-to-SQL** Integrate NL2SQL to support free-form querying over ClickHouse tables.
- **Multi-modal output**  Integrate charting libraries (ECharts / Plotly) to automatically render Kline charts, position distributions, etc.
- **Prompt refinement** Improve accuracy on complex, multi-step analytical questions.
- **Multi-agent collaboration** Split roles into:
    - Data Analysis Agent
    - Risk Assessment Agent
    - Trade Execution Agent
        
        and have them coordinate decisions.
        
- **Strategy execution** Integrate with trading APIs to support:
    - automated order placement
    - stop-loss
    - position rebalancing
        
        (with strict risk controls).
        
- **Tooling management**
    
    Improve tool documentation, grouping, and routing to raise tool-usage accuracy.

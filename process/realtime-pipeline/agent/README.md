# 1. 范围 & 总体目标
本文档基于原 aggregator 的迭代目标进行简化：**数据接入层已经把 tx / receipt / swap 事件整合为“包含所有链上原生字段的实时流”**，因此实时作业不再承担事实–事实 join，也不再维护 token / pool 等低频 Broadcast 维表。

## 1.1 当前范围（MVP）

- 链：
    - Ethereum Mainnet（chain_id = 1）
    - Arbitrum One（chain_id = 42161）
- DEX：
    - Uniswap V2
    - Uniswap V3
- Token：
    - 仍以 **5 个代表性 token**（ETH / USDC / USDT + 2 个 alt）作为首批验证对象，schema 不依赖 token 个数，可平滑扩展至所有 token。
- 数据来源：
    - 链上真实节点（QuickNode / 自建），由数据接入层解析并产出 **单一的 enriched swap 流**。

## 1.2 目标

构建一条 **ODS enriched swap → DWD dex swap** 的低延迟链路，聚焦两类能力：

1. **最新用户标签（user tag）**：随时查询、命中缓存即可获得实时标签。
2. **最新价格 / mcap**：保证 swap 事实在秒级被最新价格覆盖，支持实时估值与后续分析。

下游（本设计外）：
- token 流水与分布分析
- account 行为 / 标签反哺
- 实时 PnL、策略回测

---

# 2. Topic & 表设计总览

## 2.1 命名约定

- ODS：`ods_*`
- 维度 / 辅助：`dim_*`
- DWD：`dwd_*`
- 链维度：
    - Ethereum：topic 后缀 `_eth`
    - Arbitrum：topic 后缀 `_arb`

---

# 3. ODS 层（统一 enriched swap 流）Schema

## 3.1 `ods_dex_swap_full_eth` / `ods_dex_swap_full_arb`

> 数据接入层负责从 JSON-RPC / Websocket 解析 tx、receipt、Uniswap V2/V3 swap，并在写入 Kafka 之前完成字段整合。Flink 直接消费全量字段，**无需再与其他事实流做 join**。

**Topic：**

- `ods_dex_swap_full_eth`
- `ods_dex_swap_full_arb`

**Schema（两链一致，仅 chain_id 区别）：**

```sql
ods_dex_swap_full_* (
  -- 基本信息
  chain_id                    INT,
  dex_name                    STRING,            -- 'uniswap'
  dex_version                 STRING,            -- 'v2' / 'v3'

  tx_hash                     STRING,
  log_index                   INT,
  block_number                BIGINT,
  block_timestamp             TIMESTAMP_LTZ(3),

  -- tx / receipt 已整合
  trader_address              STRING,            -- tx.from
  router_address              STRING,            -- tx.to
  status                      TINYINT,
  gas_used                    BIGINT,
  effective_gas_price_wei     BIGINT,
  gas_cost_native             DECIMAL(38, 0),    -- gas_used * effective_gas_price

  -- swap 事件
  pool_address                STRING,
  sender_address              STRING,
  recipient_address           STRING,

  token0_address              STRING,
  token1_address              STRING,
  token0_symbol               STRING,
  token1_symbol               STRING,
  token0_decimals             INT,
  token1_decimals             INT,

  amount0_raw                 DECIMAL(38, 0),
  amount1_raw                 DECIMAL(38, 0),
  amount0_direction           STRING,            -- 'in' / 'out'
  amount1_direction           STRING,            -- 'in' / 'out'

  sqrt_price_x96              DECIMAL(38, 0),    -- v3 可用
  liquidity                   DECIMAL(38, 0),    -- v3 可用
  tick                        INT,               -- v3 可用

  ingestion_time              TIMESTAMP_LTZ(3)
)
```

> token / pool / account 的静态元数据由接入层写入该 topic；实时作业只需关注标签与价格。

---

# 4. 维度与外部服务（仅保留“动态”维度）

## 4.1 用户标签 `dim_account_tag_latest`（L1 cache + L2 Redis）

- **读取方式**：Flink `AsyncLookupFunction`
    - L1：本地 Caffeine 缓存，key=`<chain_id>:<account_address>`
    - L2：Redis Hash
- **逻辑字段**：

```sql
dim_account_tag_latest (
  chain_id            INT,
  account_address     STRING,

  is_whale            BOOLEAN,
  is_smart            BOOLEAN,
  is_bot              BOOLEAN,
  is_cex_deposit      BOOLEAN,
  vip_level           SMALLINT,
  segment             STRING,

  updated_at          TIMESTAMP_LTZ(3)
)
```

> 只需要**当前最新值**，不做按事件时间回看。

## 4.2 最新价格 / mcap `dim_token_price_current`（Kafka upsert topic）

- **写入来源**：load-executor 或外部价格服务。
- **读取方式**：Flink Kafka Source + keyed state 缓存最新值；TTL 由价格更新频率决定（建议 5 分钟）。

```sql
dim_token_price_current (
  chain_id            INT,
  token_address       STRING,

  price_usd           DOUBLE,
  mcap_usd            DOUBLE,
  source              STRING,
  updated_at          TIMESTAMP_LTZ(3),

  PRIMARY KEY (chain_id, token_address)
)
```

> 该表是实时估值的核心，需保证去重与延迟监控。

---

# 5. DWD 层：统一 Swap 明细 `dwd_dex_swap`

**Topic：**

- `dwd_dex_swap`

**Schema：**

```sql
dwd_dex_swap (
  chain_id                    INT,
  dex_name                    STRING,
  dex_version                 STRING,

  tx_hash                     STRING,
  log_index                   INT,
  block_number                BIGINT,
  block_timestamp             TIMESTAMP_LTZ(3),

  pool_address                STRING,
  router_address              STRING,
  trader_address              STRING,
  sender_address              STRING,
  recipient_address           STRING,

  token0_address              STRING,
  token1_address              STRING,
  token0_symbol               STRING,
  token1_symbol               STRING,
  token0_decimals             INT,
  token1_decimals             INT,

  amount_token0_in_raw        DECIMAL(38, 0),
  amount_token0_out_raw       DECIMAL(38, 0),
  amount_token1_in_raw        DECIMAL(38, 0),
  amount_token1_out_raw       DECIMAL(38, 0),

  amount_token0_in            DOUBLE,
  amount_token0_out           DOUBLE,
  amount_token1_in            DOUBLE,
  amount_token1_out           DOUBLE,

  base_token_address          STRING,
  quote_token_address         STRING,
  price_base_in_quote         DOUBLE,

  price_token0_usd            DOUBLE,
  price_token1_usd            DOUBLE,
  swap_value_usd              DOUBLE,
  token0_mcap_usd             DOUBLE,
  token1_mcap_usd             DOUBLE,

  gas_cost_native             DECIMAL(38, 0),
  gas_cost_usd                DOUBLE,

  trader_is_whale             BOOLEAN,
  trader_is_smart             BOOLEAN,
  trader_is_bot               BOOLEAN,
  trader_segment              STRING,

  price_source                STRING,
  account_tag_version         STRING,

  ingestion_time              TIMESTAMP_LTZ(3),

  PRIMARY KEY (chain_id, tx_hash, log_index) NOT ENFORCED
)
```

> 事实表只做“用户标签 + 价格”两件事，其他字段直接透传自 ODS。

---

# 6. Flink Job：`dex_swap_dwd_job`

## 6.1 Job 拆分与设计目标

- **Job 拆分**：目前没有跨链 join / 对齐需求，因此 `ods_dex_swap_full_eth` 与 `ods_dex_swap_full_arb` 分别由独立 Flink Job 处理：
    - `dex_swap_dwd_job_eth`：消费 `ods_dex_swap_full_eth`，产出 ETH 相关记录。
    - `dex_swap_dwd_job_arb`：消费 `ods_dex_swap_full_arb`，产出 Arbitrum 相关记录。
  两个 Job 拥有完全相同的代码逻辑，只是启动参数（chain_id、主topic）不同，便于独立扩缩容、灰度升级。

- **延迟**：秒级；无 tx / receipt join，算子链路更短。
- **重点算子**：
    1. 价格流管理：保障 swap 总能命中最新价格 / mcap。
    2. 用户标签 lookup：高命中率的多级缓存。
    3. 数量归一化 / 估值 / 标签写入。

## 6.2 Source

- Kafka Source：
    - `ods_dex_swap_full_<chain>`（Job 启动时指定链对应的 topic）
    - `dim_token_price_current`
- Redis：
    - `dim_account_tag_latest`（通过 Async lookup）

## 6.3 核心算子流程

1. **主流：enriched swap**
    - 直接解析 `ods_dex_swap_full_*`，按 `(chain_id, tx_hash, log_index)` 赋 event time = `block_timestamp`。
2. **价格 Keyed State 管理**
    - 将 price 流与 swap 流按 `chain_id` keyBy 之后在 `KeyedCoProcessFunction` 内维护 MapState（token_address → {price, mcap, updated_at, source}）。
    - 每条价格更新仅落在对应 key 的并行实例上，swap 处理时即时读取 MapState；若价格超过 TTL（如 5 分钟）则置空并记录指标，避免 Broadcast 带来的高频 state 复制。
3. **用户标签 Async enrich**
    - 对 `trader_address` 执行异步查找：
        - 命中 L1 cache：直接返回。
        - miss 时发起 Redis 异步请求，拿到数据后写回 L1 cache 并补全记录。
    - 将 tag 的获取延迟与命中率写入 metrics，方便优化 cache。
4. **数量归一化与估值**
    - `ProcessFunction`：
        - 根据 `token*_decimals` 转换 raw amount → double。
        - 按规则确定 base / quote（优先稳定币 / USDC / USDT / ETH）。
        - 从价格状态读取 `token0`、`token1` 的最新价格 / mcap，计算：
            - `price_base_in_quote`
            - `swap_value_usd`
            - `token*_mcap_usd`
        - 计算 `gas_cost_usd = gas_cost_native * native_price`.
        - 若价格缺失，记录默认值并写 metrics。
5. **Sink**
    - 输出到 Kafka `dwd_dex_swap`，启用 Exactly Once。

## 6.4 时间与状态策略

- `block_timestamp` 作为事件时间，允许 ±3 分钟乱序。
- 主流只保留短 TTL（例如 10 分钟）用于处理迟到 swap，避免堆积。
- 价格状态 TTL 由价格更新频率决定（建议更新周期 15s，TTL 5 分钟）。
- 用户标签缓存：
    - L1 TTL：5 分钟（命中短期频繁交易账户）
    - Redis 数据保持最终一致，更新延迟可控。

---

# 7. 关键设计决策

1. **取消事实–事实 join**：输入 topic 已包含 tx / receipt 所需字段，Flink 不再关注链上 JSON-RPC 细节，极大降低状态管理复杂度。
2. **静态维表外移**：token / pool / account 基础信息在接入层写入，不在 Flink 内 broadcast，从而简化部署与资源需求。
3. **核心聚焦用户标签 + 价格**
    - Redis + L1 cache 实现用户标签的低延迟查询，保障策略指标实时刷新。
    - `dim_token_price_current` 作为唯一价格源，通过 state 管理和过期监控保证估值可靠。
4. **单一 DWD 输出**：所有下游都以 `dwd_dex_swap` 为输入，schema 已囊括标签、价格、gas 成本等关键信息，方便扩展更多实时分析。




# **kafka**

**topic:dex_transaction**

```json
{    "transaction": {
      "type": "object",
      "properties": {
        "blockNumber": { "type": "integer" },
        "blockHash":   { "type": "string" },
        "timestamp":   { "type": "integer" },
        "transactionHash": { "type": "string" },
        "transactionIndex":{ "type": "integer" },
        "transactionStatus":      { "type": "string" },
        "gasUsed":     { "type": "integer" },
        "gasPrice":    { "type": "string" },
        "nonce":       { "type": "integer" },
        "fromAddress":        { "type": "string" },
        "toAddress":          { "type": "string" },
        "transactionValue":       { "type": "string" },
        "inputData":   { "type": "string" },
        "chainID":     { "type": "string" }
      },
      "required": ["blockNumber","transactionHash","fromAddress","toAddress","chainID"]
    },
    "events": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "eventName": { "type": "string" },
          "contractAddress": { "type": "string" },
          "logIndex":  { "type": "integer" },
          "blockNumber": { "type": "integer" },
          "topics":    {
            "type": "array",
            "items": { "type": "string" }
          },
          "eventData":      { "type": "string" },
          "decodedArgs": { "type": "object" }
        },
        "required": ["eventName","contractAddress","logIndex"]
      }
    }
  }
}

```

# postgresql

### **account**

```sql
CREATE TABLE account (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    chain_name VARCHAR(100) NOT NULL,
    address     VARCHAR(128) NOT NULL ,
    entity VARCHAR(255),
    tag_bitmap INTEGER NOT NULL DEFAULT 0, -- 位图替换散列的tag字段
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (chain_id,address)
);
```

### **token**

```sql

CREATE TABLE token (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    chain_name VARCHAR(100),
    token_symbol VARCHAR(50) NOT NULL,
    --token类型
    token_catagory VARCHAR(50),
    token_decimals INTEGER,
    token_address VARCHAR(255) NOT NULL,
    --发行商
    issuer VARCHAR(255),
    create_time       TIMESTAMP,
  update_time       TIMESTAMP,
);

CREATE INDEX idx_token_chain_id ON token(chain_id);
CREATE INDEX idx_token_token_symbol ON token(token_symbol);
CREATE INDEX idx_token_token_address ON token(token_address);
CREATE TABLE twswap_factory (
  id                   BIGSERIAL     PRIMARY KEY,
  chain_id             VARCHAR(64),
  factory_address      VARCHAR(128)  NOT NULL,
  time_window          VARCHAR(16),  -- '20s','1min','5min','30min','1h'
  end_time             TIMESTAMP,    -- 窗口结束时间
  pair_count           INT,
  volume_usd           DECIMAL(24,4),
  liquidity_usd        DECIMAL(24,4),
  txcnt                INT,
  update_time          TIMESTAMP,

  UNIQUE (chain_id, factory_address, time_window, end_time)
);

```

## **twswap**
```sql
CREATE TABLE twswap_pair (
  id                     BIGSERIAL     PRIMARY KEY,
  chain_id               VARCHAR(64),
  pair_address           VARCHAR(128)  NOT NULL,
  pair_name  VARCHAR(64),
  token0_id              BIGINT        REFERENCES token(id),
  token1_id              BIGINT        REFERENCES token(id),
  fee_tier               VARCHAR(16)   DEFAULT '0.3%',  -- 目前默认 0.3%
  created_at_timestamp   TIMESTAMP,
  created_at_block_number BIGINT,

  UNIQUE (chain_id, pair_address)
);
CREATE TABLE twswap_pair_metric (
  id                  BIGSERIAL     PRIMARY KEY,
  pair_id             BIGINT        NOT NULL REFERENCES twswap_pair(id) ON DELETE CASCADE,
  time_window         VARCHAR(16),  -- '20s','1min','5min','30min','1h'
  end_time            TIMESTAMP,    -- 窗口截止时间

  token0_reserve      DECIMAL(24,4),
  token1_reserve      DECIMAL(24,4),
  reserve_usd         DECIMAL(24,4),
  token0_volume_usd   DECIMAL(24,4),
  token1_volume_usd   DECIMAL(24,4),
  volume_usd          DECIMAL(24,4),
  txcnt               INT,

  UNIQUE (pair_id, time_window, end_time)
);
```
# clickhouse

## 结构与分组概览
- 核心事实: `ch_account_trade_fact`（Token/Account 双投影，时间倒序）。
- 账户维度: `ch_account_balance_snapshot`（Token 分布与 Holder 上游）、`v_account_trades_detail`。
- Token 维度: `ch_token_distribution_snapshot` + `mv_token_distribution_1min`、`ch_token_holder_balance_minute` + Top/标签视图。
- 聚合指标: `ch_account_trade_minute` + `mv_trade_to_minute`、`token_recent_metric_ch`、`ch_account_pnl_current_ma`。
- 统一规范: `side` 建议使用 'BUY'|'SELL'；`label_mask` 为 `UInt16` 位图，见“地址标签位图”。

### 交易事实 ch_account_trade_fact
— 用途: 账户-Token 粒度的去重交易事实，供 Token/Account 明细页与分钟聚合使用。
— 关键字段: 维度(`chain_id`,`token_id`,`account_id`,`pair_id`,`side`)，时间(`block_time`,`block_id`)，定位(`tx_hash`,`log_index`)，度量(`qty`,`price_usd`,`value_usd`)，标签(`label_mask`)。

```sql
CREATE TABLE IF NOT EXISTS ch_account_trade_fact
(
  -- 维度
  chain_id     UInt32,
  token_id     UInt64,
  account_id   UInt64,
  side         LowCardinality(String),      -- 'BUY' | 'SELL'
  pair_id      UInt64,                      -- 交易对（可做跳转/联动）
  -- 业务时间
  block_time   DateTime,
  block_id     UInt64,
  -- 唯一定位
  tx_hash      String,
  log_index    UInt32,
  -- 度量
  qty          Decimal(38,18),
  price_usd    Decimal(38,18),
  value_usd    Decimal(38,18),
  -- 标签
  label_mask   UInt16 DEFAULT 0,

  -- 常见过滤/排序加速
  INDEX idx_time (block_time) TYPE minmax GRANULARITY 1,

  -- 投影：Token 页面 -> “某 token 的账户交易列表（按时间倒序展示）”
  PROJECTION by_token_time
  (
    SELECT token_id, block_time, account_id, side, qty, price_usd, value_usd, tx_hash, log_index, label_mask
    ORDER BY (token_id, block_time, log_index)
  ),

  -- 投影：Account 页面 -> “某账户的跨 token 交易列表（按时间倒序展示）”
  PROJECTION by_account_time
  (
    SELECT account_id, block_time, token_id, side, qty, price_usd, value_usd, tx_hash, log_index, label_mask
    ORDER BY (account_id, block_time, log_index)
  )
)
ENGINE = ReplacingMergeTree(block_id)              -- 用区块号做去重版本，防重放/迟到
PARTITION BY toYYYYMM(block_time)
ORDER BY (token_id, block_time, log_index, account_id)  -- Token 优先（Token 页面最常见），辅以时间与事件序
TTL block_time + INTERVAL 180 DAY
SETTINGS index_granularity = 8192,deduplicate_merge_projection_mode = 'rebuild';
```

## token维度

— 上游依赖: 多数来源于 `ch_account_balance_snapshot`（资产=ERC20）。

### 宏观资产分布
— 用途: Token 持有人总体分布与集中度的分钟快照。
— 指标: `holders_count`,`median_holder_value_usd`,`top2_share/top2_value_usd`,`fresh_holder_share/fresh_value_usd`,`total/avg_holder_value_usd`。

```sql
CREATE TABLE IF NOT EXISTS ch_token_distribution_snapshot (
  token_id                 UInt64,
  snapshot_time            DateTime,
  holders_count            UInt32,
  median_holder_value_usd  Decimal(24,4),
  top2_share               Float64,
  top2_value_usd           Decimal(24,4),
  fresh_holder_share       Float64,
  fresh_value_usd          Decimal(24,4),
  total_value_usd          Decimal(24,4),
  avg_holder_value_usd     Decimal(24,4),
  version                  UInt64,
  PROJECTION proj_by_time
    (SELECT snapshot_time, token_id, holders_count, top2_share, fresh_holder_share, total_value_usd
     ORDER BY (snapshot_time, token_id))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (token_id, snapshot_time)
TTL snapshot_time + INTERVAL 30 DAY
SETTINGS index_granularity = 4096,
         deduplicate_merge_projection_mode = 'rebuild';

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_token_distribution_1min
TO ch_token_distribution_snapshot
AS
SELECT
    biz_id AS token_id,
    toStartOfMinute(observed_time) AS snapshot_time,
    uniqExactIf(account_id, value_usd > 0)                         AS holders_count,
    quantileExactIf(0.5)(value_usd, value_usd > 0)                 AS median_holder_value_usd,
    sum(value_usd)                                                 AS total_value_usd,
    avgIf(value_usd, value_usd > 0)                                AS avg_holder_value_usd,
    if(uniqExactIf(account_id, value_usd > 0) >= 2,
       toFloat64(arraySum(arraySlice(arrayReverseSort(groupArrayIf(value_usd, value_usd > 0)), 1, 2))) / nullIf(toFloat64(sum(value_usd)), 0),
       if(uniqExactIf(account_id, value_usd > 0) = 1, 1.0, 0))     AS top2_share,
    if(uniqExactIf(account_id, value_usd > 0) >= 2,
       arraySum(arraySlice(arrayReverseSort(groupArrayIf(value_usd, value_usd > 0)), 1, 2)),
       if(uniqExactIf(account_id, value_usd > 0) = 1, maxIf(value_usd, value_usd > 0), 0)) AS top2_value_usd,
    if(sum(value_usd) > 0,
       toFloat64(sumIf(value_usd, bitAnd(label_mask, toUInt16(1)) != 0 AND value_usd > 0)) / toFloat64(sum(value_usd)),
       0)                                                          AS fresh_holder_share,
    sumIf(value_usd, bitAnd(label_mask, toUInt16(1)) != 0 AND value_usd > 0)               AS fresh_value_usd,
    toUnixTimestamp(max(observed_time))                            AS version
FROM ch_account_balance_snapshot
WHERE asset_type = 'ERC20' AND value_usd > 0
GROUP BY token_id, snapshot_time;
```

### holder明细
— 用途: Token-账户分钟级持仓与价值明细，支撑 Top Holder 与标签聚合。
— 去重/版本: `ReplacingMergeTree(version)`，version 取 `block_id` 或 `observed_time` 最大值。

```sql
CREATE TABLE IF NOT EXISTS ch_token_holder_balance_minute (
  token_id     UInt64,
  end_time     DateTime,
  account_id   UInt64,
  amount       Decimal(38,18),
  value_usd    Decimal(38,18),
  label_mask   UInt16,
  version      UInt64,  -- 取 block_id 或 observed_time 的最大值
  -- 为 TopN/分页优化：按 token、时间、价值倒序扫描
  PROJECTION by_token_time_desc
    (SELECT token_id, end_time, account_id, value_usd, amount, label_mask
     ORDER BY (token_id, end_time,  account_id)),
  -- 为账户页回溯优化
  PROJECTION by_account_time
    (SELECT account_id, token_id, end_time, value_usd, amount, label_mask
     ORDER BY (account_id, end_time, token_id))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(end_time)
ORDER BY (token_id, end_time, account_id)
TTL end_time + INTERVAL 90 DAY
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_holder_balance_minute
TO ch_token_holder_balance_minute
AS
SELECT
  biz_id                       AS token_id,
  toStartOfMinute(observed_time) AS end_time,
  account_id,
  argMax(amount,    block_id)  AS amount,
  argMax(value_usd, block_id)  AS value_usd,
  argMax(label_mask,block_id)  AS label_mask,
  max(block_id)                AS version
FROM ch_account_balance_snapshot
WHERE asset_type = 'ERC20'
GROUP BY token_id, end_time, account_id;

---top holder（最近一分钟 Top 持有人）
CREATE OR REPLACE VIEW v_token_top_holders_latest AS
WITH last_minute AS (
  SELECT token_id, max(end_time) AS end_time
  FROM ch_token_holder_balance_minute
  GROUP BY token_id
)
SELECT
  h.token_id,
  h.end_time,
  h.account_id,
  h.value_usd,
  round(h.value_usd / nullIf(sum(h.value_usd) OVER (PARTITION BY h.token_id, h.end_time),0), 6) AS ownership_pct,
  h.amount,
  h.label_mask
FROM ch_token_holder_balance_minute h
INNER JOIN last_minute lm USING (token_id, end_time)
WHERE h.value_usd > 0
ORDER BY h.token_id, h.value_usd DESC
LIMIT 100 BY token_id;

---标签维度（展开 `label_mask` 进行标签聚合）
CREATE OR REPLACE VIEW v_token_holder_tag_minute AS
WITH tags AS (
  /* 这里举例映射：bit0=fresh, bit1=whale, bit2=smart, bit3=cex */
  SELECT arrayJoin([
    ('fresh_wallet', toUInt16(1)),
    ('whale',        toUInt16(2)),
    ('smart_money',  toUInt16(4)),
    ('cex',          toUInt16(8))
  ]) AS t
),
base AS (
  SELECT
    h.token_id,
    h.end_time,
    t.1 AS tag,
    sumIf(h.value_usd, bitAnd(h.label_mask, t.2) != 0 AND h.value_usd > 0)        AS value_usd,
    uniqExactIf(h.account_id, bitAnd(h.label_mask, t.2) != 0 AND h.value_usd > 0) AS holders_count
  FROM ch_token_holder_balance_minute h
  CROSS JOIN tags
  GROUP BY h.token_id, h.end_time, tag
)
SELECT
  token_id,
  end_time,
  tag,
  value_usd,
  holders_count,
  (value_usd - lagInFrame(value_usd) OVER (PARTITION BY token_id, tag ORDER BY end_time))
    / nullIf(lagInFrame(value_usd) OVER (PARTITION BY token_id, tag ORDER BY end_time), 0) AS pct_change_1min
FROM base
ORDER BY token_id, tag, end_time;
```

### 账户交易明细（Token 视角）
— 用途: 展示某 Token 的账户交易明细（常按时间倒序）。

```sql
CREATE OR REPLACE VIEW v_token_trades_detail AS
SELECT
  t.token_id,
  t.block_time,
  t.account_id,
  t.side,
  t.qty,
  t.price_usd,
  t.value_usd,
  t.tx_hash,
  t.log_index,
  t.pair_id,
  t.label_mask
FROM ch_account_trade_fact AS t
-- 常见查询：WHERE token_id = ? AND block_time >= now() - INTERVAL 7 DAY
-- ORDER BY block_time DESC LIMIT 100
--example 
SELECT *
FROM v_token_trades_detail
WHERE token_id = 2
  AND block_time >= now() - INTERVAL 1 DAY
ORDER BY block_time DESC
LIMIT 50
SETTINGS use_projection = 1;

```

## account维度

### 资产分布
— 用途: 账户在 Token/LP 的分钟级余额与估值快照，上游于 Token 宏观与 Holder 明细。
— 索引: `bloom_filter` 与 `minmax`，便于常用过滤。

```sql
CREATE TABLE IF NOT EXISTS ch_account_balance_snapshot (
  account_id       UInt64,
  observed_time    DateTime,--快照时间（通常按区块时间对齐）
  block_id         UInt64,                   -- 版本/去重
  asset_type       LowCardinality(String),   -- 'ERC20'/'LP'
  biz_id           UInt64,                   -- token_id 或 pair_id
  amount           Decimal(38,18),
  price_usd        Decimal(38,18),
  value_usd        Decimal(38,18),
  label_mask       UInt16 DEFAULT 0, --标签位图（Fresh、CEX、Whale、SmartMoney 等）
  INDEX idx_account_time (account_id, observed_time) TYPE bloom_filter() GRANULARITY 1,
  INDEX idx_value_usd     (value_usd)       TYPE minmax        GRANULARITY 1,
  INDEX idx_label_mask    (label_mask)      TYPE bloom_filter() GRANULARITY 1,
  PROJECTION proj_by_token
    (SELECT biz_id, asset_type, account_id, observed_time, amount, value_usd, label_mask
     ORDER BY (biz_id, observed_time, account_id)),
  PROJECTION proj_by_time
    (SELECT observed_time, biz_id, account_id, value_usd, label_mask
     ORDER BY (observed_time, biz_id))
)
ENGINE = ReplacingMergeTree(block_id)
PARTITION BY (asset_type, toYYYYMM(observed_time))
ORDER BY (biz_id, account_id, block_id, observed_time)
TTL observed_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';

```

### token交易明细（Account 视角）
— 用途: 展示某账户的跨 Token 交易明细（常按时间倒序）。

```sql
CREATE OR REPLACE VIEW v_account_trades_detail AS
SELECT
  t.account_id,
  t.block_time,
  t.token_id,
  t.side,
  t.qty,
  t.price_usd,
  t.value_usd,
  t.tx_hash,
  t.log_index,
  t.pair_id,
  t.label_mask
FROM ch_account_trade_fact AS t
-- 常见查询：WHERE account_id = ? AND block_time >= now() - INTERVAL 7 DAY
-- ORDER BY block_time DESC LIMIT 100
--example
SELECT *
FROM v_account_trades_detail
WHERE account_id = 2
  AND block_time >= now() - INTERVAL 7 DAY
ORDER BY block_time DESC
LIMIT 50
SETTINGS use_projection = 1;
```

### account 交易分钟级聚合
— 对象: `ch_account_trade_minute` + 物化视图 `mv_trade_to_minute`。
— 字段: `end_time`,`account_id`,`token_id`,`side`（如存在），`trade_cnt`,`volume_usd`。

```sql
CREATE TABLE IF NOT EXISTS ch_account_trade_minute
(
  end_time   DateTime,
  account_id UInt64,
  token_id   UInt64,
  trade_cnt  UInt32,
  volume_usd Decimal(38,18)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(end_time)
ORDER BY (account_id, end_time, token_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trade_to_minute
TO ch_account_trade_minute AS
SELECT
  toStartOfMinute(block_time) AS end_time,
  account_id,
  token_id,
  count()        AS trade_cnt,
  sum(value_usd) AS volume_usd
FROM ch_account_trade_fact
GROUP BY end_time, account_id, token_id;
```

## 滑动窗口指标

### Token 滑动窗口指标
— 用途: 多时间窗（20s/1min/5min/1h）下的交易统计、买压与价格/市值/流动性等指标。
— 分组键: `token_id`,`time_window`,`tag`,`end_time`；指标包含计数、金额、价格与规模。

```sql
CREATE TABLE IF NOT EXISTS token_recent_metric_ch
(
    token_id UInt64,
    time_window LowCardinality(String),  -- '20s','1min','5min','1h'
    end_time DateTime,
    tag LowCardinality(String),          -- 'all','cex','smart_money','whale','fresh_wallet'
    -- 计数
    txcnt UInt32,
    buy_count UInt32,
    sell_count UInt32,
    -- 金额
    volume_usd Decimal(24,4),
    buy_volume_usd Decimal(24,4),
    sell_volume_usd Decimal(24,4),
    buy_pressure_usd Decimal(24,4),
    -- 价格
    token_price_usd Decimal(24,4),
    mcap_usd Decimal(24,4),
    fdv_usd Decimal(24,4),
    liquidity_usd Decimal(24,4),
    -- 元数据
    process_time DateTime DEFAULT now(),
    create_time  DateTime DEFAULT now(),
    -- Projections
    PROJECTION by_tag
      (SELECT token_id, tag, time_window, end_time, volume_usd, buy_pressure_usd, token_price_usd
       ORDER BY (tag, token_id, end_time)),
    PROJECTION by_time_range
      (SELECT token_id, time_window, end_time, volume_usd, txcnt
       ORDER BY (end_time, token_id))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(end_time)
ORDER BY (token_id, time_window, tag, end_time)
TTL end_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
```

---

## 账户 PnL

### 账户 PnL（最新结构）
— 用途: 账户-Token 维度的持仓、移动加权成本、已/未实现盈亏与 ROI 等（含账户/Token 双投影）。

```sql
CREATE TABLE IF NOT EXISTS ch_account_pnl_current_ma (
    account_id           UInt64,
    token_id             UInt64,
    position             Decimal(38,18),         -- 剩余仓位
    avg_cost             Decimal(38,18),         -- 移动加权成本
    realized_cost_usd    Decimal(38,18),         -- 已实现成本累计
    realized_proceeds_usd Decimal(38,18),        -- 已实现收入累计
    realized_pnl_usd     Decimal(38,18),         -- 已实现盈亏
    last_price_usd       Decimal(38,18),         -- 最新价格
    unrealized_pnl_usd   Decimal(38,18),         -- 未实现盈亏
    total_pnl_usd        Decimal(38,18),         -- 总盈亏
    roi_pct              Float64,                -- 投资回报率（比例）
    holding_pct          Float64,                -- 持仓比例（可选指标）
    last_tx_time         DateTime,               -- 最近交易时间
    version              UInt64,                 -- 去重/排序版本
    -- 索引
    INDEX idx_account_token (account_id, token_id) TYPE bloom_filter() GRANULARITY 1,
    INDEX idx_roi (roi_pct) TYPE minmax GRANULARITY 1,
    INDEX idx_total_pnl (total_pnl_usd) TYPE minmax GRANULARITY 1,
    -- Projections
    PROJECTION proj_by_account
      (SELECT account_id, token_id, position, total_pnl_usd, roi_pct, last_tx_time
       ORDER BY (account_id, last_tx_time, token_id)),
    PROJECTION proj_by_token
      (SELECT token_id, account_id, position, total_pnl_usd, roi_pct, last_tx_time
       ORDER BY (token_id, last_tx_time, account_id))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(last_tx_time)
ORDER BY (account_id, token_id, last_tx_time)
TTL last_tx_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';
```

### 已实现盈亏事件

```sql
CREATE TABLE IF NOT EXISTS ch_pnl_realized_event (
  token_id UInt64,
  account_id UInt64,
  block_id UInt64,
  block_time DateTime,
  realized_qty Decimal(38,18),
  realized_cost_usd Decimal(38,18),
  realized_proceeds_usd Decimal(38,18),
  realized_pnl_usd Decimal(38,18)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (token_id, block_id, account_id)
TTL block_time + INTERVAL 180 DAY;
```

### 聚合态

```sql
CREATE TABLE IF NOT EXISTS ch_token_macro_minute_state (
  token_id UInt64,
  end_time DateTime,
  -- mcap：分钟唯一值
  mcap_max_state          AggregateFunction(max, Decimal(38,4)),
  -- realized cap 近似：Σ(position*avg_cost)
  realized_cap_sum_state  AggregateFunction(sum, Decimal(38,4)),
  -- 未实现盈亏严格拆分
  unreal_profit_sum_state AggregateFunction(sum, Decimal(38,4)),
  unreal_loss_sum_state   AggregateFunction(sum, Decimal(38,4)),
  -- SOPR/Realized PnL
  sopr_proceeds_sum_state AggregateFunction(sum, Decimal(38,4)),
  sopr_cost_sum_state     AggregateFunction(sum, Decimal(38,4)),
  realized_pnl_sum_state  AggregateFunction(sum, Decimal(38,4))
)
ENGINE = AggregatingMergeTree
ORDER BY (token_id, end_time)
TTL end_time + INTERVAL 90 DAY;
```

### MV：来自实时 mcap（1min）

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_macro_from_rt_mcap
TO ch_token_macro_minute_state AS
SELECT
  token_id,
  end_time,
  maxState(toDecimal128(mcap_usd, 4)) AS mcap_max_state,
  -- 其余占位
  sumState(toDecimal128(0, 4)) AS realized_cap_sum_state,
  sumState(toDecimal128(0, 4)) AS unreal_profit_sum_state,
  sumState(toDecimal128(0, 4)) AS unreal_loss_sum_state,
  sumState(toDecimal128(0, 4)) AS sopr_proceeds_sum_state,
  sumState(toDecimal128(0, 4)) AS sopr_cost_sum_state,
  sumState(toDecimal128(0, 4)) AS realized_pnl_sum_state
FROM token_recent_metric_ch
WHERE tag='all' AND time_window='1min' AND mcap_usd IS NOT NULL AND mcap_usd > 0
GROUP BY token_id, end_time;
```

### MV：来自账户快照（RealizedCap + 未实现拆分）

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_macro_from_pnl_snapshot
TO ch_token_macro_minute_state AS
SELECT
  token_id,
  toStartOfMinute(last_tx_time) AS end_time,
  -- realized cap
  sumState(toDecimal128(position * avg_cost, 4)) AS realized_cap_sum_state,
  -- 未实现盈亏严格拆分（仅有效仓/价）
  sumState(toDecimal128(CASE WHEN position > 0 AND last_price_usd > 0 AND avg_cost > 0
                               THEN greatest(position * (last_price_usd - avg_cost), 0)
                               ELSE 0 END, 4)) AS unreal_profit_sum_state,
  sumState(toDecimal128(CASE WHEN position > 0 AND last_price_usd > 0 AND avg_cost > 0
                               THEN greatest(position * (avg_cost - last_price_usd), 0)
                               ELSE 0 END, 4)) AS unreal_loss_sum_state,
  -- 其余占位
  maxState(toDecimal128(0, 4)) AS mcap_max_state,
  sumState(toDecimal128(0, 4)) AS sopr_proceeds_sum_state,
  sumState(toDecimal128(0, 4)) AS sopr_cost_sum_state,
  sumState(toDecimal128(0, 4)) AS realized_pnl_sum_state
FROM ch_account_pnl_current_ma
WHERE position > 0 AND avg_cost > 0
GROUP BY token_id, end_time;
```

### MV：来自已实现事件（SOPR / Realized PnL）

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_macro_from_realized_event
TO ch_token_macro_minute_state AS
SELECT
  token_id,
  toStartOfMinute(block_time) AS end_time,
  sumState(toDecimal128(realized_proceeds_usd, 4)) AS sopr_proceeds_sum_state,
  sumState(toDecimal128(realized_cost_usd, 4))     AS sopr_cost_sum_state,
  sumState(toDecimal128(realized_pnl_usd, 4))      AS realized_pnl_sum_state,
  -- 其余占位
  maxState(toDecimal128(0, 4)) AS mcap_max_state,
  sumState(toDecimal128(0, 4)) AS realized_cap_sum_state,
  sumState(toDecimal128(0, 4)) AS unreal_profit_sum_state,
  sumState(toDecimal128(0, 4)) AS unreal_loss_sum_state
FROM ch_pnl_realized_event
WHERE realized_qty > 0
GROUP BY token_id, end_time;
```

### 终端视图：宏观指标（严格拆分 NUPL）

```sql
CREATE OR REPLACE VIEW v_token_macro_minute AS
SELECT
  token_id,
  end_time,
  round(maxMerge(mcap_max_state), 2)                                  AS mcap_usd,
  round(sumMerge(realized_cap_sum_state), 2)                           AS realized_cap_usd,
  round(sumMerge(realized_cap_sum_state)
      + sumMerge(unreal_profit_sum_state)
      - sumMerge(unreal_loss_sum_state), 2)                            AS network_value_usd,
  round(sumMerge(unreal_profit_sum_state), 2)                          AS unrealized_profit_usd,
  round(sumMerge(unreal_loss_sum_state), 2)                            AS unrealized_loss_usd,
  /* NUPL（严格拆分分子，分母用 network_value） */
  CASE WHEN (sumMerge(realized_cap_sum_state)
          + sumMerge(unreal_profit_sum_state)
          - sumMerge(unreal_loss_sum_state)) > 0
       THEN round((sumMerge(unreal_profit_sum_state) - sumMerge(unreal_loss_sum_state)) /
                  (sumMerge(realized_cap_sum_state)
                 + sumMerge(unreal_profit_sum_state)
                 - sumMerge(unreal_loss_sum_state)), 6)
       ELSE NULL END                                                   AS nupl,
  /* 其他指标 */
  CASE WHEN sumMerge(realized_cap_sum_state) > 0 AND maxMerge(mcap_max_state) > 0
       THEN round(maxMerge(mcap_max_state) / sumMerge(realized_cap_sum_state), 4)
       ELSE NULL END                                                   AS mvrv,
  CASE WHEN sumMerge(realized_cap_sum_state) > 0
       THEN round((sumMerge(realized_cap_sum_state)
                 + sumMerge(unreal_profit_sum_state)
                 - sumMerge(unreal_loss_sum_state)) /
                  sumMerge(realized_cap_sum_state), 4)
       ELSE NULL END                                                   AS nvt_ratio,
  CASE WHEN sumMerge(sopr_cost_sum_state) > 0
       THEN round(sumMerge(sopr_proceeds_sum_state) / sumMerge(sopr_cost_sum_state), 4)
       ELSE NULL END                                                   AS sopr,
  round(sumMerge(realized_pnl_sum_state), 2)                           AS realized_pnl_usd,
  /* 完整性标记 */
  (maxMerge(mcap_max_state) > 0)                                       AS has_mcap,
  (sumMerge(realized_cap_sum_state) > 0)                               AS has_realized_cap,
  (sumMerge(unreal_profit_sum_state) + sumMerge(unreal_loss_sum_state) > 0) AS has_unrealized_pnl,
  (sumMerge(sopr_proceeds_sum_state) > 0)                              AS has_sopr,
  now()                                                                AS last_updated
FROM ch_token_macro_minute_state
GROUP BY token_id, end_time
HAVING has_mcap OR has_realized_cap OR has_unrealized_pnl OR has_sopr
ORDER BY token_id, end_time;
```

### 最新一笔（近 1 天）便捷视图

```sql
CREATE OR REPLACE VIEW v_token_macro_latest AS
SELECT
  token_id,
  max(end_time)                                 AS latest_time,
  argMax(mcap_usd, end_time)                    AS mcap_usd,
  argMax(realized_cap_usd, end_time)            AS realized_cap_usd,
  argMax(network_value_usd, end_time)           AS network_value_usd,
  argMax(nupl, end_time)                        AS nupl,
  argMax(mvrv, end_time)                        AS mvrv,
  argMax(sopr, end_time)                        AS sopr,
  argMax(realized_pnl_usd, end_time)            AS realized_pnl_usd
FROM v_token_macro_minute
WHERE end_time >= now() - INTERVAL 1 DAY
GROUP BY token_id
ORDER BY token_id;
```

# paimon
```sql

-- === Sinks: flat tables for StarRocks-friendly schemas ===
CREATE TABLE IF NOT EXISTS lake_bronze.tx_transaction (
  chain_id STRING,
  block_number BIGINT,
  block_timestamp TIMESTAMP_LTZ(3),
  transaction_hash STRING,
  gas_used BIGINT,
  gas_price STRING,
  nonce BIGINT,
  from_address STRING,
  to_address STRING,
  transaction_value STRING,
  tx_status STRING,
  input_data STRING,
  source STRING,
  ingest_time TIMESTAMP_LTZ(3),
  pt STRING
) PARTITIONED BY (pt)
WITH (
  'write-mode' = 'append-only',
  'file.format' = 'parquet'
);

CREATE TABLE IF NOT EXISTS lake_bronze.tx_events (
  chain_id STRING,
  block_number BIGINT,
  block_timestamp TIMESTAMP_LTZ(3),
  transaction_hash STRING,
  event_name STRING,
  contract_address STRING,
  log_index INT,
  topics_json STRING,
  event_data STRING,
  decoded_args_json STRING,
  source STRING,
  ingest_time TIMESTAMP_LTZ(3),
  pt STRING
) PARTITIONED BY (pt)
WITH (
  'write-mode' = 'append-only',
  'file.format' = 'parquet'
);

```
# redis
### 代币价格
key:
token_price:{address}
### 元数据
key:
pairMetadata
[{"id":"1","address":"0x1d7673a54e7e1d025972f611cb9a46b2e10b146c","token0":{"id":"1","address":"0x74a6379d012ce53e3b0718c05dd72a3de87f0c6a","symbol":"USDC"},"token1":{"id":"2","address":"0xecc540e356b9e7c6e17fea13c6fe192debefb51d","symbol":"WETH"},"chainId":"31337","chainName":"ethereum"}]
key:
accountMetadata
[[{"id":"1","address":"0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266","tag":"cex"}]
Key: 
tokenMetadata
[{"id":"1","address":"0x74a6379d012ce53e3b0718c05dd72a3de87f0c6a","symbol":"USDC","name":"USDC","decimals":"18","chainId":"31337","chainName":"ethereum"}]
### address标签位图
```markdown
Key：label:{chain_id}:{address}
Value（bitset，UInt16）：
1<<0 EX（cex）
1<<1 SM（smart）
1<<2 WH（whale）
1<<3 PF（public）
1<<4 FR（fresh）
1<<5 TP（TopPnL）
```
### account资产分布

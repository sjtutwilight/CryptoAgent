-- ============================================================
-- 模块: 永续合约分析（Perpetual Contract Analytics）
-- 存储: ClickHouse
-- 维护: Aggregator模块
-- 上游Topic: perp.orderbook, perp.trades, perp.mark_index, perp.funding_rate, perp.open_interest
-- 关联Job: PerpExecJob, PerpContextJob, PerpPanelJob
-- ============================================================

-- ========================================
-- 1. 执行面秒级表：订单簿和成交指标
-- ========================================
CREATE TABLE IF NOT EXISTS dws_exec_1s
(
    symbol              LowCardinality(String),      -- 交易对符号（BTCUSDT）
    exchange            LowCardinality(String),      -- 交易所（binance）
    end_time            DateTime,                    -- 秒级窗口结束时间
    algo_version        LowCardinality(String),      -- 算法版本（用于A/B测试）

    -- 订单簿指标
    mid_price           Decimal(18, 8),              -- 中间价
    spread_bps          Float64,                     -- 点差（基点）
    spread_abs          Decimal(18, 8),              -- 绝对点差

    -- 深度指标（累计深度，单位：USD）
    depth_10k           Decimal(18, 2),              -- ±10k USD内的深度
    depth_50k           Decimal(18, 2),              -- ±50k USD内的深度
    depth_100k          Decimal(18, 2),              -- ±100k USD内的深度

    -- 订单簿不平衡
    imbalance_top5      Float64,                     -- 前5档不平衡 (bid-ask)/(bid+ask)
    imbalance_total     Float64,                     -- 总不平衡

    -- 冲击成本（买入X USD需要的滑点，基点）
    impact_10k_bps      Float64,                     -- 10k冲击成本
    impact_50k_bps      Float64,                     -- 50k冲击成本
    impact_100k_bps     Float64,                     -- 100k冲击成本

    -- OFI (Order Flow Imbalance) - L1版本
    ofi                 Float64,                     -- 订单流不平衡（L1版）

    -- 成交指标
    trade_count         UInt32,                      -- 成交笔数
    volume_usd          Decimal(18, 2),              -- 成交量（USD）
    vwap                Decimal(18, 8),              -- 成交均价
    buy_volume_usd      Decimal(18, 2),              -- 主动买入成交量
    sell_volume_usd     Decimal(18, 2),              -- 主动卖出成交量

    -- 流动性指标（可选）
    illiq_lambda        Float64,                     -- Amihud流动性系数 λ

    -- 元数据
    process_time        DateTime DEFAULT now(),

    INDEX idx_time (end_time) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(end_time)
ORDER BY (symbol, exchange, end_time)
TTL end_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- ========================================
-- 2. 语境面分钟级表：标记价格、资金费率、持仓量
-- ========================================
CREATE TABLE IF NOT EXISTS dws_perps_ctx_1m
(
    symbol              LowCardinality(String),      -- 交易对符号
    exchange            LowCardinality(String),      -- 交易所
    end_time            DateTime,                    -- 分钟级窗口
    algo_version        LowCardinality(String),      -- 算法版本

    -- 价格指标
    mark_price          Decimal(18, 8),              -- 标记价格
    index_price         Decimal(18, 8),              -- 指数价格
    basis_bps           Float64,                     -- 基差（基点）(mark-index)/index*10000

    -- 资金费率
    funding_rate        Decimal(18, 8),              -- 当前资金费率
    funding_rate_8h     Decimal(18, 8),              -- 8h资金费率
    funding_ema_24h     Decimal(18, 8),              -- 24h资金费率EMA（在线计算）
    next_funding_time   DateTime,                    -- 下次资金费结算时间

    -- 持仓量
    oi                  Decimal(18, 2),              -- 持仓量（张）
    oi_usd              Decimal(18, 2),              -- 持仓量（USD）
    oi_delta_1m         Decimal(18, 2),              -- 1分钟OI变化（采样差分）
    oi_delta_pct        Float64,                     -- OI变化百分比
    is_oi_carried       Boolean,                     -- OI是否为前值填充

    -- 元数据
    process_time        DateTime DEFAULT now(),

    INDEX idx_time (end_time) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(process_time)
PARTITION BY toYYYYMM(end_time)
ORDER BY (symbol, exchange, end_time)
TTL end_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- ========================================
-- 3. 汇合面板表（Job 3用，当前预留结构）
-- ========================================
CREATE TABLE IF NOT EXISTS dws_perps_panel_1m
(
    symbol              LowCardinality(String),
    exchange            LowCardinality(String),
    end_time            DateTime,
    algo_version        LowCardinality(String),

    -- 执行面聚合（从1s rollup）
    avg_spread_bps      Float64,
    max_spread_bps      Float64,
    avg_depth_50k       Decimal(18, 2),
    avg_impact_50k_bps  Float64,
    avg_imbalance       Float64,
    sum_ofi             Float64,
    volume_usd          Decimal(18, 2),
    trade_count         UInt32,

    -- 语境面
    mark_price          Decimal(18, 8),
    basis_bps           Float64,
    funding_rate        Decimal(18, 8),
    funding_ema_24h     Decimal(18, 8),
    oi_usd              Decimal(18, 2),
    oi_delta_1m         Decimal(18, 2),

    -- 衍生指标
    liquidity_regime    LowCardinality(String),     -- THICK/NORMAL/THIN
    crowding_score      Float64,                    -- 拥挤度得分

    -- 元数据
    process_time        DateTime DEFAULT now(),

    INDEX idx_time (end_time) TYPE minmax GRANULARITY 1,

    -- 投影：按时间查询优化
    PROJECTION by_time
    (
        SELECT end_time, symbol, avg_spread_bps, volume_usd, funding_rate, oi_usd
        ORDER BY (end_time, symbol)
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(end_time)
ORDER BY (symbol, exchange, end_time)
TTL end_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';

-- ========================================
-- 4. 信号表：存储所有检测到的异常信号
-- ========================================
CREATE TABLE IF NOT EXISTS perp_signals
(
    symbol              LowCardinality(String),      -- 交易对符号
    exchange            LowCardinality(String),      -- 交易所
    signal_time         DateTime,                    -- 信号产生时间
    signal_type         LowCardinality(String),      -- EXEC_HEALTH/CROWDING/LIQUIDATION_RISK
    signal_level        LowCardinality(String),      -- INFO/WARNING/CRITICAL

    -- 信号内容
    metric_name         String,                      -- spread_anomaly/depth_thin/funding_extreme
    metric_value        Float64,                     -- 指标值
    threshold           Float64,                     -- 阈值

    -- 上下文
    context_json        String,                      -- 完整上下文JSON

    -- 元数据
    algo_version        LowCardinality(String),      -- 算法版本
    process_time        DateTime DEFAULT now(),

    INDEX idx_time (signal_time) TYPE minmax GRANULARITY 1,
    INDEX idx_type (signal_type) TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(signal_time)
ORDER BY (symbol, signal_type, signal_time)
TTL signal_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;







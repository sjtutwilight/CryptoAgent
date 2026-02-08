-- ============================================================
-- 模块: K线分析（Kline Analytics）
-- 存储: ClickHouse
-- 维护: Aggregator模块
-- 上游Topic: binance.kline
-- 关联Job: KlineMetricsJob, KlineIndicatorJob
-- ============================================================

-- ========================================
-- 1. K线指标表
-- ========================================
CREATE TABLE IF NOT EXISTS kline_metrics
(
    exchange            LowCardinality(String),
    symbol              String,
    interval            LowCardinality(String),
    start_time          DateTime64(3, 'UTC'),
    close_time          DateTime64(3, 'UTC'),
    event_time          DateTime64(3, 'UTC'),
    is_closed           UInt8,
    ingest_time         DateTime64(3, 'UTC'),
    open_price          Decimal(18, 8),
    high_price          Decimal(18, 8),
    low_price           Decimal(18, 8),
    close_price         Decimal(18, 8),
    base_volume         Decimal(18, 8),
    quote_volume        Decimal(18, 8),
    trade_count         UInt32,
    amplitude_pct       Decimal(18, 6),
    change_pct          Decimal(18, 6),
    ma_short_period     UInt16,
    ma_short_value      Decimal(18, 8),
    ma_medium_period    UInt16,
    ma_medium_value     Decimal(18, 8),
    ma_long_period      UInt16,
    ma_long_value       Decimal(18, 8),
    ema_short_value     Nullable(Decimal(18, 8)),
    ema_long_value      Nullable(Decimal(18, 8)),
    signal_type         LowCardinality(String),
    signal_strength     Decimal(10, 6),
    signal_detail       String,
    signal_timestamp    DateTime64(3, 'UTC'),
    process_time        DateTime64(3, 'UTC'),
    create_time         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(process_time)
PARTITION BY toDate(start_time)
ORDER BY (symbol, interval, start_time);

-- ========================================
-- 2. K线技术指标表
-- ========================================
CREATE TABLE IF NOT EXISTS kline_indicator_metrics
(
    exchange        LowCardinality(String),
    symbol          String,
    interval        LowCardinality(String),
    start_time      DateTime64(3, 'UTC'),
    end_time        DateTime64(3, 'UTC'),
    indicator       LowCardinality(String),       -- RSI / MACD / BOLL / ATR / KDJ / MA 等
    variant         LowCardinality(String),       -- 具体参数，如 "period=14", "fast=12_slow=26"
    value           Float64,                      -- 若只有一个主值，如 RSI
    components      Nested (name String, val Float64),  -- 多输出组件（MACD、BOLL、KDJ）
    thresholds      Nested (name String, val Float64),  -- 例如 RSI 超买/超卖、BOLL stddev
    signal_type     Enum8('NONE'=0,'BUY'=1,'SELL'=2),
    signal_strength Float32,
    signal_detail   String,
    extra_tags      Map(LowCardinality(String), String),
    process_time    DateTime64(3, 'UTC'),
    create_time     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(create_time)
PARTITION BY toDate(start_time)
ORDER BY (symbol, interval, indicator, start_time);






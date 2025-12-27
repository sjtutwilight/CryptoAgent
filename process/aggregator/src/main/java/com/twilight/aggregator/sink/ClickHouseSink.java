package com.twilight.aggregator.sink;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.FlinkConfig;
import com.twilight.aggregator.model.PairMetric;
import com.twilight.aggregator.model.TokenRecentMetric;
import com.twilight.aggregator.model.AccountPnLSnapshot;
import com.twilight.aggregator.model.PnLRealizedEvent;
import com.twilight.aggregator.model.TradeFact;
import com.twilight.aggregator.model.KlineMetrics;
import com.twilight.aggregator.model.KlineSignal;
import com.twilight.aggregator.model.IndicatorMetric;

/**
 * ClickHouse高性能批量写入Sink
 * 支持批量写入优化，提升写入性能
 */
public class ClickHouseSink<T> extends RichSinkFunction<T> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseSink.class);
    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final int batchSize;
    private final long flushIntervalMs;
    private final String insertSql;

    private Connection connection;
    private List<T> batch;
    private long lastFlushTime;

    private ClickHouseSink(String tableName, String insertSql, int batchSize, long flushIntervalMs) {
        this.tableName = tableName;
        this.insertSql = insertSql;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.batch = new ArrayList<>();
        this.lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 从配置中获取ClickHouse连接信息
        FlinkConfig config = FlinkConfig.getInstance();
        String url = config.getClickHouseUrl();
        String username = config.getClickHouseUsername();
        String password = config.getClickHousePassword();
        String driverClass = config.getClickHouseDriverClassName();
        
        // 加载驱动
        Class.forName(driverClass);
        
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("socket_timeout", "30000");
        props.setProperty("connection_timeout", "10000");
        
        connection = DriverManager.getConnection(url, props);
        
        log.info("ClickHouseSink initialized for table: {}, url: {}, batchSize: {}, flushInterval: {}ms", 
                tableName, url, batchSize, flushIntervalMs);
    }

    @Override
    public synchronized void invoke(T value, Context context) throws Exception {
        batch.add(value);
        
        // 检查是否需要刷新
        long currentTime = System.currentTimeMillis();
        if (batch.size() >= batchSize || (currentTime - lastFlushTime) >= flushIntervalMs) {
            flush();
        }
    }

    private void flush() throws Exception {
        if (batch.isEmpty()) {
            return;
        }

        int retries = 0;
        int maxRetries = 3;
        long retryDelayMs = 1000;
        Exception lastException = null;

        log.debug("ClickHouseSink: Flushing batch of {} records to table {}", batch.size(), tableName);

        while (retries < maxRetries) {
            try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
                
                // 批量设置参数
                for (T value : batch) {
                    setParameters(stmt, value);
                    stmt.addBatch();
                }

                // 执行批量插入
                int[] results = stmt.executeBatch();
                
                log.debug("ClickHouseSink: Successfully inserted {} records to {}", results.length, tableName);
                
                // 清空缓存并更新时间
                batch.clear();
                lastFlushTime = System.currentTimeMillis();
                return; // 成功执行，退出方法
                
            } catch (SQLException e) {
                lastException = e;
                retries++;
                log.warn("Error writing to ClickHouse table {} (attempt {}/{}): {}", 
                        tableName, retries, maxRetries, e.getMessage());

                if (retries < maxRetries) {
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting to retry ClickHouse operation", ie);
                    }
                }
            }
        }

        // 如果所有重试都失败，记录错误并抛出异常
        log.error("Failed to write to ClickHouse table {} after {} attempts", tableName, maxRetries, lastException);
        throw lastException;
    }

    private void setParameters(PreparedStatement stmt, T value) throws SQLException {
        if (value instanceof TokenRecentMetric) {
            setTokenRecentMetricParameters(stmt, (TokenRecentMetric) value);
        } else if (value instanceof PairMetric) {
            setPairMetricParameters(stmt, (PairMetric) value);
        } else if (value instanceof AccountPnLSnapshot) {
            setAccountPnLSnapshotParameters(stmt, (AccountPnLSnapshot) value);
        } else if (value instanceof PnLRealizedEvent) {
            setPnLRealizedEventParameters(stmt, (PnLRealizedEvent) value);
        } else if (value instanceof TradeFact) {
            setTradeFactParameters(stmt, (TradeFact) value);
        } else if (value instanceof com.twilight.aggregator.model.perp.ExecutionMetrics) {
            setExecutionMetricsParameters(stmt, (com.twilight.aggregator.model.perp.ExecutionMetrics) value);
        } else if (value instanceof com.twilight.aggregator.model.perp.ContextMetrics) {
            setContextMetricsParameters(stmt, (com.twilight.aggregator.model.perp.ContextMetrics) value);
        } else if (value instanceof com.twilight.aggregator.model.perp.PerpSignal) {
            setPerpSignalParameters(stmt, (com.twilight.aggregator.model.perp.PerpSignal) value);
        } else if (value instanceof com.twilight.aggregator.model.perp.PanelMetrics) {
            setPanelMetricsParameters(stmt, (com.twilight.aggregator.model.perp.PanelMetrics) value);
        } else if (value instanceof KlineMetrics) {
            setKlineMetricsParameters(stmt, (KlineMetrics) value);
        } else if (value instanceof IndicatorMetric) {
            setIndicatorMetricParameters(stmt, (IndicatorMetric) value);
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + value.getClass().getName());
        }
    }

    private void setTokenRecentMetricParameters(PreparedStatement stmt, TokenRecentMetric metric) throws SQLException {
        stmt.setLong(1, metric.getTokenId());
        stmt.setString(2, metric.getTimeWindow());
        stmt.setTimestamp(3, new Timestamp(metric.getEndTime()));
        stmt.setString(4, metric.getTag());
        stmt.setInt(5, metric.getTxCnt());
        stmt.setInt(6, metric.getBuyCount());
        stmt.setInt(7, metric.getSellCount());
        stmt.setBigDecimal(8, java.math.BigDecimal.valueOf(metric.getVolumeUsd()));
        stmt.setBigDecimal(9, java.math.BigDecimal.valueOf(metric.getBuyVolumeUsd()));
        stmt.setBigDecimal(10, java.math.BigDecimal.valueOf(metric.getSellVolumeUsd()));
        stmt.setBigDecimal(11, java.math.BigDecimal.valueOf(metric.getBuyPressureUsd()));
        stmt.setBigDecimal(12, java.math.BigDecimal.valueOf(metric.getTokenPriceUsd()));
        // 新增的Token指标字段
        stmt.setBigDecimal(13, java.math.BigDecimal.valueOf(metric.getMcapUsd() != null ? metric.getMcapUsd() : 0.0));
        stmt.setBigDecimal(14, java.math.BigDecimal.valueOf(metric.getFdvUsd() != null ? metric.getFdvUsd() : 0.0));
        stmt.setBigDecimal(15, java.math.BigDecimal.valueOf(metric.getLiquidityUsd() != null ? metric.getLiquidityUsd() : 0.0));
        stmt.setTimestamp(16, new Timestamp(System.currentTimeMillis())); // process_time
        stmt.setTimestamp(17, new Timestamp(System.currentTimeMillis())); // create_time
    }


    private void setPairMetricParameters(PreparedStatement stmt, PairMetric metric) throws SQLException {
        stmt.setLong(1, metric.getPairId());
        stmt.setString(2, metric.getTimeWindow());
        stmt.setTimestamp(3, new Timestamp(metric.getEndTime()));
        stmt.setBigDecimal(4, java.math.BigDecimal.valueOf(metric.getToken0Reserve()));
        stmt.setBigDecimal(5, java.math.BigDecimal.valueOf(metric.getToken1Reserve()));
        stmt.setBigDecimal(6, java.math.BigDecimal.valueOf(metric.getReserveUsd()));
        stmt.setBigDecimal(7, java.math.BigDecimal.valueOf(metric.getToken0VolumeUsd()));
        stmt.setBigDecimal(8, java.math.BigDecimal.valueOf(metric.getToken1VolumeUsd()));
        stmt.setBigDecimal(9, java.math.BigDecimal.valueOf(metric.getVolumeUsd()));
        stmt.setInt(10, metric.getTxcnt());
        stmt.setTimestamp(11, new Timestamp(System.currentTimeMillis())); // process_time
        stmt.setTimestamp(12, new Timestamp(System.currentTimeMillis())); // create_time
    }

    private void setAccountPnLSnapshotParameters(PreparedStatement stmt, AccountPnLSnapshot snapshot) throws SQLException {
        stmt.setLong(1, snapshot.getAccountId());
        stmt.setString(2, snapshot.getAccountAddress());
        stmt.setLong(3, snapshot.getTokenId());
        stmt.setBigDecimal(4, snapshot.getPosition());
        stmt.setBigDecimal(5, snapshot.getAvgCost());
        stmt.setBigDecimal(6, snapshot.getRealizedCostUsd());
        stmt.setBigDecimal(7, snapshot.getRealizedProceedsUsd());
        stmt.setBigDecimal(8, snapshot.getRealizedPnLUsd());
        stmt.setBigDecimal(9, snapshot.getLastPriceUsd());
        stmt.setBigDecimal(10, snapshot.getUnrealizedPnLUsd());
        stmt.setBigDecimal(11, snapshot.getTotalPnLUsd());
        stmt.setDouble(12, snapshot.getRoiPct() != null ? snapshot.getRoiPct() : 0.0);
        stmt.setDouble(13, snapshot.getHoldingPct() != null ? snapshot.getHoldingPct() : 0.0);
        stmt.setTimestamp(14, java.sql.Timestamp.valueOf(snapshot.getLastTxTime()));
        stmt.setLong(15, snapshot.getVersion());
    }
    
    private void setPnLRealizedEventParameters(PreparedStatement stmt, PnLRealizedEvent event) throws SQLException {
        stmt.setLong(1, event.getTokenId());
        stmt.setLong(2, event.getAccountId());
        stmt.setLong(3, event.getBlockId());
        stmt.setTimestamp(4, java.sql.Timestamp.valueOf(event.getBlockTime()));
        stmt.setBigDecimal(5, event.getRealizedQty());
        stmt.setBigDecimal(6, event.getRealizedCostUsd());
        stmt.setBigDecimal(7, event.getRealizedProceedsUsd());
        stmt.setBigDecimal(8, event.getRealizedPnLUsd());
    }

    @Override
    public void close() throws Exception {
        if (!batch.isEmpty()) {
            flush();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        super.close();
    }

    // 静态工厂方法
    public static ClickHouseSink<TokenRecentMetric> createTokenRecentMetricSink() {
        String insertSql = "INSERT INTO token_recent_metric_ch " +
                "(token_id, time_window, end_time, tag, txcnt, buy_count, sell_count, " +
                "volume_usd, buy_volume_usd, sell_volume_usd, buy_pressure_usd, token_price_usd, " +
                "mcap_usd, fdv_usd, liquidity_usd, " +
                "process_time, create_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("token_recent_metric_ch", insertSql, 1000, 5000);
    }



    public static ClickHouseSink<PairMetric> createPairMetricSink() {
        String insertSql = "INSERT INTO twswap_pair_metric_ch " +
                "(pair_id, time_window, end_time, token0_reserve, token1_reserve, reserve_usd, " +
                "token0_volume_usd, token1_volume_usd, volume_usd, txcnt, process_time, create_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("twswap_pair_metric_ch", insertSql, 1000, 5000);
    }

    public static ClickHouseSink<AccountPnLSnapshot> createAccountPnLSink() {
        String insertSql = "INSERT INTO ch_account_pnl_current_ma " +
                "(account_id, account_address, token_id, position, avg_cost, realized_cost_usd, realized_proceeds_usd, " +
                "realized_pnl_usd, last_price_usd, unrealized_pnl_usd, total_pnl_usd, " +
                "roi_pct, holding_pct, last_tx_time, version) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("ch_account_pnl_current_ma", insertSql, 100, 5000);
    }

    public static ClickHouseSink<PnLRealizedEvent> createPnLRealizedEventSink() {
        String insertSql = "INSERT INTO ch_pnl_realized_event " +
                "(token_id, account_id, block_id, block_time, realized_qty, " +
                "realized_cost_usd, realized_proceeds_usd, realized_pnl_usd) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("ch_pnl_realized_event", insertSql, 100, 5000);
    }

    private void setTradeFactParameters(PreparedStatement stmt, TradeFact tradeFact) throws SQLException {
        stmt.setInt(1, tradeFact.getChainId());                        // chain_id
        stmt.setLong(2, tradeFact.getTokenId());                       // token_id
        stmt.setLong(3, tradeFact.getAccountId());                     // account_id
        stmt.setString(4, tradeFact.getAccountAddress());              // account_address
        stmt.setString(5, tradeFact.getSide());                        // side
        stmt.setLong(6, tradeFact.getPairId());                        // pair_id
        stmt.setString(7, tradeFact.getPairAddress());                 // pair_address
        
        // 时间字段转换
        if (tradeFact.getBlockTime() != null) {
            stmt.setTimestamp(8, Timestamp.valueOf(tradeFact.getBlockTime())); // block_time
        } else {
            stmt.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
        }
        stmt.setLong(9, tradeFact.getBlockId());                       // block_id
        
        // 唯一定位
        stmt.setString(10, tradeFact.getTxHash());                     // tx_hash
        stmt.setInt(11, tradeFact.getLogIndex());                      // log_index
        
        // 度量字段
        stmt.setBigDecimal(12, tradeFact.getQty());                    // qty
        stmt.setBigDecimal(13, tradeFact.getPriceUsd());               // price_usd
        stmt.setBigDecimal(14, tradeFact.getValueUsd());               // value_usd
        
        // 标签位图
        stmt.setInt(15, tradeFact.getLabelMask());                     // label_mask
    }

    private void setKlineMetricsParameters(PreparedStatement stmt, KlineMetrics metrics) throws SQLException {
        stmt.setString(1, metrics.getExchange());
        stmt.setString(2, metrics.getSymbol());
        stmt.setString(3, metrics.getInterval());

        stmt.setTimestamp(4, metrics.getStartTime() != null ? new Timestamp(metrics.getStartTime()) : null);
        stmt.setTimestamp(5, metrics.getCloseTime() != null ? new Timestamp(metrics.getCloseTime()) : null);
        stmt.setTimestamp(6, metrics.getEventTime() != null ? new Timestamp(metrics.getEventTime()) : null);
        stmt.setObject(7, metrics.getClosed(), java.sql.Types.BOOLEAN);
        stmt.setTimestamp(8, metrics.getIngestTime() != null ? new Timestamp(metrics.getIngestTime()) : null);

        stmt.setBigDecimal(9, metrics.getOpenPrice());
        stmt.setBigDecimal(10, metrics.getHighPrice());
        stmt.setBigDecimal(11, metrics.getLowPrice());
        stmt.setBigDecimal(12, metrics.getClosePrice());

        stmt.setBigDecimal(13, metrics.getBaseVolume());
        stmt.setBigDecimal(14, metrics.getQuoteVolume());
        stmt.setObject(15, metrics.getTradeCount(), java.sql.Types.INTEGER);

        stmt.setBigDecimal(16, metrics.getAmplitudePercent());
        stmt.setBigDecimal(17, metrics.getChangePercent());

        stmt.setObject(18, metrics.getShortPeriod(), java.sql.Types.INTEGER);
        stmt.setBigDecimal(19, metrics.getShortMa());
        stmt.setObject(20, metrics.getMediumPeriod(), java.sql.Types.INTEGER);
        stmt.setBigDecimal(21, metrics.getMediumMa());
        stmt.setObject(22, metrics.getLongPeriod(), java.sql.Types.INTEGER);
        stmt.setBigDecimal(23, metrics.getLongMa());
        
        stmt.setBigDecimal(24, metrics.getEmaShortValue());
        stmt.setBigDecimal(25, metrics.getEmaLongValue());

        stmt.setString(26, metrics.getSignalType() != null ? metrics.getSignalType().name() : KlineSignal.SignalType.HOLD.name());
        stmt.setBigDecimal(27, metrics.getSignalStrength() != null ? metrics.getSignalStrength() : BigDecimal.ZERO);
        stmt.setString(28, metrics.getSignalDetail() != null ? metrics.getSignalDetail() : "");
        long signalTs = metrics.getSignalTimestamp() != null ? metrics.getSignalTimestamp() : System.currentTimeMillis();
        stmt.setTimestamp(29, new Timestamp(signalTs));

        Timestamp now = new Timestamp(System.currentTimeMillis());
        stmt.setTimestamp(30, now); // process_time
        stmt.setTimestamp(31, now); // create_time
    }

    private void setIndicatorMetricParameters(PreparedStatement stmt, IndicatorMetric metric) throws SQLException {
        stmt.setString(1, metric.getExchange());
        stmt.setString(2, metric.getSymbol());
        stmt.setString(3, metric.getInterval());

        stmt.setTimestamp(4, toTimestampOrDefault(metric.getStartTime()));
        stmt.setTimestamp(5, toTimestampOrDefault(metric.getEndTime()));

        stmt.setString(6, metric.getIndicator());
        stmt.setString(7, metric.getVariant());

        stmt.setDouble(8, metric.getValue() != null ? metric.getValue().doubleValue() : 0.0d);

        ArrayPair componentArrays = mapToArrays(metric.getComponents());
        stmt.setArray(9, stmt.getConnection().createArrayOf("String", componentArrays.keys));
        stmt.setArray(10, stmt.getConnection().createArrayOf("Float64", componentArrays.values));

        ArrayPair thresholdArrays = mapToArrays(metric.getThresholds());
        stmt.setArray(11, stmt.getConnection().createArrayOf("String", thresholdArrays.keys));
        stmt.setArray(12, stmt.getConnection().createArrayOf("Float64", thresholdArrays.values));

        KlineSignal.SignalType signalType = metric.getSignalType() != null
                ? metric.getSignalType()
                : KlineSignal.SignalType.HOLD;
        stmt.setString(13, mapSignalType(signalType));

        BigDecimal strength = metric.getSignalStrength() != null ? metric.getSignalStrength() : BigDecimal.ZERO;
        stmt.setDouble(14, strength.doubleValue());

        stmt.setString(15, metric.getSignalDetail() != null ? metric.getSignalDetail() : "");

        long process = metric.getProcessTime() != null ? metric.getProcessTime() : System.currentTimeMillis();
        Timestamp processTs = new Timestamp(process);
        stmt.setTimestamp(16, processTs);
        stmt.setTimestamp(17, new Timestamp(System.currentTimeMillis()));
    }

    public static ClickHouseSink<TradeFact> createTradeFactSink() {
        String insertSql = "INSERT INTO ch_account_trade_fact " +
                "(chain_id, token_id, account_id, account_address, side, pair_id, pair_address, block_time, block_id, " +
                "tx_hash, log_index, qty, price_usd, value_usd, label_mask) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("ch_account_trade_fact", insertSql, 200, 10000);  // 200条批量，10秒刷新
    }

    public static ClickHouseSink<KlineMetrics> createKlineMetricsSink() {
        String insertSql = "INSERT INTO kline_metrics " +
                "(exchange, symbol, interval, start_time, close_time, event_time, is_closed, ingest_time, " +
                "open_price, high_price, low_price, close_price, base_volume, quote_volume, trade_count, " +
                "amplitude_pct, change_pct, ma_short_period, ma_short_value, ma_medium_period, ma_medium_value, " +
                "ma_long_period, ma_long_value, ema_short_value, ema_long_value, " +
                "signal_type, signal_strength, signal_detail, signal_timestamp, process_time, create_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return new ClickHouseSink<>("kline_metrics", insertSql, 500, 5000);
    }

    public static ClickHouseSink<IndicatorMetric> createKlineIndicatorMetricsSink() {
        String insertSql = "INSERT INTO kline_indicator_metrics " +
                "(exchange, symbol, interval, start_time, end_time, indicator, variant, value, " +
                "components.name, components.val, thresholds.name, thresholds.val, signal_type, signal_strength, signal_detail, " +
                "process_time, create_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return new ClickHouseSink<>("kline_indicator_metrics", insertSql, 500, 5000);
    }

    private ArrayPair mapToArrays(Map<String, BigDecimal> source) {
        if (source == null || source.isEmpty()) {
            return ArrayPair.EMPTY;
        }

        String[] keys = new String[source.size()];
        Double[] values = new Double[source.size()];
        int index = 0;
        for (Map.Entry<String, BigDecimal> entry : source.entrySet()) {
            keys[index] = entry.getKey();
            values[index] = entry.getValue() != null ? entry.getValue().doubleValue() : 0.0d;
            index++;
        }
        return new ArrayPair(keys, values);
    }

    private static final class ArrayPair {
        private static final ArrayPair EMPTY = new ArrayPair(new String[0], new Double[0]);

        private final String[] keys;
        private final Double[] values;

        private ArrayPair(String[] keys, Double[] values) {
            this.keys = keys;
            this.values = values;
        }
    }

    private String mapSignalType(KlineSignal.SignalType signalType) {
        if (signalType == null) {
            return "NONE";
        }
        switch (signalType) {
            case BUY:
                return "BUY";
            case SELL:
                return "SELL";
            default:
                return "NONE";
        }
    }

    private Timestamp toTimestampOrDefault(Long epochMillis) {
        long value = epochMillis != null ? epochMillis : System.currentTimeMillis();
        return new Timestamp(value);
    }

    // ===== 永续合约指标Sink =====

    /**
     * 设置执行面指标参数
     */
    private void setExecutionMetricsParameters(PreparedStatement stmt, 
            com.twilight.aggregator.model.perp.ExecutionMetrics metrics) throws SQLException {
        stmt.setString(1, metrics.getSymbol());
        stmt.setString(2, metrics.getExchange());
        stmt.setTimestamp(3, new Timestamp(metrics.getEndTime()));
        stmt.setString(4, metrics.getAlgoVersion());
        
        // 订单簿指标
        stmt.setBigDecimal(5, metrics.getMidPrice());
        stmt.setDouble(6, metrics.getSpreadBps() != null ? metrics.getSpreadBps() : 0.0);
        stmt.setBigDecimal(7, metrics.getSpreadAbs());
        stmt.setBigDecimal(8, metrics.getDepth10k());
        stmt.setBigDecimal(9, metrics.getDepth50k());
        stmt.setBigDecimal(10, metrics.getDepth100k());
        stmt.setDouble(11, metrics.getImbalanceTop5() != null ? metrics.getImbalanceTop5() : 0.0);
        stmt.setDouble(12, metrics.getImbalanceTotal() != null ? metrics.getImbalanceTotal() : 0.0);
        stmt.setDouble(13, metrics.getImpact10kBps() != null ? metrics.getImpact10kBps() : 0.0);
        stmt.setDouble(14, metrics.getImpact50kBps() != null ? metrics.getImpact50kBps() : 0.0);
        stmt.setDouble(15, metrics.getImpact100kBps() != null ? metrics.getImpact100kBps() : 0.0);
        
        // OFI
        stmt.setDouble(16, metrics.getOfi() != null ? metrics.getOfi() : 0.0);
        
        // 成交指标
        stmt.setInt(17, metrics.getTradeCount() != null ? metrics.getTradeCount() : 0);
        stmt.setBigDecimal(18, metrics.getVolumeUsd());
        stmt.setBigDecimal(19, metrics.getVwap());
        stmt.setBigDecimal(20, metrics.getBuyVolumeUsd());
        stmt.setBigDecimal(21, metrics.getSellVolumeUsd());
        
        // 流动性指标（可选）
        stmt.setDouble(22, metrics.getIlliqLambda() != null ? metrics.getIlliqLambda() : 0.0);
        
        // 元数据
        stmt.setTimestamp(23, new Timestamp(metrics.getProcessTime() != null ? metrics.getProcessTime() : System.currentTimeMillis()));
    }

    /**
     * 设置语境面指标参数
     */
    private void setContextMetricsParameters(PreparedStatement stmt, 
            com.twilight.aggregator.model.perp.ContextMetrics metrics) throws SQLException {
        stmt.setString(1, metrics.getSymbol());
        stmt.setString(2, metrics.getExchange());
        stmt.setTimestamp(3, new Timestamp(metrics.getEndTime()));
        stmt.setString(4, metrics.getAlgoVersion());
        
        // 价格指标
        stmt.setBigDecimal(5, metrics.getMarkPrice());
        stmt.setBigDecimal(6, metrics.getIndexPrice());
        stmt.setDouble(7, metrics.getBasisBps() != null ? metrics.getBasisBps() : 0.0);
        
        // 资金费率
        stmt.setBigDecimal(8, metrics.getFundingRate());
        stmt.setBigDecimal(9, metrics.getFundingRate8h());
        stmt.setBigDecimal(10, metrics.getFundingEma24h());
        stmt.setTimestamp(11, metrics.getNextFundingTime() != null ? new Timestamp(metrics.getNextFundingTime()) : null);
        
        // 持仓量
        stmt.setBigDecimal(12, metrics.getOi());
        stmt.setBigDecimal(13, metrics.getOiUsd());
        stmt.setBigDecimal(14, metrics.getOiDelta1m());
        stmt.setDouble(15, metrics.getOiDeltaPct() != null ? metrics.getOiDeltaPct() : 0.0);
        stmt.setBoolean(16, metrics.getIsOiCarried() != null ? metrics.getIsOiCarried() : false);
        
        // 元数据
        stmt.setTimestamp(17, new Timestamp(metrics.getProcessTime() != null ? metrics.getProcessTime() : System.currentTimeMillis()));
    }

    /**
     * 设置信号参数
     */
    private void setPerpSignalParameters(PreparedStatement stmt, 
            com.twilight.aggregator.model.perp.PerpSignal signal) throws SQLException {
        stmt.setString(1, signal.getSymbol());
        stmt.setString(2, signal.getExchange());
        stmt.setTimestamp(3, new Timestamp(signal.getSignalTime()));
        stmt.setString(4, signal.getSignalType() != null ? signal.getSignalType().name() : null);
        stmt.setString(5, signal.getSignalLevel() != null ? signal.getSignalLevel().name() : null);
        stmt.setString(6, signal.getMetricName());
        stmt.setDouble(7, signal.getMetricValue() != null ? signal.getMetricValue() : 0.0);
        stmt.setDouble(8, signal.getThreshold() != null ? signal.getThreshold() : 0.0);
        stmt.setString(9, signal.getContextJson());
        stmt.setString(10, signal.getAlgoVersion());
        stmt.setTimestamp(11, new Timestamp(signal.getProcessTime() != null ? signal.getProcessTime() : System.currentTimeMillis()));
    }

    /**
     * 创建执行面指标Sink
     */
    public static ClickHouseSink<com.twilight.aggregator.model.perp.ExecutionMetrics> createExecutionMetricsSink() {
        String insertSql = "INSERT INTO dws_exec_1s " +
                "(symbol, exchange, end_time, algo_version, " +
                "mid_price, spread_bps, spread_abs, depth_10k, depth_50k, depth_100k, " +
                "imbalance_top5, imbalance_total, impact_10k_bps, impact_50k_bps, impact_100k_bps, " +
                "ofi, trade_count, volume_usd, vwap, buy_volume_usd, sell_volume_usd, " +
                "illiq_lambda, process_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("dws_exec_1s", insertSql, 1000, 5000);
    }

    /**
     * 创建语境面指标Sink
     */
    public static ClickHouseSink<com.twilight.aggregator.model.perp.ContextMetrics> createContextMetricsSink() {
        String insertSql = "INSERT INTO dws_perps_ctx_1m " +
                "(symbol, exchange, end_time, algo_version, " +
                "mark_price, index_price, basis_bps, " +
                "funding_rate, funding_rate_8h, funding_ema_24h, next_funding_time, " +
                "oi, oi_usd, oi_delta_1m, oi_delta_pct, is_oi_carried, " +
                "process_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("dws_perps_ctx_1m", insertSql, 500, 5000);
    }

    /**
     * 创建信号Sink
     */
    public static ClickHouseSink<com.twilight.aggregator.model.perp.PerpSignal> createPerpSignalSink() {
        String insertSql = "INSERT INTO perp_signals " +
                "(symbol, exchange, signal_time, signal_type, signal_level, " +
                "metric_name, metric_value, threshold, context_json, algo_version, process_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("perp_signals", insertSql, 100, 3000);
    }

    /**
     * 设置Panel指标参数（汇合面板）
     */
    private void setPanelMetricsParameters(PreparedStatement stmt, 
            com.twilight.aggregator.model.perp.PanelMetrics panel) throws SQLException {
        stmt.setString(1, panel.getSymbol());
        stmt.setString(2, panel.getExchange());
        stmt.setTimestamp(3, new Timestamp(panel.getEndTime()));
        stmt.setString(4, "v1.0");  // algo_version
        
        // 执行面聚合指标（从1s rollup）
        stmt.setDouble(5, panel.getAvgSpreadBps() != null ? panel.getAvgSpreadBps() : 0.0);
        stmt.setDouble(6, panel.getMaxSpreadBps() != null ? panel.getMaxSpreadBps() : 0.0);
        stmt.setBigDecimal(7, panel.getAvgDepth50k());
        stmt.setDouble(8, panel.getAvgImpact50kBps() != null ? panel.getAvgImpact50kBps() : 0.0);
        stmt.setDouble(9, panel.getAvgImbalance() != null ? panel.getAvgImbalance() : 0.0);
        stmt.setDouble(10, panel.getSumOfi() != null ? panel.getSumOfi() : 0.0);
        stmt.setBigDecimal(11, panel.getVolumeUsd());
        stmt.setInt(12, panel.getTradeCount() != null ? panel.getTradeCount() : 0);
        
        // 语境面指标
        stmt.setBigDecimal(13, panel.getMarkPrice());
        stmt.setDouble(14, panel.getBasisBps() != null ? panel.getBasisBps() : 0.0);
        stmt.setBigDecimal(15, panel.getFundingRate());
        stmt.setBigDecimal(16, panel.getFundingEma24h());
        stmt.setBigDecimal(17, panel.getOiUsd());
        stmt.setBigDecimal(18, panel.getOiDelta1m());
        
        // 衍生指标
        stmt.setString(19, panel.getLiquidityRegime());
        stmt.setDouble(20, panel.getCrowdingScore() != null ? panel.getCrowdingScore() : 0.0);
        
        // 元数据
        stmt.setTimestamp(21, new Timestamp(System.currentTimeMillis()));
    }

    /**
     * 创建Panel指标Sink（汇合面板）
     */
    public static ClickHouseSink<com.twilight.aggregator.model.perp.PanelMetrics> createPanelMetricsSink() {
        String insertSql = "INSERT INTO dws_perps_panel_1m " +
                "(symbol, exchange, end_time, algo_version, " +
                "avg_spread_bps, max_spread_bps, avg_depth_50k, avg_impact_50k_bps, " +
                "avg_imbalance, sum_ofi, volume_usd, trade_count, " +
                "mark_price, basis_bps, funding_rate, funding_ema_24h, oi_usd, oi_delta_1m, " +
                "liquidity_regime, crowding_score, process_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("dws_perps_panel_1m", insertSql, 500, 5000);
    }
}

package com.twilight.aggregator.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

    public static ClickHouseSink<TradeFact> createTradeFactSink() {
        String insertSql = "INSERT INTO ch_account_trade_fact " +
                "(chain_id, token_id, account_id, account_address, side, pair_id, pair_address, block_time, block_id, " +
                "tx_hash, log_index, qty, price_usd, value_usd, label_mask) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return new ClickHouseSink<>("ch_account_trade_fact", insertSql, 200, 10000);  // 200条批量，10秒刷新
    }
}

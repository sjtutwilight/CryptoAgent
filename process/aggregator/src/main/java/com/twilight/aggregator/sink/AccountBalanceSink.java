package com.twilight.aggregator.sink;

import com.twilight.aggregator.config.FlinkConfig;
import com.twilight.aggregator.model.AccountBalance;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * AccountBalance ClickHouse Sink
 * 高性能批量写入账户余额快照数据
 */
public class AccountBalanceSink extends RichSinkFunction<AccountBalance> {
    private static final Logger log = LoggerFactory.getLogger(AccountBalanceSink.class);
    private static final long serialVersionUID = 1L;
    
    private final int batchSize;
    private final long flushIntervalMs;
    
    private transient Connection connection;
    private transient List<AccountBalance> batch;
    private transient long lastFlushTime;
    
    // ClickHouse INSERT SQL
    private static final String INSERT_SQL = 
        "INSERT INTO ch_account_balance_snapshot " +
        "(snapshot_id, account_id, account_address, asset_type, biz_id, observed_time, block_id, amount, price_usd, value_usd, label_mask) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    public AccountBalanceSink() {
        this(100, 1000); // 默认100条批量，5秒刷新
    }
    
    public AccountBalanceSink(int batchSize, long flushIntervalMs) {
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
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
        props.setProperty("compress", "1");  // 启用压缩提高性能
        
        connection = DriverManager.getConnection(url, props);
        
        // 初始化批处理
        batch = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();
        
        log.info("✅ AccountBalanceSink connected to ClickHouse: {}, batch_size: {}, flush_interval: {}ms", 
                url, batchSize, flushIntervalMs);
    }
    
    @Override
    public synchronized void invoke(AccountBalance balance, Context context) throws Exception {
        batch.add(balance);
        
        // 检查是否需要刷新
        long currentTime = System.currentTimeMillis();
        if (batch.size() >= batchSize || (currentTime - lastFlushTime) >= flushIntervalMs) {
            flush();
        }
    }
    
    @Override
    public void close() throws Exception {
        try {
            // 刷新剩余数据
            if (batch != null && !batch.isEmpty()) {
                flush();
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        super.close();
    }
    
    /**
     * 批量刷新数据到ClickHouse
     */
    private void flush() throws Exception {
        if (batch.isEmpty()) {
            return;
        }
        
        int retries = 0;
        int maxRetries = 3;
        long retryDelayMs = 1000;
        Exception lastException = null;
        
        log.debug("💾 Flushing {} account balance records to ClickHouse", batch.size());
        
        while (retries < maxRetries) {
            try (PreparedStatement stmt = connection.prepareStatement(INSERT_SQL)) {
                
                // 批量设置参数
                for (AccountBalance balance : batch) {
                    setBalanceParameters(stmt, balance);
                    stmt.addBatch();
                }
                
                // 执行批量插入
                int[] results = stmt.executeBatch();
                
                log.debug("✅ Successfully inserted {} account balance records", results.length);
                
                // 清空缓存并更新时间
                batch.clear();
                lastFlushTime = System.currentTimeMillis();
                return; // 成功执行，退出方法
                
            } catch (SQLException e) {
                lastException = e;
                retries++;
                log.warn("❌ Error writing account balances to ClickHouse (attempt {}/{}): {}", 
                        retries, maxRetries, e.getMessage());
                
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
        log.error("💥 Failed to write account balances to ClickHouse after {} attempts", maxRetries, lastException);
        throw lastException;
    }
    
    /**
     * 设置AccountBalance参数
     */
    private void setBalanceParameters(PreparedStatement stmt, AccountBalance balance) throws SQLException {
        stmt.setLong(1, balance.getSnapshotId());
        stmt.setLong(2, balance.getAccountId());
        stmt.setString(3, balance.getAccountAddress());
        stmt.setString(4, balance.getAssetType());
        stmt.setLong(5, balance.getBizId());

        // 转换LocalDateTime为Timestamp
        Timestamp timestamp = Timestamp.valueOf(balance.getObservedTime());
        stmt.setTimestamp(6, timestamp);
        
        stmt.setLong(7, balance.getBlockId());
        stmt.setBigDecimal(8, balance.getAmount());
        stmt.setBigDecimal(9, balance.getPriceUsd());
        stmt.setBigDecimal(10, balance.getValueUsd());
        stmt.setInt(11, balance.getLabelMask());
    }
    
    /**
     * 创建默认的AccountBalanceSink实例
     */
    public static AccountBalanceSink create() {
        return new AccountBalanceSink();
    }
    
    /**
     * 创建自定义批量大小的AccountBalanceSink实例
     */
    public static AccountBalanceSink create(int batchSize, long flushIntervalMs) {
        return new AccountBalanceSink(batchSize, flushIntervalMs);
    }
}

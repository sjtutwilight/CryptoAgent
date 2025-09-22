// package com.twilight.aggregator.sink;

// import java.sql.Connection;
// import java.sql.PreparedStatement;
// import java.sql.Timestamp;
// import java.sql.SQLException;

// import javax.sql.DataSource;

// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import com.twilight.aggregator.config.DatabaseConfig;
// import com.twilight.aggregator.model.PairMetric;
// import com.twilight.aggregator.model.TokenRecentMetric;

// public class PostgresSink<T> extends RichSinkFunction<T> {
//     private static transient Logger log;
//     private static final long serialVersionUID = 1L;

//     private transient DataSource dataSource;
//     private final String insertSql;

//     private PostgresSink(String insertSql) {
//         this.insertSql = insertSql;
//     }

//     @Override
//     public void open(Configuration parameters) throws Exception {
//         dataSource = DatabaseConfig.getInstance().getHikariDataSource();
//         log = LoggerFactory.getLogger(PostgresSink.class);
//     }

//     @Override
//     public void invoke(T value, Context context) throws Exception {
//         int retries = 0;
//         int maxRetries = 3;
//         long retryDelayMs = 1000;
//         Exception lastException = null;

//         log.info("PostgresSink: Invoking with value: {}", value);

//         while (retries < maxRetries) {
//             try (Connection conn = dataSource.getConnection();
//                     PreparedStatement stmt = conn.prepareStatement(insertSql)) {

//                 if (value instanceof PairMetric) {
//                     PairMetric metric = (PairMetric) value;
//                     insertPairMetric(stmt, metric);
//                     log.info("PostgresSink: Inserting PairMetric: {}", metric);
                
//                 } else if (value instanceof TokenRecentMetric) {
//                     TokenRecentMetric metric = (TokenRecentMetric) value;
//                     insertTokenRecentMetric(stmt, metric);
//                     log.info("PostgresSink: Inserting TokenRecentMetric: {}", metric);
//                 }

//                 stmt.executeUpdate();
//                 log.info("PostgresSink: Successfully executed update");
//                 return; // 成功执行，退出方法
//             } catch (SQLException e) {
//                 lastException = e;
//                 retries++;
//                 log.warn("Error writing to database (attempt {}/{}): {}", retries, maxRetries, e.getMessage());

//                 if (retries < maxRetries) {
//                     try {
//                         Thread.sleep(retryDelayMs);
//                     } catch (InterruptedException ie) {
//                         Thread.currentThread().interrupt();
//                         throw new RuntimeException("Interrupted while waiting to retry database operation", ie);
//                     }
//                 }
//             } catch (Exception e) {
//                 log.error("Unexpected error writing to database", e);
//                 throw e;
//             }
//         }

//         // 如果所有重试都失败，记录错误并抛出异常
//         log.error("Failed to write to database after {} attempts", maxRetries, lastException);
//         throw lastException;
//     }

//     private void insertPairMetric(PreparedStatement stmt, PairMetric metric) throws SQLException {
//         stmt.setLong(1, metric.getPairId());
//         stmt.setString(2, metric.getTimeWindow());
//         stmt.setTimestamp(3, new Timestamp(metric.getEndTime()));
//         stmt.setDouble(4, metric.getToken0Reserve());
//         stmt.setDouble(5, metric.getToken1Reserve());
//         stmt.setDouble(6, metric.getReserveUsd());
//         stmt.setDouble(7, metric.getToken0VolumeUsd());
//         stmt.setDouble(8, metric.getToken1VolumeUsd());
//         stmt.setDouble(9, metric.getVolumeUsd());
//         stmt.setInt(10, metric.getTxcnt());
//     }

   

//     private void insertTokenRecentMetric(PreparedStatement stmt, TokenRecentMetric metric) throws SQLException {
//         stmt.setLong(1, metric.getTokenId());
//         stmt.setString(2, metric.getTimeWindow());
//         stmt.setTimestamp(3, new Timestamp(metric.getEndTime()));
//         stmt.setString(4, metric.getTag());
//         stmt.setInt(5, metric.getTxCnt());
//         stmt.setInt(6, metric.getBuyCount());
//         stmt.setInt(7, metric.getSellCount());
//         stmt.setDouble(8, metric.getVolumeUsd());
//         stmt.setDouble(9, metric.getBuyVolumeUsd());
//         stmt.setDouble(10, metric.getSellVolumeUsd());
//         stmt.setDouble(11, metric.getBuyPressureUsd());
//         stmt.setDouble(12, metric.getTokenPriceUsd());
//     }

//     public static PostgresSink<PairMetric> createPairMetricSink() {
//         return new PostgresSink<>(
//                 "INSERT INTO twswap_pair_metric (pair_id, time_window, end_time, token0_reserve, token1_reserve, " +
//                         "reserve_usd, token0_volume_usd, token1_volume_usd, volume_usd, txcnt) " +
//                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
//                         "ON CONFLICT (pair_id, time_window, end_time) DO UPDATE SET " +
//                         "token0_reserve = EXCLUDED.token0_reserve, " +
//                         "token1_reserve = EXCLUDED.token1_reserve, " +
//                         "reserve_usd = EXCLUDED.reserve_usd, " +
//                         "token0_volume_usd = EXCLUDED.token0_volume_usd, " +
//                         "token1_volume_usd = EXCLUDED.token1_volume_usd, " +
//                         "volume_usd = EXCLUDED.volume_usd, " +
//                         "txcnt = EXCLUDED.txcnt");
//     }

  

//     public static PostgresSink<TokenRecentMetric> createTokenRecentMetricSink() {
//         return new PostgresSink<>(
//                 "INSERT INTO token_recent_metric (token_id, time_window, end_time, tag, txcnt, " +
//                         "buy_count, sell_count, volume_usd, buy_volume_usd, sell_volume_usd, " +
//                         "buy_pressure_usd, token_price_usd) " +
//                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
//                         "ON CONFLICT (token_id, time_window, end_time, tag) DO UPDATE SET " +
//                         "txcnt = EXCLUDED.txcnt, " +
//                         "buy_count = EXCLUDED.buy_count, " +
//                         "sell_count = EXCLUDED.sell_count, " +
//                         "volume_usd = EXCLUDED.volume_usd, " +
//                         "buy_volume_usd = EXCLUDED.buy_volume_usd, " +
//                         "sell_volume_usd = EXCLUDED.sell_volume_usd, " +
//                         "buy_pressure_usd = EXCLUDED.buy_pressure_usd, " +
//                         "token_price_usd = EXCLUDED.token_price_usd");
//     }
// }

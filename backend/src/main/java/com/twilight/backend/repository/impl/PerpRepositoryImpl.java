package com.twilight.backend.repository.impl;

import com.twilight.backend.model.PerpContextMetric;
import com.twilight.backend.model.PerpExecutionMetric;
import com.twilight.backend.model.PerpPanelMetric;
import com.twilight.backend.model.PerpSignal;
import com.twilight.backend.repository.PerpRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * 永续合约数据访问实现
 */
@Slf4j
@Repository
public class PerpRepositoryImpl implements PerpRepository {

    private static final int DEFAULT_SERIES_LIMIT = 3600;
    private static final int MAX_SERIES_LIMIT = 10000;
    private static final int MAX_PAGE_SIZE = 500;

    private final JdbcTemplate clickhouseJdbcTemplate;

    public PerpRepositoryImpl(@Qualifier("clickhouseJdbcTemplate") JdbcTemplate clickhouseJdbcTemplate) {
        this.clickhouseJdbcTemplate = clickhouseJdbcTemplate;
    }

    @Override
    public List<PerpPanelMetric> findLatestPanelSnapshots(List<String> symbols,
                                                          String exchange,
                                                          String algoVersion,
                                                          int page,
                                                          int pageSize,
                                                          String sortBy,
                                                          String order) {
        int safePage = Math.max(page, 1);
        int safePageSize = Math.min(Math.max(pageSize, 1), MAX_PAGE_SIZE);
        int offset = (safePage - 1) * safePageSize;

        String sortColumn = mapPanelSortColumn(sortBy);
        String sortDirection = "asc".equalsIgnoreCase(order) ? "ASC" : "DESC";

        StringBuilder where = new StringBuilder(" WHERE 1 = 1 ");
        List<Object> params = new ArrayList<>();

        if (symbols != null && !symbols.isEmpty()) {
            where.append(" AND symbol IN (")
                    .append(buildPlaceholders(symbols.size()))
                    .append(") ");
            params.addAll(symbols);
        }

        if (exchange != null && !exchange.isBlank()) {
            where.append(" AND exchange = ? ");
            params.add(exchange);
        }

        if (algoVersion != null && !algoVersion.isBlank()) {
            where.append(" AND algo_version = ? ");
            params.add(algoVersion);
        }

        String baseQuery = """
                SELECT
                    symbol,
                    exchange,
                    end_time,
                    algo_version,
                    avg_spread_bps,
                    max_spread_bps,
                    avg_depth_50k,
                    avg_impact_50k_bps,
                    avg_imbalance,
                    sum_ofi,
                    volume_usd,
                    trade_count,
                    mark_price,
                    basis_bps,
                    funding_rate,
                    funding_ema_24h,
                    oi_usd,
                    oi_delta_1m,
                    liquidity_regime,
                    crowding_score,
                    process_time,
                    ROW_NUMBER() OVER (PARTITION BY symbol, exchange ORDER BY end_time DESC) AS rn
                FROM dws_perps_panel_1m
                %s
                """.formatted(where);

        String sql = """
                SELECT
                    symbol,
                    exchange,
                    end_time,
                    algo_version,
                    avg_spread_bps,
                    max_spread_bps,
                    avg_depth_50k,
                    avg_impact_50k_bps,
                    avg_imbalance,
                    sum_ofi,
                    volume_usd,
                    trade_count,
                    mark_price,
                    basis_bps,
                    funding_rate,
                    funding_ema_24h,
                    oi_usd,
                    oi_delta_1m,
                    liquidity_regime,
                    crowding_score,
                    process_time
                FROM (
                    %s
                )
                WHERE rn = 1
                ORDER BY %s %s
                LIMIT %d OFFSET %d
                """.formatted(baseQuery, sortColumn, sortDirection, safePageSize, offset);

        try {
            return clickhouseJdbcTemplate.query(sql, params.toArray(), new PerpPanelMetricRowMapper());
        } catch (Exception ex) {
            log.error("查询永续面板快照失败 symbols={}, exchange={}, algo={}", symbols, exchange, algoVersion, ex);
            return List.of();
        }
    }

    @Override
    public List<PerpExecutionMetric> findExecutionMetrics(String symbol,
                                                          String exchange,
                                                          String algo,
                                                          LocalDateTime startTime,
                                                          LocalDateTime endTime,
                                                          int limit) {
        int safeLimit = sanitizeLimit(limit);
        StringBuilder where = new StringBuilder(" WHERE 1 = 1 ");
        List<Object> params = new ArrayList<>();

        applyCommonFilters(where, params, symbol, exchange, algo, startTime, endTime);

        String inner = """
                SELECT
                    symbol,
                    exchange,
                    end_time,
                    algo_version,
                    mid_price,
                    spread_bps,
                    spread_abs,
                    depth_10k,
                    depth_50k,
                    depth_100k,
                    imbalance_top5,
                    imbalance_total,
                    impact_10k_bps,
                    impact_50k_bps,
                    impact_100k_bps,
                    ofi,
                    trade_count,
                    volume_usd,
                    vwap,
                    buy_volume_usd,
                    sell_volume_usd,
                    illiq_lambda,
                    process_time
                FROM dws_exec_1s
                %s
                ORDER BY end_time DESC
                LIMIT %d
                """.formatted(where, safeLimit);

        String sql = """
                SELECT *
                FROM (
                    %s
                )
                ORDER BY end_time ASC
                """.formatted(inner);

        try {
            return clickhouseJdbcTemplate.query(sql, params.toArray(), new PerpExecutionMetricRowMapper());
        } catch (Exception ex) {
            log.error("查询执行面指标失败 symbol={}, exchange={}, algo={}", symbol, exchange, algo, ex);
            return List.of();
        }
    }

    @Override
    public List<PerpContextMetric> findContextMetrics(String symbol,
                                                      String exchange,
                                                      String algo,
                                                      LocalDateTime startTime,
                                                      LocalDateTime endTime,
                                                      int limit) {
        int safeLimit = sanitizeLimit(limit);
        StringBuilder where = new StringBuilder(" WHERE 1 = 1 ");
        List<Object> params = new ArrayList<>();

        applyCommonFilters(where, params, symbol, exchange, algo, startTime, endTime);

        String inner = """
                SELECT
                    symbol,
                    exchange,
                    end_time,
                    algo_version,
                    mark_price,
                    index_price,
                    basis_bps,
                    funding_rate,
                    funding_rate_8h,
                    funding_ema_24h,
                    next_funding_time,
                    oi,
                    oi_usd,
                    oi_delta_1m,
                    oi_delta_pct,
                    is_oi_carried,
                    process_time
                FROM dws_perps_ctx_1m
                %s
                ORDER BY end_time DESC
                LIMIT %d
                """.formatted(where, safeLimit);

        String sql = """
                SELECT *
                FROM (
                    %s
                )
                ORDER BY end_time ASC
                """.formatted(inner);

        try {
            return clickhouseJdbcTemplate.query(sql, params.toArray(), new PerpContextMetricRowMapper());
        } catch (Exception ex) {
            log.error("查询语境面指标失败 symbol={}, exchange={}, algo={}", symbol, exchange, algo, ex);
            return List.of();
        }
    }

    @Override
    public List<PerpPanelMetric> findPanelMetrics(String symbol,
                                                  String exchange,
                                                  String algo,
                                                  LocalDateTime startTime,
                                                  LocalDateTime endTime,
                                                  int limit) {
        int safeLimit = sanitizeLimit(limit);
        StringBuilder where = new StringBuilder(" WHERE 1 = 1 ");
        List<Object> params = new ArrayList<>();

        applyCommonFilters(where, params, symbol, exchange, algo, startTime, endTime);

        String inner = """
                SELECT
                    symbol,
                    exchange,
                    end_time,
                    algo_version,
                    avg_spread_bps,
                    max_spread_bps,
                    avg_depth_50k,
                    avg_impact_50k_bps,
                    avg_imbalance,
                    sum_ofi,
                    volume_usd,
                    trade_count,
                    mark_price,
                    basis_bps,
                    funding_rate,
                    funding_ema_24h,
                    oi_usd,
                    oi_delta_1m,
                    liquidity_regime,
                    crowding_score,
                    process_time
                FROM dws_perps_panel_1m
                %s
                ORDER BY end_time DESC
                LIMIT %d
                """.formatted(where, safeLimit);

        String sql = """
                SELECT *
                FROM (
                    %s
                )
                ORDER BY end_time ASC
                """.formatted(inner);

        try {
            return clickhouseJdbcTemplate.query(sql, params.toArray(), new PerpPanelMetricRowMapper());
        } catch (Exception ex) {
            log.error("查询面板指标失败 symbol={}, exchange={}, algo={}", symbol, exchange, algo, ex);
            return List.of();
        }
    }

    @Override
    public List<PerpSignal> findSignals(List<String> symbols,
                                        List<String> exchanges,
                                        List<String> types,
                                        List<String> levels,
                                        String algoVersion,
                                        LocalDateTime startTime,
                                        LocalDateTime endTime,
                                        int limit) {
        int safeLimit = sanitizeLimit(limit);

        StringBuilder where = new StringBuilder(" WHERE 1 = 1 ");
        List<Object> params = new ArrayList<>();

        appendOptionalInFilter(where, params, "symbol", symbols);
        appendOptionalInFilter(where, params, "exchange", exchanges);
        appendOptionalInFilter(where, params, "signal_type", types);
        appendOptionalInFilter(where, params, "signal_level", levels);

        if (algoVersion != null && !algoVersion.isBlank()) {
            where.append(" AND algo_version = ? ");
            params.add(algoVersion);
        }

        if (startTime != null) {
            where.append(" AND signal_time >= toDateTime(?) ");
            params.add(startTime.toString());
        }

        if (endTime != null) {
            where.append(" AND signal_time <= toDateTime(?) ");
            params.add(endTime.toString());
        }

        String sql = """
                SELECT
                    symbol,
                    exchange,
                    signal_time,
                    signal_type,
                    signal_level,
                    metric_name,
                    metric_value,
                    threshold,
                    context_json,
                    algo_version,
                    process_time
                FROM perp_signals
                %s
                ORDER BY signal_time DESC
                LIMIT %d
                """.formatted(where, safeLimit);

        try {
            return clickhouseJdbcTemplate.query(sql, params.toArray(), new PerpSignalRowMapper());
        } catch (Exception ex) {
            log.error("查询永续信号失败 symbols={}, exchanges={}, types={}", symbols, exchanges, types, ex);
            return List.of();
        }
    }

    private void applyCommonFilters(StringBuilder where,
                                    List<Object> params,
                                    String symbol,
                                    String exchange,
                                    String algo,
                                    LocalDateTime startTime,
                                    LocalDateTime endTime) {
        if (symbol != null && !symbol.isBlank()) {
            where.append(" AND symbol = ? ");
            params.add(symbol);
        }

        if (exchange != null && !exchange.isBlank()) {
            where.append(" AND exchange = ? ");
            params.add(exchange);
        }

        if (algo != null && !algo.isBlank()) {
            where.append(" AND algo_version = ? ");
            params.add(algo);
        }

        if (startTime != null) {
            where.append(" AND end_time >= toDateTime(?) ");
            params.add(startTime.toString());
        }

        if (endTime != null) {
            where.append(" AND end_time <= toDateTime(?) ");
            params.add(endTime.toString());
        }
    }

    private void appendOptionalInFilter(StringBuilder where, List<Object> params, String column, List<String> values) {
        if (values != null && !values.isEmpty()) {
            where.append(" AND ").append(column)
                    .append(" IN (")
                    .append(buildPlaceholders(values.size()))
                    .append(") ");
            params.addAll(values);
        }
    }

    private int sanitizeLimit(int limit) {
        if (limit <= 0) {
            return DEFAULT_SERIES_LIMIT;
        }
        return Math.min(limit, MAX_SERIES_LIMIT);
    }

    private String buildPlaceholders(int size) {
        return String.join(",", Collections.nCopies(size, "?"));
    }

    private String mapPanelSortColumn(String sortBy) {
        if (sortBy == null || sortBy.isBlank()) {
            return "volume_usd";
        }
        return switch (sortBy.toLowerCase(Locale.ROOT)) {
            case "spread" -> "avg_spread_bps";
            case "maxspread" -> "max_spread_bps";
            case "impact" -> "avg_impact_50k_bps";
            case "imbalance" -> "avg_imbalance";
            case "crowding" -> "crowding_score";
            case "funding" -> "funding_rate";
            case "oi" -> "oi_usd";
            case "basis" -> "basis_bps";
            default -> "volume_usd";
        };
    }

    private static class PerpExecutionMetricRowMapper implements RowMapper<PerpExecutionMetric> {
        @Override
        public PerpExecutionMetric mapRow(ResultSet rs, int rowNum) throws SQLException {
            PerpExecutionMetric metric = new PerpExecutionMetric();
            metric.setSymbol(rs.getString("symbol"));
            metric.setExchange(rs.getString("exchange"));
            metric.setEndTime(getDateTime(rs, "end_time"));
            metric.setAlgoVersion(rs.getString("algo_version"));

            metric.setMidPrice(rs.getBigDecimal("mid_price"));
            metric.setSpreadBps(getDouble(rs, "spread_bps"));
            metric.setSpreadAbs(rs.getBigDecimal("spread_abs"));

            metric.setDepth10k(rs.getBigDecimal("depth_10k"));
            metric.setDepth50k(rs.getBigDecimal("depth_50k"));
            metric.setDepth100k(rs.getBigDecimal("depth_100k"));

            metric.setImbalanceTop5(getDouble(rs, "imbalance_top5"));
            metric.setImbalanceTotal(getDouble(rs, "imbalance_total"));

            metric.setImpact10kBps(getDouble(rs, "impact_10k_bps"));
            metric.setImpact50kBps(getDouble(rs, "impact_50k_bps"));
            metric.setImpact100kBps(getDouble(rs, "impact_100k_bps"));

            metric.setOfi(getDouble(rs, "ofi"));

            metric.setTradeCount(getLong(rs, "trade_count"));
            metric.setVolumeUsd(rs.getBigDecimal("volume_usd"));
            metric.setVwap(rs.getBigDecimal("vwap"));
            metric.setBuyVolumeUsd(rs.getBigDecimal("buy_volume_usd"));
            metric.setSellVolumeUsd(rs.getBigDecimal("sell_volume_usd"));

            metric.setIlliqLambda(getDouble(rs, "illiq_lambda"));
            metric.setProcessTime(getDateTime(rs, "process_time"));
            return metric;
        }
    }

    private static class PerpContextMetricRowMapper implements RowMapper<PerpContextMetric> {
        @Override
        public PerpContextMetric mapRow(ResultSet rs, int rowNum) throws SQLException {
            PerpContextMetric metric = new PerpContextMetric();
            metric.setSymbol(rs.getString("symbol"));
            metric.setExchange(rs.getString("exchange"));
            metric.setEndTime(getDateTime(rs, "end_time"));
            metric.setAlgoVersion(rs.getString("algo_version"));

            metric.setMarkPrice(rs.getBigDecimal("mark_price"));
            metric.setIndexPrice(rs.getBigDecimal("index_price"));
            metric.setBasisBps(getDouble(rs, "basis_bps"));

            metric.setFundingRate(rs.getBigDecimal("funding_rate"));
            metric.setFundingRate8h(rs.getBigDecimal("funding_rate_8h"));
            metric.setFundingEma24h(rs.getBigDecimal("funding_ema_24h"));
            metric.setNextFundingTime(getDateTime(rs, "next_funding_time"));

            metric.setOi(rs.getBigDecimal("oi"));
            metric.setOiUsd(rs.getBigDecimal("oi_usd"));
            metric.setOiDelta1m(rs.getBigDecimal("oi_delta_1m"));
            metric.setOiDeltaPct(getDouble(rs, "oi_delta_pct"));
            metric.setOiCarried(getBoolean(rs, "is_oi_carried"));

            metric.setProcessTime(getDateTime(rs, "process_time"));
            return metric;
        }
    }

    private static class PerpPanelMetricRowMapper implements RowMapper<PerpPanelMetric> {
        @Override
        public PerpPanelMetric mapRow(ResultSet rs, int rowNum) throws SQLException {
            PerpPanelMetric metric = new PerpPanelMetric();
            metric.setSymbol(rs.getString("symbol"));
            metric.setExchange(rs.getString("exchange"));
            metric.setEndTime(getDateTime(rs, "end_time"));
            metric.setAlgoVersion(rs.getString("algo_version"));

            metric.setAvgSpreadBps(getDouble(rs, "avg_spread_bps"));
            metric.setMaxSpreadBps(getDouble(rs, "max_spread_bps"));
            metric.setAvgDepth50k(rs.getBigDecimal("avg_depth_50k"));
            metric.setAvgImpact50kBps(getDouble(rs, "avg_impact_50k_bps"));
            metric.setAvgImbalance(getDouble(rs, "avg_imbalance"));
            metric.setSumOfi(getDouble(rs, "sum_ofi"));
            metric.setVolumeUsd(rs.getBigDecimal("volume_usd"));
            metric.setTradeCount(getLong(rs, "trade_count"));

            metric.setMarkPrice(rs.getBigDecimal("mark_price"));
            metric.setBasisBps(getDouble(rs, "basis_bps"));
            metric.setFundingRate(rs.getBigDecimal("funding_rate"));
            metric.setFundingEma24h(rs.getBigDecimal("funding_ema_24h"));
            metric.setOiUsd(rs.getBigDecimal("oi_usd"));
            metric.setOiDelta1m(rs.getBigDecimal("oi_delta_1m"));

            metric.setLiquidityRegime(rs.getString("liquidity_regime"));
            metric.setCrowdingScore(getDouble(rs, "crowding_score"));

            metric.setProcessTime(getDateTime(rs, "process_time"));
            return metric;
        }
    }

    private static class PerpSignalRowMapper implements RowMapper<PerpSignal> {
        @Override
        public PerpSignal mapRow(ResultSet rs, int rowNum) throws SQLException {
            PerpSignal signal = new PerpSignal();
            signal.setSymbol(rs.getString("symbol"));
            signal.setExchange(rs.getString("exchange"));
            signal.setSignalTime(getDateTime(rs, "signal_time"));
            signal.setSignalType(rs.getString("signal_type"));
            signal.setSignalLevel(rs.getString("signal_level"));
            signal.setMetricName(rs.getString("metric_name"));
            signal.setMetricValue(getDouble(rs, "metric_value"));
            signal.setThreshold(getDouble(rs, "threshold"));
            signal.setContextJson(rs.getString("context_json"));
            signal.setAlgoVersion(rs.getString("algo_version"));
            signal.setProcessTime(getDateTime(rs, "process_time"));
            return signal;
        }
    }

    private static Double getDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private static Long getLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static Boolean getBoolean(ResultSet rs, String column) throws SQLException {
        boolean value = rs.getBoolean(column);
        return rs.wasNull() ? null : value;
    }

    private static LocalDateTime getDateTime(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp != null ? timestamp.toLocalDateTime() : null;
    }
}

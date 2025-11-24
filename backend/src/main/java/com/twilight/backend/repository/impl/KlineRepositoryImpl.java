package com.twilight.backend.repository.impl;

import com.twilight.backend.model.KlineIndicatorComponent;
import com.twilight.backend.model.KlineIndicatorMetric;
import com.twilight.backend.model.KlineIndicatorThreshold;
import com.twilight.backend.model.KlineMetric;
import com.twilight.backend.repository.KlineRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * ClickHouse K线指标查询实现
 */
@Slf4j
@Repository
public class KlineRepositoryImpl implements KlineRepository {

    private static final int DEFAULT_SERIES_LIMIT = 1000;
    private static final int MAX_SERIES_LIMIT = 5000;
    private static final int MAX_PAGE_SIZE = 500;

    private final JdbcTemplate clickhouseJdbcTemplate;

    public KlineRepositoryImpl(@Qualifier("clickhouseJdbcTemplate") JdbcTemplate clickhouseJdbcTemplate) {
        this.clickhouseJdbcTemplate = clickhouseJdbcTemplate;
    }

    @Override
    public List<KlineMetric> findLatestKlines(List<String> symbols,
                                              String exchange,
                                              String interval,
                                              int page,
                                              int pageSize,
                                              String sortBy,
                                              String order) {
        int safePage = Math.max(page, 1);
        int safePageSize = Math.min(Math.max(pageSize, 1), MAX_PAGE_SIZE);
        int offset = (safePage - 1) * safePageSize;

        String sortColumn = mapSnapshotSortColumn(sortBy);
        String sortDirection = "asc".equalsIgnoreCase(order) ? "ASC" : "DESC";

        StringBuilder where = new StringBuilder(" WHERE 1 = 1 AND is_closed = 1 ");
        List<Object> params = new ArrayList<>();

        appendOptionalInFilter(where, params, "symbol", symbols);

        if (exchange != null && !exchange.isBlank()) {
            where.append(" AND exchange = ? ");
            params.add(exchange);
        }

        if (interval != null && !interval.isBlank()) {
            where.append(" AND interval = ? ");
            params.add(interval);
        }

        String baseQuery = """
                SELECT
                    exchange,
                    symbol,
                    interval,
                    start_time,
                    close_time,
                    event_time,
                    is_closed,
                    ingest_time,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    base_volume,
                    quote_volume,
                    trade_count,
                    amplitude_pct,
                    change_pct,
                    ma_short_period,
                    ma_short_value,
                    ma_medium_period,
                    ma_medium_value,
                    ma_long_period,
                    ma_long_value,
                    ema_short_value,
                    ema_long_value,
                    signal_type,
                    signal_strength,
                    signal_detail,
                    signal_timestamp,
                    process_time,
                    create_time,
                    ROW_NUMBER() OVER (PARTITION BY exchange, symbol, interval ORDER BY close_time DESC, event_time DESC) AS rn
                FROM kline_metrics
                %s
                """.formatted(where);

        String sql = """
                SELECT
                    exchange,
                    symbol,
                    interval,
                    start_time,
                    close_time,
                    event_time,
                    is_closed,
                    ingest_time,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    base_volume,
                    quote_volume,
                    trade_count,
                    amplitude_pct,
                    change_pct,
                    ma_short_period,
                    ma_short_value,
                    ma_medium_period,
                    ma_medium_value,
                    ma_long_period,
                    ma_long_value,
                    ema_short_value,
                    ema_long_value,
                    signal_type,
                    signal_strength,
                    signal_detail,
                    signal_timestamp,
                    process_time,
                    create_time
                FROM (
                    %s
                )
                WHERE rn = 1
                ORDER BY %s %s
                LIMIT %d OFFSET %d
                """.formatted(baseQuery, sortColumn, sortDirection, safePageSize, offset);

        try {
            return clickhouseJdbcTemplate.query(sql, params.toArray(), new KlineMetricRowMapper());
        } catch (Exception ex) {
            log.error("查询K线快照失败 symbols={}, exchange={}, interval={}", symbols, exchange, interval, ex);
            return List.of();
        }
    }

    @Override
    public List<KlineMetric> findKlineSeries(String symbol,
                                             String exchange,
                                             String interval,
                                             LocalDateTime startTime,
                                             LocalDateTime endTime,
                                             int limit) {
        int safeLimit = sanitizeLimit(limit);
        StringBuilder where = new StringBuilder(" WHERE 1 = 1 ");
        List<Object> params = new ArrayList<>();

        if (symbol != null && !symbol.isBlank()) {
            where.append(" AND symbol = ? ");
            params.add(symbol);
        }

        if (exchange != null && !exchange.isBlank()) {
            where.append(" AND exchange = ? ");
            params.add(exchange);
        }

        if (interval != null && !interval.isBlank()) {
            where.append(" AND interval = ? ");
            params.add(interval);
        }

        if (startTime != null) {
            where.append(" AND start_time >= toDateTime(?) ");
            params.add(startTime.toString());
        }

        if (endTime != null) {
            where.append(" AND start_time <= toDateTime(?) ");
            params.add(endTime.toString());
        }

        String inner = """
                SELECT
                    exchange,
                    symbol,
                    interval,
                    start_time,
                    close_time,
                    event_time,
                    is_closed,
                    ingest_time,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    base_volume,
                    quote_volume,
                    trade_count,
                    amplitude_pct,
                    change_pct,
                    ma_short_period,
                    ma_short_value,
                    ma_medium_period,
                    ma_medium_value,
                    ma_long_period,
                    ma_long_value,
                    ema_short_value,
                    ema_long_value,
                    signal_type,
                    signal_strength,
                    signal_detail,
                    signal_timestamp,
                    process_time,
                    create_time
                FROM kline_metrics
                %s
                ORDER BY start_time DESC
                LIMIT %d
                """.formatted(where, safeLimit);

        String sql = """
                SELECT *
                FROM (
                    %s
                )
                ORDER BY start_time ASC
                """.formatted(inner);

        try {
            return clickhouseJdbcTemplate.query(sql, params.toArray(), new KlineMetricRowMapper());
        } catch (Exception ex) {
            log.error("查询K线时间序列失败 symbol={}, exchange={}, interval={}", symbol, exchange, interval, ex);
            return List.of();
        }
    }

    @Override
    public List<KlineIndicatorMetric> findIndicatorSeries(String symbol,
                                                          String exchange,
                                                          String interval,
                                                          List<String> indicators,
                                                          LocalDateTime startTime,
                                                          LocalDateTime endTime,
                                                          int limit) {
        int safeLimit = sanitizeLimit(limit);
        StringBuilder where = new StringBuilder(" WHERE 1 = 1 ");
        List<Object> params = new ArrayList<>();

        if (symbol != null && !symbol.isBlank()) {
            where.append(" AND symbol = ? ");
            params.add(symbol);
        }

        if (exchange != null && !exchange.isBlank()) {
            where.append(" AND exchange = ? ");
            params.add(exchange);
        }

        if (interval != null && !interval.isBlank()) {
            where.append(" AND interval = ? ");
            params.add(interval);
        }

        appendOptionalInFilter(where, params, "indicator", indicators);

        if (startTime != null) {
            where.append(" AND start_time >= toDateTime(?) ");
            params.add(startTime.toString());
        }

        if (endTime != null) {
            where.append(" AND start_time <= toDateTime(?) ");
            params.add(endTime.toString());
        }

        String inner = """
                SELECT
                    exchange,
                    symbol,
                    interval,
                    start_time,
                    end_time,
                    indicator,
                    variant,
                    value,
                    components.name AS component_names,
                    components.val AS component_values,
                    thresholds.name AS threshold_names,
                    thresholds.val AS threshold_values,
                    signal_type,
                    signal_strength,
                    signal_detail,
                    extra_tags,
                    process_time,
                    create_time
                FROM kline_indicator_metrics
                %s
                ORDER BY start_time DESC
                LIMIT %d
                """.formatted(where, safeLimit);

        String sql = """
                SELECT *
                FROM (
                    %s
                )
                ORDER BY start_time ASC
                """.formatted(inner);

        try {
            return clickhouseJdbcTemplate.query(sql, params.toArray(), new KlineIndicatorMetricRowMapper());
        } catch (Exception ex) {
            log.error("查询K线指标序列失败 symbol={}, exchange={}, interval={}, indicators={}",
                    symbol, exchange, interval, indicators, ex);
            return List.of();
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

    private String mapSnapshotSortColumn(String sortBy) {
        if (sortBy == null || sortBy.isBlank()) {
            return "quote_volume";
        }
        return switch (sortBy.toLowerCase(Locale.ROOT)) {
            case "change", "changepct" -> "change_pct";
            case "amplitude", "amplitudepct" -> "amplitude_pct";
            case "volume", "quotevolume" -> "quote_volume";
            case "basevolume" -> "base_volume";
            case "tradecount" -> "trade_count";
            case "close" -> "close_price";
            case "open" -> "open_price";
            default -> "quote_volume";
        };
    }

    private static class KlineMetricRowMapper implements RowMapper<KlineMetric> {
        @Override
        public KlineMetric mapRow(ResultSet rs, int rowNum) throws SQLException {
            KlineMetric metric = new KlineMetric();
            metric.setExchange(rs.getString("exchange"));
            metric.setSymbol(rs.getString("symbol"));
            metric.setInterval(rs.getString("interval"));

            metric.setStartTime(getDateTime(rs, "start_time"));
            metric.setCloseTime(getDateTime(rs, "close_time"));
            metric.setEventTime(getDateTime(rs, "event_time"));
            metric.setClosed(getBoolean(rs, "is_closed"));
            metric.setIngestTime(getDateTime(rs, "ingest_time"));

            metric.setOpenPrice(rs.getBigDecimal("open_price"));
            metric.setHighPrice(rs.getBigDecimal("high_price"));
            metric.setLowPrice(rs.getBigDecimal("low_price"));
            metric.setClosePrice(rs.getBigDecimal("close_price"));

            metric.setBaseVolume(rs.getBigDecimal("base_volume"));
            metric.setQuoteVolume(rs.getBigDecimal("quote_volume"));
            metric.setTradeCount(getLong(rs, "trade_count"));

            metric.setAmplitudePct(rs.getBigDecimal("amplitude_pct"));
            metric.setChangePct(rs.getBigDecimal("change_pct"));

            metric.setMaShortPeriod(getInteger(rs, "ma_short_period"));
            metric.setMaShortValue(rs.getBigDecimal("ma_short_value"));
            metric.setMaMediumPeriod(getInteger(rs, "ma_medium_period"));
            metric.setMaMediumValue(rs.getBigDecimal("ma_medium_value"));
            metric.setMaLongPeriod(getInteger(rs, "ma_long_period"));
            metric.setMaLongValue(rs.getBigDecimal("ma_long_value"));

            metric.setEmaShortValue(rs.getBigDecimal("ema_short_value"));
            metric.setEmaLongValue(rs.getBigDecimal("ema_long_value"));

            metric.setSignalType(rs.getString("signal_type"));
            metric.setSignalStrength(rs.getBigDecimal("signal_strength"));
            metric.setSignalDetail(rs.getString("signal_detail"));
            metric.setSignalTimestamp(getDateTime(rs, "signal_timestamp"));

            metric.setProcessTime(getDateTime(rs, "process_time"));
            metric.setCreateTime(getDateTime(rs, "create_time"));
            return metric;
        }
    }

    private static class KlineIndicatorMetricRowMapper implements RowMapper<KlineIndicatorMetric> {
        @Override
        public KlineIndicatorMetric mapRow(ResultSet rs, int rowNum) throws SQLException {
            KlineIndicatorMetric metric = new KlineIndicatorMetric();
            metric.setExchange(rs.getString("exchange"));
            metric.setSymbol(rs.getString("symbol"));
            metric.setInterval(rs.getString("interval"));
            metric.setStartTime(getDateTime(rs, "start_time"));
            metric.setEndTime(getDateTime(rs, "end_time"));
            metric.setIndicator(rs.getString("indicator"));
            metric.setVariant(rs.getString("variant"));
            metric.setValue(getDouble(rs, "value"));
            metric.setComponents(mapComponents(rs, "component_names", "component_values"));
            metric.setThresholds(mapThresholds(rs, "threshold_names", "threshold_values"));
            metric.setSignalType(rs.getString("signal_type"));
            metric.setSignalStrength(getDouble(rs, "signal_strength"));
            metric.setSignalDetail(rs.getString("signal_detail"));
            metric.setExtraTags(mapExtraTags(rs, "extra_tags"));
            metric.setProcessTime(getDateTime(rs, "process_time"));
            metric.setCreateTime(getDateTime(rs, "create_time"));
            return metric;
        }

        private List<KlineIndicatorComponent> mapComponents(ResultSet rs, String namesColumn, String valuesColumn) throws SQLException {
            Array namesArray = rs.getArray(namesColumn);
            Array valuesArray = rs.getArray(valuesColumn);
            Object[] names = safeArray(namesArray);
            Object[] values = safeArray(valuesArray);
            int len = Math.min(names.length, values.length);
            if (len == 0) {
                return List.of();
            }

            List<KlineIndicatorComponent> list = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                String name = names[i] != null ? names[i].toString() : null;
                Double value = convertToDouble(values[i]);
                if (name == null) {
                    continue;
                }
                KlineIndicatorComponent component = new KlineIndicatorComponent();
                component.setName(name);
                component.setValue(value);
                list.add(component);
            }
            return list;
        }

        private List<KlineIndicatorThreshold> mapThresholds(ResultSet rs, String namesColumn, String valuesColumn) throws SQLException {
            Array namesArray = rs.getArray(namesColumn);
            Array valuesArray = rs.getArray(valuesColumn);
            Object[] names = safeArray(namesArray);
            Object[] values = safeArray(valuesArray);
            int len = Math.min(names.length, values.length);
            if (len == 0) {
                return List.of();
            }

            List<KlineIndicatorThreshold> list = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                String name = names[i] != null ? names[i].toString() : null;
                Double value = convertToDouble(values[i]);
                if (name == null) {
                    continue;
                }
                KlineIndicatorThreshold threshold = new KlineIndicatorThreshold();
                threshold.setName(name);
                threshold.setValue(value);
                list.add(threshold);
            }
            return list;
        }

        @SuppressWarnings("unchecked")
        private Map<String, String> mapExtraTags(ResultSet rs, String column) throws SQLException {
            Object obj = rs.getObject(column);
            if (obj == null) {
                return Map.of();
            }
            if (obj instanceof Map<?, ?> raw) {
                Map<String, String> map = new HashMap<>();
                raw.forEach((k, v) -> map.put(Objects.toString(k, null), Objects.toString(v, null)));
                return map;
            }
            // ClickHouse JDBC 可能返回 JSON 字符串
            return Map.of("raw", obj.toString());
        }
    }

    private static Object[] safeArray(Array sqlArray) throws SQLException {
        if (sqlArray == null) {
            return new Object[0];
        }
        Object raw = sqlArray.getArray();
        if (raw == null) {
            return new Object[0];
        }
        if (raw instanceof Object[] objects) {
            return objects;
        }
        int length = java.lang.reflect.Array.getLength(raw);
        Object[] copy = new Object[length];
        for (int i = 0; i < length; i++) {
            copy[i] = java.lang.reflect.Array.get(raw, i);
        }
        return copy;
    }

    private static Double convertToDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException ignored) {
            return null;
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
        if (rs.wasNull()) {
            return null;
        }
        return value;
    }

    private static Integer getInteger(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static LocalDateTime getDateTime(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp != null ? timestamp.toLocalDateTime() : null;
    }
}

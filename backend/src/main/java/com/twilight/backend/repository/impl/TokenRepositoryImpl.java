package com.twilight.backend.repository.impl;

import com.twilight.backend.model.TokenInfo;
import com.twilight.backend.model.TokenMetrics;
import com.twilight.backend.model.TokenPriceHistory;
import com.twilight.backend.repository.TokenRepository;
import com.twilight.backend.util.RandomValueGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 代币数据访问实现类
 */
@Slf4j
@Repository
public class TokenRepositoryImpl implements TokenRepository {

    private final JdbcTemplate postgresqlJdbcTemplate;
    private final JdbcTemplate clickhouseJdbcTemplate;
    private final RandomValueGenerator randomValueGenerator;

    public TokenRepositoryImpl(@Qualifier("postgresqlJdbcTemplate") JdbcTemplate postgresqlJdbcTemplate,
                              @Qualifier("clickhouseJdbcTemplate") JdbcTemplate clickhouseJdbcTemplate,
                              RandomValueGenerator randomValueGenerator) {
        this.postgresqlJdbcTemplate = postgresqlJdbcTemplate;
        this.clickhouseJdbcTemplate = clickhouseJdbcTemplate;
        this.randomValueGenerator = randomValueGenerator;
    }

    @Override
    public TokenInfo findTokenById(Long tokenId) {
        String sql = """
            SELECT id, chain_id, chain_name, token_symbol, token_catagory,
                   token_decimals, token_address, issuer
            FROM token 
            WHERE id = ?
            """;
        
        try {
            return postgresqlJdbcTemplate.queryForObject(sql, new TokenInfoRowMapper(), tokenId);
        } catch (Exception e) {
            log.error("获取代币信息失败, tokenId: {}", tokenId, e);
            return null;
        }
    }

    @Override
    public List<TokenInfo> findAllTokens() {
        String sql = """
            SELECT id, chain_id, chain_name, token_symbol, token_catagory,
                   token_decimals, token_address, issuer
            FROM token 
            ORDER BY id
            """;
        
        try {
            return postgresqlJdbcTemplate.query(sql, new TokenInfoRowMapper());
        } catch (Exception e) {
            log.error("获取代币列表失败", e);
            return List.of();
        }
    }

    @Override
    public TokenMetrics findLatestMetrics(Long tokenId) {
        String sql = """
            SELECT 
              token_id,
              end_time,
              token_price_usd as current_price,
              mcap_usd as mcap,
              fdv_usd as fdv,
              liquidity_usd as liquidity,
              fdv_usd / nullIf(mcap_usd, 0) as fdv_mcap_ratio,
              mcap_usd / nullIf(liquidity_usd, 0) as mcap_liquidity_ratio,
              fdv_usd / nullIf(liquidity_usd, 0) as fdv_liquidity_ratio
            FROM token_recent_metric_ch 
            WHERE token_id = ? 
              AND tag = 'all'
              AND time_window = '1min'
            ORDER BY end_time DESC 
            LIMIT 1
            """;
        
        try {
            List<TokenMetrics> results = clickhouseJdbcTemplate.query(sql, new TokenMetricsRowMapper(), tokenId);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("获取代币指标失败, tokenId: {}", tokenId, e);
            return null;
        }
    }

    @Override
    public List<TokenPriceHistory> findPriceHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime) {
        return findPriceHistoryByWindow(tokenId, "1min", startTime, endTime);
    }

    @Override
    public List<TokenPriceHistory> findPriceHistoryByWindow(Long tokenId, String timeWindow, 
                                                          LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT 
              token_id,
              end_time,
              token_price_usd as price,
              volume_usd,
              mcap_usd
            FROM token_recent_metric_ch 
            WHERE token_id = ? 
              AND tag = 'all'
              AND time_window = ?
              AND end_time >= ?
              AND end_time <= ?
            ORDER BY end_time ASC
            """;
        
        try {
            // 将本地时间转换为ClickHouse UTC时间
            String startTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(startTime);
            String endTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(endTime);
            
            String finalSql = sql.replace("AND end_time >= ?", "AND end_time >= '" + startTimeStr + "'")
                                 .replace("AND end_time <= ?", "AND end_time <= '" + endTimeStr + "'");
                                 
            return clickhouseJdbcTemplate.query(finalSql, new TokenPriceHistoryRowMapper(), 
                tokenId, timeWindow);
        } catch (Exception e) {
            log.error("获取代币历史价格失败, tokenId: {}, timeWindow: {}, startTime: {}, endTime: {}", 
                tokenId, timeWindow, startTime, endTime, e);
            return List.of();
        }
    }

    /**
     * TokenInfo行映射器
     */
    private class TokenInfoRowMapper implements RowMapper<TokenInfo> {
        @Override
        public TokenInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
            TokenInfo tokenInfo = new TokenInfo();
            
            Long tokenId = rs.getLong("id");
            tokenInfo.setTokenId(tokenId);
            tokenInfo.setChainName(rs.getString("chain_name"));
            
            String symbol = rs.getString("token_symbol");
            tokenInfo.setSymbol(symbol);
            tokenInfo.setName(symbol); // name使用symbol
            
            tokenInfo.setTokenCategory(rs.getString("token_catagory"));
            tokenInfo.setDecimals(rs.getInt("token_decimals"));
            tokenInfo.setAddress(rs.getString("token_address"));
            tokenInfo.setIssuer(rs.getString("issuer"));
            
            // 生成随机值（基于tokenId种子确保一致性）
            tokenInfo.setAge(randomValueGenerator.generateAge(tokenId));
            tokenInfo.setSecurityScore(randomValueGenerator.generateSecurityScore(tokenId));
            
            // 生成简单的描述信息
            tokenInfo.setDescription(generateDescription(symbol, tokenInfo.getTokenCategory()));
            
            return tokenInfo;
        }
    }

    /**
     * TokenMetrics行映射器
     */
    private class TokenMetricsRowMapper implements RowMapper<TokenMetrics> {
        @Override
        public TokenMetrics mapRow(ResultSet rs, int rowNum) throws SQLException {
            TokenMetrics metrics = new TokenMetrics();
            
            metrics.setTokenId(rs.getLong("token_id"));
            metrics.setEndTime(rs.getTimestamp("end_time").toLocalDateTime());
            metrics.setCurrentPrice(rs.getBigDecimal("current_price"));
            metrics.setMcap(rs.getBigDecimal("mcap"));
            metrics.setFdv(rs.getBigDecimal("fdv"));
            metrics.setLiquidity(rs.getBigDecimal("liquidity"));
            
            // 计算比值，避免除零
            BigDecimal fdvMcapRatio = rs.getBigDecimal("fdv_mcap_ratio");
            metrics.setFdvMcapRatio(fdvMcapRatio != null ? fdvMcapRatio.setScale(4, RoundingMode.HALF_UP) : null);
            
            BigDecimal mcapLiquidityRatio = rs.getBigDecimal("mcap_liquidity_ratio");
            metrics.setMcapLiquidityRatio(mcapLiquidityRatio != null ? mcapLiquidityRatio.setScale(4, RoundingMode.HALF_UP) : null);
            
            BigDecimal fdvLiquidityRatio = rs.getBigDecimal("fdv_liquidity_ratio");
            metrics.setFdvLiquidityRatio(fdvLiquidityRatio != null ? fdvLiquidityRatio.setScale(4, RoundingMode.HALF_UP) : null);
            
            return metrics;
        }
    }

    /**
     * TokenPriceHistory行映射器
     */
    private class TokenPriceHistoryRowMapper implements RowMapper<TokenPriceHistory> {
        @Override
        public TokenPriceHistory mapRow(ResultSet rs, int rowNum) throws SQLException {
            TokenPriceHistory history = new TokenPriceHistory();
            
            history.setTokenId(rs.getLong("token_id"));
            history.setEndTime(rs.getTimestamp("end_time").toLocalDateTime());
            history.setPrice(rs.getBigDecimal("price"));
            history.setVolumeUsd(rs.getBigDecimal("volume_usd"));
            history.setMcap(rs.getBigDecimal("mcap_usd"));
            
            // TODO: 计算价格变化量和百分比需要前一个时间点的数据
            // 这里先设置为null，可以在Service层处理
            history.setPriceChange(null);
            history.setPriceChangePercent(null);
            
            return history;
        }
    }

    /**
     * 生成简单的代币描述
     */
    private String generateDescription(String symbol, String category) {
        if (symbol == null) return "暂无描述";
        
        String baseDesc = symbol + "是一个";
        
        if ("stablecoin".equalsIgnoreCase(category)) {
            baseDesc += "稳定币";
        } else if ("defi".equalsIgnoreCase(category)) {
            baseDesc += "DeFi代币";
        } else if ("meme".equalsIgnoreCase(category)) {
            baseDesc += "Meme代币";
        } else {
            baseDesc += "数字资产";
        }
        
        baseDesc += "，在区块链生态中发挥重要作用。";
        
        return baseDesc;
    }
}

package com.twilight.backend.repository.impl;

import com.twilight.backend.model.TagHolding;
import com.twilight.backend.model.TokenDistribution;
import com.twilight.backend.model.TopHolder;
import com.twilight.backend.repository.DistributionRepository;
import com.twilight.backend.util.LabelBitmapUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 代币分布数据访问实现类
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class DistributionRepositoryImpl implements DistributionRepository {

    @Qualifier("clickhouseJdbcTemplate")
    private final JdbcTemplate clickhouseJdbcTemplate;
    
    private final LabelBitmapUtil labelBitmapUtil;

    @Override
    public TokenDistribution findLatestDistribution(Long tokenId) {
        String sql = """
            SELECT
              token_id,
              end_time,
              holders_count,
              total_value_usd,
              median_holder_value_usd,
              avg_holder_value_usd,
              top2_value_usd,
              top2_share,
              fresh_holder_count_share,
              fresh_holder_value_share
            FROM v_token_distribution_minute
            WHERE token_id = ?
            ORDER BY end_time DESC
            LIMIT 1
            """;
        
        try {
            List<TokenDistribution> results = clickhouseJdbcTemplate.query(sql, new TokenDistributionRowMapper(), tokenId);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("获取代币分布失败, tokenId: {}", tokenId, e);
            return null;
        }
    }

    @Override
    public List<TokenDistribution> findDistributionHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT
              token_id,
              end_time,
              holders_count,
              total_value_usd,
              median_holder_value_usd,
              avg_holder_value_usd,
              top2_value_usd,
              top2_share,
              fresh_holder_count_share,
              fresh_holder_value_share
            FROM v_token_distribution_minute
            WHERE token_id = ?
              AND end_time >= ?
              AND end_time <= ?
            ORDER BY end_time ASC
            """;
        
        try {
            // 使用字符串格式避免ClickHouse的日期时间问题
            String startTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(startTime);
            String endTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(endTime);
            
            String finalSql = sql.replace("AND end_time >= ?", "AND end_time >= '" + startTimeStr + "'")
                                 .replace("AND end_time <= ?", "AND end_time <= '" + endTimeStr + "'");
                                 
            return clickhouseJdbcTemplate.query(finalSql, new TokenDistributionRowMapper(), tokenId);
        } catch (Exception e) {
            log.error("获取代币分布历史失败, tokenId: {}", tokenId, e);
            return List.of();
        }
    }

    @Override
    public List<TagHolding> findTagHoldings(Long tokenId, LocalDateTime endTime) {
        String sql = """
            SELECT
              token_id,
              end_time,
              tag,
              value_usd,
              holders_count,
              pct_change_1min
            FROM v_token_holder_tag_minute
            WHERE token_id = ?
              AND end_time = ?
            ORDER BY value_usd DESC
            """;
        
        try {
            // 使用字符串格式避免ClickHouse的日期时间问题
            String endTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(endTime);
            
            String finalSql = sql.replace("AND end_time = ?", "AND end_time = '" + endTimeStr + "'");
                                 
            return clickhouseJdbcTemplate.query(finalSql, new TagHoldingRowMapper(), tokenId);
        } catch (Exception e) {
            log.error("获取标签持仓失败, tokenId: {}, endTime: {}", tokenId, endTime, e);
            return List.of();
        }
    }

    @Override
    public List<TagHolding> findTagHoldingsHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT
              token_id,
              end_time,
              tag,
              value_usd,
              holders_count,
              pct_change_1min
            FROM v_token_holder_tag_minute
            WHERE token_id = ?
              AND end_time >= ?
              AND end_time <= ?
            ORDER BY end_time ASC, tag ASC
            """;
        
        try {
            return clickhouseJdbcTemplate.query(sql, new TagHoldingRowMapper(), 
                tokenId, startTime, endTime);
        } catch (Exception e) {
            log.error("获取标签持仓历史失败, tokenId: {}", tokenId, e);
            return List.of();
        }
    }

    @Override
    public List<TopHolder> findTopHolders(Long tokenId, Integer limit) {
        String sql = """
            SELECT
              h.token_id,
              h.end_time,
              h.account_id,
              h.value_usd,
              h.ownership_pct,
              h.amount,
              h.label_mask,
              a.address
            FROM v_token_top_holders_latest h
            LEFT JOIN account a ON h.account_id = a.id
            WHERE h.token_id = ?
            ORDER BY h.value_usd DESC
            LIMIT ?
            """;
        
        try {
            return clickhouseJdbcTemplate.query(sql, new TopHolderRowMapper(), tokenId, limit);
        } catch (Exception e) {
            log.error("获取Top Holder失败, tokenId: {}", tokenId, e);
            return List.of();
        }
    }

    @Override
    public List<TopHolder> findTopHolders(Long tokenId, LocalDateTime endTime, Integer limit) {
        String sql = """
            SELECT
              token_id,
              end_time,
              account_id,
              value_usd,
              round(value_usd / nullIf(sum(value_usd) OVER (PARTITION BY token_id, end_time),0), 6) AS ownership_pct,
              amount,
              label_mask,
              a.address
            FROM ch_token_holder_balance_minute h
            LEFT JOIN account a ON h.account_id = a.id
            WHERE h.token_id = ?
              AND h.end_time = ?
              AND h.value_usd > 0
            ORDER BY h.value_usd DESC
            LIMIT ?
            """;
        
        try {
            return clickhouseJdbcTemplate.query(sql, new TopHolderRowMapper(), tokenId, endTime, limit);
        } catch (Exception e) {
            log.error("获取指定时间Top Holder失败, tokenId: {}, endTime: {}", tokenId, endTime, e);
            return List.of();
        }
    }

    /**
     * TokenDistribution行映射器
     */
    private static class TokenDistributionRowMapper implements RowMapper<TokenDistribution> {
        @Override
        public TokenDistribution mapRow(ResultSet rs, int rowNum) throws SQLException {
            TokenDistribution distribution = new TokenDistribution();
            
            distribution.setTokenId(rs.getLong("token_id"));
            distribution.setEndTime(rs.getTimestamp("end_time").toLocalDateTime());
            distribution.setHoldersCount(rs.getInt("holders_count"));
            distribution.setTotalValueUsd(rs.getBigDecimal("total_value_usd"));
            distribution.setMedianHolderValueUsd(rs.getBigDecimal("median_holder_value_usd"));
            distribution.setAvgHolderValueUsd(rs.getBigDecimal("avg_holder_value_usd"));
            distribution.setTop2ValueUsd(rs.getBigDecimal("top2_value_usd"));
            distribution.setTop2SharePercent(rs.getDouble("top2_share") * 100); // 转换为百分比
            distribution.setFreshHolderCountShare(rs.getDouble("fresh_holder_count_share") * 100);
            distribution.setFreshHolderValueShare(rs.getDouble("fresh_holder_value_share") * 100);
            
            // 计算集中度指数（基于Top2占比）
            Double top2Share = rs.getDouble("top2_share");
            if (top2Share != null) {
                distribution.setConcentrationIndex(calculateConcentrationIndex(top2Share));
            }
            
            return distribution;
        }
        
        private Double calculateConcentrationIndex(Double top2Share) {
            // 简单的集中度计算：0-1之间，越接近1越集中
            if (top2Share >= 0.8) return 0.9; // 高度集中
            if (top2Share >= 0.5) return 0.7; // 中度集中
            if (top2Share >= 0.2) return 0.5; // 轻度集中
            return 0.3; // 分散
        }
    }

    /**
     * TagHolding行映射器
     */
    private static class TagHoldingRowMapper implements RowMapper<TagHolding> {
        @Override
        public TagHolding mapRow(ResultSet rs, int rowNum) throws SQLException {
            TagHolding holding = new TagHolding();
            
            holding.setTokenId(rs.getLong("token_id"));
            holding.setEndTime(rs.getTimestamp("end_time").toLocalDateTime());
            
            String tag = rs.getString("tag");
            holding.setTag(tag);
            holding.setTagName(getTagDisplayName(tag));
            
            holding.setValueUsd(rs.getBigDecimal("value_usd"));
            holding.setHoldersCount(rs.getInt("holders_count"));
            holding.setChangePercent1Min(rs.getDouble("pct_change_1min") * 100); // 转换为百分比
            
            return holding;
        }
        
        private String getTagDisplayName(String tag) {
            return switch (tag.toLowerCase()) {
                case "fresh_wallet" -> "新钱包";
                case "whale" -> "巨鲸";
                case "smart_money" -> "聪明钱";
                case "cex" -> "交易所";
                default -> tag;
            };
        }
    }

    /**
     * TopHolder行映射器
     */
    private class TopHolderRowMapper implements RowMapper<TopHolder> {
        @Override
        public TopHolder mapRow(ResultSet rs, int rowNum) throws SQLException {
            TopHolder holder = new TopHolder();
            
            holder.setTokenId(rs.getLong("token_id"));
            holder.setEndTime(rs.getTimestamp("end_time").toLocalDateTime());
            holder.setAccountId(rs.getLong("account_id"));
            holder.setAddress(rs.getString("address"));
            holder.setValueUsd(rs.getBigDecimal("value_usd"));
            holder.setOwnershipPercent(rs.getDouble("ownership_pct") * 100); // 转换为百分比
            holder.setBalance(rs.getBigDecimal("amount"));
            holder.setRank(rowNum + 1); // 设置排名
            
            Integer labelMask = rs.getInt("label_mask");
            holder.setLabelMask(labelMask);
            holder.setLabels(labelBitmapUtil.parseLabels(labelMask));
            
            return holder;
        }
    }
}

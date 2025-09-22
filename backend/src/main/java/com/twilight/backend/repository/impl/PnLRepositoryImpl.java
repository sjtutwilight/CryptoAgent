package com.twilight.backend.repository.impl;

import com.twilight.backend.model.MacroPnLMetrics;
import com.twilight.backend.model.TopPnLInfo;
import com.twilight.backend.repository.PnLRepository;
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
 * PnL数据访问实现类
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class PnLRepositoryImpl implements PnLRepository {

    @Qualifier("clickhouseJdbcTemplate")
    private final JdbcTemplate clickhouseJdbcTemplate;
    
    @Qualifier("postgresqlJdbcTemplate")
    private final JdbcTemplate postgresqlJdbcTemplate;
    
    private final LabelBitmapUtil labelBitmapUtil;

    @Override
    public List<TopPnLInfo> findTopPnL(Long tokenId, Integer limit) {
        String sql = """
            SELECT 
              p.account_id,
              p.token_id,
              p.position,
              p.avg_cost,
              p.realized_cost_usd,
              p.realized_proceeds_usd,
              p.realized_pnl_usd,
              p.last_price_usd,
              p.unrealized_pnl_usd,
              p.total_pnl_usd,
              p.roi_pct,
              p.holding_pct,
              p.last_tx_time,
              a.address,
              a.tag_bitmap
            FROM ch_account_pnl_current_ma p
            LEFT JOIN account a ON p.account_id = a.id  
            WHERE p.token_id = ?
            ORDER BY p.total_pnl_usd DESC
            LIMIT ?
            """;
        
        try {
            return clickhouseJdbcTemplate.query(sql, new TopPnLInfoRowMapper(), tokenId, limit);
        } catch (Exception e) {
            log.error("获取Top PnL失败, tokenId: {}", tokenId, e);
            return List.of();
        }
    }

    @Override
    public MacroPnLMetrics findLatestMacroPnL(Long tokenId) {
        String sql = """
            SELECT
              token_id,
              end_time,
              mcap_usd,
              realized_cap_usd,
              network_value_usd,
              unrealized_profit_usd,
              unrealized_loss_usd,
              nupl,
              mvrv,
              nvt_ratio,
              sopr,
              realized_pnl_usd,
              has_mcap,
              has_realized_cap,
              has_unrealized_pnl,
              has_sopr,
              last_updated
            FROM v_token_macro_minute
            WHERE token_id = ?
            ORDER BY end_time DESC
            LIMIT 1
            """;
        
        try {
            List<MacroPnLMetrics> results = clickhouseJdbcTemplate.query(sql, new MacroPnLMetricsRowMapper(), tokenId);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("获取宏观PnL指标失败, tokenId: {}", tokenId, e);
            return null;
        }
    }

    @Override
    public List<MacroPnLMetrics> findMacroPnLHistory(Long tokenId, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT
              token_id,
              end_time,
              mcap_usd,
              realized_cap_usd,
              network_value_usd,
              unrealized_profit_usd,
              unrealized_loss_usd,
              nupl,
              mvrv,
              nvt_ratio,
              sopr,
              realized_pnl_usd,
              has_mcap,
              has_realized_cap,
              has_unrealized_pnl,
              has_sopr,
              last_updated
            FROM v_token_macro_minute
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
                                 
            return clickhouseJdbcTemplate.query(finalSql, new MacroPnLMetricsRowMapper(), tokenId);
        } catch (Exception e) {
            log.error("获取宏观PnL历史失败, tokenId: {}", tokenId, e);
            return List.of();
        }
    }

    @Override
    public TopPnLInfo findAccountPnL(Long accountId, Long tokenId) {
        String sql = """
            SELECT 
              p.account_id,
              p.token_id,
              p.position,
              p.avg_cost,
              p.realized_cost_usd,
              p.realized_proceeds_usd,
              p.realized_pnl_usd,
              p.last_price_usd,
              p.unrealized_pnl_usd,
              p.total_pnl_usd,
              p.roi_pct,
              p.holding_pct,
              p.last_tx_time,
              a.address,
              a.tag_bitmap
            FROM ch_account_pnl_current_ma p
            LEFT JOIN account a ON p.account_id = a.id  
            WHERE p.account_id = ? AND p.token_id = ?
            """;
        
        try {
            List<TopPnLInfo> results = clickhouseJdbcTemplate.query(sql, new TopPnLInfoRowMapper(), 
                accountId, tokenId);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("获取账户PnL失败, accountId: {}, tokenId: {}", accountId, tokenId, e);
            return null;
        }
    }

    @Override
    public List<TopPnLInfo> findAccountAllPnL(Long accountId, Integer limit) {
        String sql = """
            SELECT 
              p.account_id,
              p.token_id,
              p.position,
              p.avg_cost,
              p.realized_cost_usd,
              p.realized_proceeds_usd,
              p.realized_pnl_usd,
              p.last_price_usd,
              p.unrealized_pnl_usd,
              p.total_pnl_usd,
              p.roi_pct,
              p.holding_pct,
              p.last_tx_time,
              a.address,
              a.tag_bitmap
            FROM ch_account_pnl_current_ma p
            LEFT JOIN account a ON p.account_id = a.id  
            WHERE p.account_id = ?
            ORDER BY p.total_pnl_usd DESC
            LIMIT ?
            """;
        
        try {
            return clickhouseJdbcTemplate.query(sql, new TopPnLInfoRowMapper(), accountId, limit);
        } catch (Exception e) {
            log.error("获取账户所有PnL失败, accountId: {}", accountId, e);
            return List.of();
        }
    }

    /**
     * TopPnLInfo行映射器
     */
    private class TopPnLInfoRowMapper implements RowMapper<TopPnLInfo> {
        @Override
        public TopPnLInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
            TopPnLInfo info = new TopPnLInfo();
            
            info.setAccountId(rs.getLong("account_id"));
            info.setAddress(rs.getString("address"));
            info.setTokenId(rs.getLong("token_id"));
            info.setTotalPnlUsd(rs.getBigDecimal("total_pnl_usd"));
            info.setRoiPercent(rs.getDouble("roi_pct"));
            info.setRealizedPnlUsd(rs.getBigDecimal("realized_pnl_usd"));
            info.setUnrealizedPnlUsd(rs.getBigDecimal("unrealized_pnl_usd"));
            info.setStillHoldingPercent(rs.getDouble("holding_pct"));
            info.setPosition(rs.getBigDecimal("position"));
            info.setAvgCost(rs.getBigDecimal("avg_cost"));
            info.setLastPrice(rs.getBigDecimal("last_price_usd"));
            info.setLastTxTime(rs.getTimestamp("last_tx_time").toLocalDateTime());
            
            Integer labelMask = rs.getInt("tag_bitmap");
            info.setLabelMask(labelMask);
            info.setLabels(labelBitmapUtil.parseLabels(labelMask));
            
            return info;
        }
    }

    /**
     * MacroPnLMetrics行映射器
     */
    private static class MacroPnLMetricsRowMapper implements RowMapper<MacroPnLMetrics> {
        @Override
        public MacroPnLMetrics mapRow(ResultSet rs, int rowNum) throws SQLException {
            MacroPnLMetrics metrics = new MacroPnLMetrics();
            
            metrics.setTokenId(rs.getLong("token_id"));
            metrics.setEndTime(rs.getTimestamp("end_time").toLocalDateTime());
            metrics.setMcapUsd(rs.getBigDecimal("mcap_usd"));
            metrics.setRealizedCapUsd(rs.getBigDecimal("realized_cap_usd"));
            metrics.setNetworkValueUsd(rs.getBigDecimal("network_value_usd"));
            metrics.setUnrealizedProfitUsd(rs.getBigDecimal("unrealized_profit_usd"));
            metrics.setUnrealizedLossUsd(rs.getBigDecimal("unrealized_loss_usd"));
            metrics.setNupl(rs.getDouble("nupl"));
            metrics.setMvrv(rs.getDouble("mvrv"));
            metrics.setNvtRatio(rs.getDouble("nvt_ratio"));
            metrics.setSopr(rs.getDouble("sopr"));
            metrics.setRealizedPnlUsd(rs.getBigDecimal("realized_pnl_usd"));
            metrics.setHasMcap(rs.getBoolean("has_mcap"));
            metrics.setHasRealizedCap(rs.getBoolean("has_realized_cap"));
            metrics.setHasUnrealizedPnl(rs.getBoolean("has_unrealized_pnl"));
            metrics.setHasSopr(rs.getBoolean("has_sopr"));
            metrics.setLastUpdated(rs.getTimestamp("last_updated").toLocalDateTime());
            
            return metrics;
        }
    }
}

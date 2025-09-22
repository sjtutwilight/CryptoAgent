package com.twilight.backend.repository.impl;

import com.twilight.backend.model.DexTradeDetail;
import com.twilight.backend.model.PageResult;
import com.twilight.backend.model.TagNetFlow;
import com.twilight.backend.model.TokenTradeVolume;
import com.twilight.backend.repository.TradeRepository;
import com.twilight.backend.util.LabelBitmapUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 交易数据访问实现类
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class TradeRepositoryImpl implements TradeRepository {

    @Qualifier("clickhouseJdbcTemplate")
    private final JdbcTemplate clickhouseJdbcTemplate;
    
    private final LabelBitmapUtil labelBitmapUtil;

    @Override
    public List<TokenTradeVolume> findTradeVolume(Long tokenId, String timeWindow, String tag, 
                                                LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT 
              token_id,
              time_window,
              end_time,
              tag,
              txcnt,
              buy_count,
              sell_count,
              volume_usd,
              buy_volume_usd,
              sell_volume_usd,
              buy_pressure_usd,
              token_price_usd
            FROM token_recent_metric_ch 
            WHERE token_id = ? 
              AND time_window = ?
              AND tag = ?
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
                                 
            return clickhouseJdbcTemplate.query(finalSql, new TokenTradeVolumeRowMapper(), 
                tokenId, timeWindow, tag);
        } catch (Exception e) {
            log.error("获取交易量统计失败, tokenId: {}, timeWindow: {}, tag: {}", tokenId, timeWindow, tag, e);
            return List.of();
        }
    }

    @Override
    public List<TagNetFlow> calculateNetFlow(Long tokenId, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT 
              token_id,
              end_time,
              tag,
              buy_volume_usd - sell_volume_usd as net_flow_usd,
              buy_volume_usd as inflow_usd,
              sell_volume_usd as outflow_usd,
              buy_count + sell_count as traders_count,
              buy_count,
              sell_count
            FROM token_recent_metric_ch 
            WHERE token_id = ? 
              AND tag = 'all'
              AND time_window = '1min'
              AND end_time >= ?
              AND end_time <= ?
            ORDER BY end_time ASC, tag ASC
            """;
        
        try {
            // 将本地时间转换为ClickHouse UTC时间
            String startTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(startTime);
            String endTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(endTime);
            
            String finalSql = sql.replace("AND end_time >= ?", "AND end_time >= '" + startTimeStr + "'")
                                 .replace("AND end_time <= ?", "AND end_time <= '" + endTimeStr + "'");
                                 
            return clickhouseJdbcTemplate.query(finalSql, new TagNetFlowRowMapper(), tokenId);
        } catch (Exception e) {
            log.error("计算标签净流入失败, tokenId: {}", tokenId, e);
            return List.of();
        }
    }

    @Override
    public PageResult<DexTradeDetail> findDexTradesByToken(Long tokenId, Integer offset, Integer limit, 
                                                         LocalDateTime startTime, LocalDateTime endTime) {
        // 先查询总数
        Long total = countDexTradesByToken(tokenId, startTime, endTime);
        
        if (total == 0) {
            return PageResult.empty(offset / limit + 1, limit);
        }
        
        String sql = """
            SELECT 
              tx_hash,
              block_time,
              CASE 
                WHEN side = 'BUY' THEN pair_address
                ELSE account_address
              END as from_address,
              CASE 
                WHEN side = 'BUY' THEN account_address  
                ELSE pair_address
              END as to_address,
              token_id,
              side,
              qty,
              price_usd,
              value_usd,
              pair_id,
              log_index,
              label_mask
            FROM ch_account_trade_fact
            WHERE token_id = ?
              AND block_time >= ?
              AND block_time <= ?
            ORDER BY block_time DESC
            LIMIT ? OFFSET ?
            """;
        
        try {
            // 使用字符串格式避免ClickHouse的日期时间问题
            String startTimeStr = startTime.withNano(0).toString().replace("T", " ");
            String endTimeStr = endTime.withNano(0).toString().replace("T", " ");
            
            String finalSql = sql.replace("AND block_time >= ?", "AND block_time >= '" + startTimeStr + "'")
                                 .replace("AND block_time <= ?", "AND block_time <= '" + endTimeStr + "'");
                                 
            List<DexTradeDetail> trades = clickhouseJdbcTemplate.query(finalSql, new DexTradeDetailRowMapper(), 
                tokenId, limit, offset);
            
            int page = offset / limit + 1;
            return PageResult.of(trades, page, limit, total);
        } catch (Exception e) {
            log.error("获取DEX交易明细失败, tokenId: {}", tokenId, e);
            return PageResult.empty(offset / limit + 1, limit);
        }
    }

    @Override
    public PageResult<DexTradeDetail> findDexTradesByAccount(Long accountId, Integer offset, Integer limit, 
                                                           LocalDateTime startTime, LocalDateTime endTime) {
        Long total = countDexTradesByAccount(accountId, startTime, endTime);
        
        if (total == 0) {
            return PageResult.empty(offset / limit + 1, limit);
        }
        
        String sql = """
            SELECT 
              tx_hash,
              block_time,
              CASE 
                WHEN side = 'BUY' THEN pair_address
                ELSE account_address
              END as from_address,
              CASE 
                WHEN side = 'BUY' THEN account_address  
                ELSE pair_address
              END as to_address,
              token_id,
              side,
              qty,
              price_usd,
              value_usd,
              pair_id,
              log_index,
              label_mask
            FROM ch_account_trade_fact
            WHERE account_id = ?
              AND block_time >= ?
              AND block_time <= ?
            ORDER BY block_time DESC
            LIMIT ? OFFSET ?
            """;
        
        try {
            List<DexTradeDetail> trades = clickhouseJdbcTemplate.query(sql, new DexTradeDetailRowMapper(), 
                accountId, startTime, endTime, limit, offset);
            
            int page = offset / limit + 1;
            return PageResult.of(trades, page, limit, total);
        } catch (Exception e) {
            log.error("获取账户交易明细失败, accountId: {}", accountId, e);
            return PageResult.empty(offset / limit + 1, limit);
        }
    }

    @Override
    public Long countDexTradesByToken(Long tokenId, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT COUNT(*) 
            FROM ch_account_trade_fact
            WHERE token_id = ?
              AND block_time >= ?
              AND block_time <= ?
            """;
        
        try {
            return clickhouseJdbcTemplate.queryForObject(sql, Long.class, tokenId, startTime, endTime);
        } catch (Exception e) {
            log.error("获取代币交易总数失败, tokenId: {}", tokenId, e);
            return 0L;
        }
    }

    @Override
    public Long countDexTradesByAccount(Long accountId, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT COUNT(*) 
            FROM ch_account_trade_fact
            WHERE account_id = ?
              AND block_time >= ?
              AND block_time <= ?
            """;
        
        try {
            return clickhouseJdbcTemplate.queryForObject(sql, Long.class, accountId, startTime, endTime);
        } catch (Exception e) {
            log.error("获取账户交易总数失败, accountId: {}", accountId, e);
            return 0L;
        }
    }

    /**
     * TokenTradeVolume行映射器
     */
    private static class TokenTradeVolumeRowMapper implements RowMapper<TokenTradeVolume> {
        @Override
        public TokenTradeVolume mapRow(ResultSet rs, int rowNum) throws SQLException {
            TokenTradeVolume volume = new TokenTradeVolume();
            
            volume.setTokenId(rs.getLong("token_id"));
            volume.setTimeWindow(rs.getString("time_window"));
            volume.setEndTime(rs.getTimestamp("end_time").toLocalDateTime());
            volume.setTag(rs.getString("tag"));
            volume.setTxCount(rs.getInt("txcnt"));
            volume.setBuyCount(rs.getInt("buy_count"));
            volume.setSellCount(rs.getInt("sell_count"));
            volume.setVolumeUsd(rs.getBigDecimal("volume_usd"));
            volume.setBuyVolumeUsd(rs.getBigDecimal("buy_volume_usd"));
            volume.setSellVolumeUsd(rs.getBigDecimal("sell_volume_usd"));
            volume.setBuyPressureUsd(rs.getBigDecimal("buy_pressure_usd"));
            volume.setTokenPriceUsd(rs.getBigDecimal("token_price_usd"));
            
            return volume;
        }
    }

    /**
     * TagNetFlow行映射器
     */
    private class TagNetFlowRowMapper implements RowMapper<TagNetFlow> {
        @Override
        public TagNetFlow mapRow(ResultSet rs, int rowNum) throws SQLException {
            TagNetFlow netFlow = new TagNetFlow();
            
            netFlow.setTokenId(rs.getLong("token_id"));
            netFlow.setEndTime(rs.getTimestamp("end_time").toLocalDateTime());
            
            String tag = rs.getString("tag");
            netFlow.setTag(tag);
            netFlow.setTagName(getTagDisplayName(tag));
            
            netFlow.setNetFlowUsd(rs.getBigDecimal("net_flow_usd"));
            netFlow.setInflowUsd(rs.getBigDecimal("inflow_usd"));
            netFlow.setOutflowUsd(rs.getBigDecimal("outflow_usd"));
            netFlow.setTradersCount(rs.getInt("traders_count"));
            netFlow.setBuyCount(rs.getInt("buy_count"));
            netFlow.setSellCount(rs.getInt("sell_count"));
            
            return netFlow;
        }
        
        private String getTagDisplayName(String tag) {
            return switch (tag.toLowerCase()) {
                case "exchange", "cex" -> "交易所";
                case "smart_money" -> "聪明钱";
                case "whale" -> "巨鲸";
                case "fresh_wallet" -> "新钱包";
                default -> tag;
            };
        }
    }

    /**
     * DexTradeDetail行映射器
     */
    private class DexTradeDetailRowMapper implements RowMapper<DexTradeDetail> {
        @Override
        public DexTradeDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
            DexTradeDetail detail = new DexTradeDetail();
            
            detail.setTxHash(rs.getString("tx_hash"));
            detail.setBlockTime(rs.getTimestamp("block_time").toLocalDateTime());
            detail.setFromAddress(rs.getString("from_address"));
            detail.setToAddress(rs.getString("to_address"));
            detail.setTokenId(rs.getLong("token_id"));
            detail.setSide(rs.getString("side"));
            detail.setQty(rs.getBigDecimal("qty"));
            detail.setPriceUsd(rs.getBigDecimal("price_usd"));
            detail.setValueUsd(rs.getBigDecimal("value_usd"));
            detail.setPairId(rs.getLong("pair_id"));
            detail.setLogIndex(rs.getInt("log_index"));
            
            Integer labelMask = rs.getInt("label_mask");
            detail.setLabelMask(labelMask);
            detail.setLabels(labelBitmapUtil.parseLabels(labelMask));
            
            return detail;
        }
    }
}

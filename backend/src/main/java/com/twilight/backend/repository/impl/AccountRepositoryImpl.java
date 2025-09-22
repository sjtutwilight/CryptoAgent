package com.twilight.backend.repository.impl;

import com.twilight.backend.model.AccountAsset;
import com.twilight.backend.model.AccountInfo;
import com.twilight.backend.model.AccountTransferHistory;
import com.twilight.backend.repository.AccountRepository;
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
 * 账户数据访问实现类
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class AccountRepositoryImpl implements AccountRepository {

    @Qualifier("postgresqlJdbcTemplate")
    private final JdbcTemplate postgresqlJdbcTemplate;
    
    @Qualifier("clickhouseJdbcTemplate")
    private final JdbcTemplate clickhouseJdbcTemplate;
    
    private final LabelBitmapUtil labelBitmapUtil;

    @Override
    public AccountInfo findAccountById(Long accountId) {
        String sql = """
            SELECT id, chain_id, chain_name, address, entity, tag_bitmap
            FROM account 
            WHERE id = ?
            """;
        
        try {
            List<AccountInfo> results = postgresqlJdbcTemplate.query(sql, new AccountInfoRowMapper(), accountId);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("获取账户信息失败, accountId: {}", accountId, e);
            return null;
        }
    }

    @Override
    public AccountInfo findAccountByAddress(String address) {
        String sql = """
            SELECT id, chain_id, chain_name, address, entity, tag_bitmap
            FROM account 
            WHERE LOWER(address) = LOWER(?)
            """;
        
        try {
            List<AccountInfo> results = postgresqlJdbcTemplate.query(sql, new AccountInfoRowMapper(), address);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("获取账户信息失败, address: {}", address, e);
            return null;
        }
    }

    @Override
    public List<AccountInfo> findAllAccounts(Integer offset, Integer limit) {
        String sql = """
            SELECT id, chain_id, chain_name, address, entity, tag_bitmap
            FROM account 
            ORDER BY id
            LIMIT ? OFFSET ?
            """;
        
        try {
            return postgresqlJdbcTemplate.query(sql, new AccountInfoRowMapper(), limit, offset);
        } catch (Exception e) {
            log.error("获取账户列表失败", e);
            return List.of();
        }
    }

    @Override
    public Long countAccounts() {
        String sql = "SELECT COUNT(*) FROM account";
        
        try {
            return postgresqlJdbcTemplate.queryForObject(sql, Long.class);
        } catch (Exception e) {
            log.error("获取账户总数失败", e);
            return 0L;
        }
    }

    @Override
    public List<AccountAsset> findAccountAssets(Long accountId) {
        String sql = """
            SELECT 
              account_id,
              asset_type,
              biz_id,
              CASE 
                WHEN asset_type = 'erc20' THEN (SELECT token_symbol FROM token WHERE id = biz_id)
                WHEN asset_type = 'lp' THEN 'LP Token'
                ELSE 'ETH'
              END as biz_name,
              amount,
              value_usd,
              price_usd,
              observed_time,
              label_mask
            FROM ch_account_balance_snapshot
            WHERE account_id = ?
              AND observed_time = (
                SELECT max(observed_time) 
                FROM ch_account_balance_snapshot 
                WHERE account_id = ?
              )
            ORDER BY value_usd DESC
            """;
        
        try {
            return clickhouseJdbcTemplate.query(sql, new AccountAssetRowMapper(), accountId, accountId);
        } catch (Exception e) {
            log.error("获取账户资产失败, accountId: {}", accountId, e);
            return List.of();
        }
    }

    @Override
    public List<AccountAsset> findAccountAssetsByType(Long accountId, String assetType) {
        String sql = """
            SELECT 
              account_id,
              asset_type,
              biz_id,
              CASE 
                WHEN asset_type = 'erc20' THEN (SELECT token_symbol FROM token WHERE id = biz_id)
                WHEN asset_type = 'lp' THEN 'LP Token'
                ELSE 'ETH'
              END as biz_name,
              amount,
              value_usd,
              price_usd,
              observed_time,
              label_mask
            FROM ch_account_balance_snapshot
            WHERE account_id = ?
              AND asset_type = ?
              AND observed_time = (
                SELECT max(observed_time) 
                FROM ch_account_balance_snapshot 
                WHERE account_id = ? AND asset_type = ?
              )
            ORDER BY value_usd DESC
            """;
        
        try {
            return clickhouseJdbcTemplate.query(sql, new AccountAssetRowMapper(), 
                accountId, assetType, accountId, assetType);
        } catch (Exception e) {
            log.error("获取账户指定类型资产失败, accountId: {}, assetType: {}", accountId, assetType, e);
            return List.of();
        }
    }

    @Override
    public List<AccountTransferHistory> findTransferHistory(Long accountId, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT
              account_id,
              end_time,
              SUM(CASE WHEN side = 'BUY' THEN trade_cnt ELSE 0 END) as buy_tx_count,
              SUM(CASE WHEN side = 'SELL' THEN trade_cnt ELSE 0 END) as sell_tx_count,
              SUM(trade_cnt) as total_tx_count,
              SUM(CASE WHEN side = 'BUY' THEN volume_usd ELSE 0 END) as buy_volume_usd,
              SUM(CASE WHEN side = 'SELL' THEN volume_usd ELSE 0 END) as sell_volume_usd,
              SUM(volume_usd) as total_volume_usd
            FROM ch_account_trade_minute
            WHERE account_id = ?
              AND end_time >= ?
              AND end_time <= ?
            GROUP BY account_id, end_time
            ORDER BY end_time ASC
            """;
        
        try {
            // 使用字符串格式避免ClickHouse的日期时间问题
            String startTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(startTime);
            String endTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(endTime);
            
            String finalSql = sql.replace("AND end_time >= ?", "AND end_time >= '" + startTimeStr + "'")
                                 .replace("AND end_time <= ?", "AND end_time <= '" + endTimeStr + "'");
                                 
            return clickhouseJdbcTemplate.query(finalSql, new AccountTransferHistoryRowMapper(), accountId);
        } catch (Exception e) {
            log.error("获取账户转账历史失败, accountId: {}", accountId, e);
            return List.of();
        }
    }

    @Override
    public List<AccountTransferHistory> findTransferHistoryByToken(Long accountId, Long tokenId, 
                                                                 LocalDateTime startTime, LocalDateTime endTime) {
        String sql = """
            SELECT
              account_id,
              token_id,
              end_time,
              SUM(CASE WHEN side = 'BUY' THEN trade_cnt ELSE 0 END) as buy_tx_count,
              SUM(CASE WHEN side = 'SELL' THEN trade_cnt ELSE 0 END) as sell_tx_count,
              SUM(trade_cnt) as total_tx_count,
              SUM(CASE WHEN side = 'BUY' THEN volume_usd ELSE 0 END) as buy_volume_usd,
              SUM(CASE WHEN side = 'SELL' THEN volume_usd ELSE 0 END) as sell_volume_usd,
              SUM(volume_usd) as total_volume_usd
            FROM ch_account_trade_minute
            WHERE account_id = ?
              AND token_id = ?
              AND end_time >= ?
              AND end_time <= ?
            GROUP BY account_id, token_id, end_time
            ORDER BY end_time ASC
            """;
        
        try {
            // 使用字符串格式避免ClickHouse的日期时间问题
            String startTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(startTime);
            String endTimeStr = com.twilight.backend.util.TimeZoneUtil.toClickHouseTimeString(endTime);
            
            String finalSql = sql.replace("AND end_time >= ?", "AND end_time >= '" + startTimeStr + "'")
                                 .replace("AND end_time <= ?", "AND end_time <= '" + endTimeStr + "'");
                                 
            return clickhouseJdbcTemplate.query(finalSql, new AccountTransferHistoryRowMapper(), 
                accountId, tokenId);
        } catch (Exception e) {
            log.error("获取账户指定代币转账历史失败, accountId: {}, tokenId: {}", accountId, tokenId, e);
            return List.of();
        }
    }

    /**
     * AccountInfo行映射器
     */
    private class AccountInfoRowMapper implements RowMapper<AccountInfo> {
        @Override
        public AccountInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
            AccountInfo info = new AccountInfo();
            
            info.setAccountId(rs.getLong("id"));
            info.setChainName(rs.getString("chain_name"));
            info.setAddress(rs.getString("address"));
            info.setEntity(rs.getString("entity"));
            
            Integer tagBitmap = rs.getInt("tag_bitmap");
            info.setTagBitmap(tagBitmap);
            info.setLabels(labelBitmapUtil.parseLabels(tagBitmap));
            
            return info;
        }
    }

    /**
     * AccountAsset行映射器
     */
    private static class AccountAssetRowMapper implements RowMapper<AccountAsset> {
        @Override
        public AccountAsset mapRow(ResultSet rs, int rowNum) throws SQLException {
            AccountAsset asset = new AccountAsset();
            
            asset.setAccountId(rs.getLong("account_id"));
            asset.setAssetType(rs.getString("asset_type"));
            asset.setBizId(rs.getLong("biz_id"));
            asset.setBizName(rs.getString("biz_name"));
            asset.setAmount(rs.getBigDecimal("amount"));
            asset.setValueUsd(rs.getBigDecimal("value_usd"));
            asset.setPrice(rs.getBigDecimal("price_usd"));
            asset.setObservedTime(rs.getTimestamp("observed_time").toLocalDateTime());
            asset.setLabelMask(rs.getInt("label_mask"));
            
            return asset;
        }
    }

    /**
     * AccountTransferHistory行映射器
     */
    private static class AccountTransferHistoryRowMapper implements RowMapper<AccountTransferHistory> {
        @Override
        public AccountTransferHistory mapRow(ResultSet rs, int rowNum) throws SQLException {
            AccountTransferHistory history = new AccountTransferHistory();
            
            history.setAccountId(rs.getLong("account_id"));
            
            // 检查是否有token_id列
            try {
                history.setTokenId(rs.getLong("token_id"));
            } catch (SQLException e) {
                // token_id列不存在，跳过
            }
            
            history.setEndTime(rs.getTimestamp("end_time").toLocalDateTime());
            history.setBuyTxCount(rs.getInt("buy_tx_count"));
            history.setSellTxCount(rs.getInt("sell_tx_count"));
            history.setTotalTxCount(rs.getInt("total_tx_count"));
            history.setBuyVolumeUsd(rs.getBigDecimal("buy_volume_usd"));
            history.setSellVolumeUsd(rs.getBigDecimal("sell_volume_usd"));
            history.setTotalVolumeUsd(rs.getBigDecimal("total_volume_usd"));
            
            // 计算净买入量
            history.setNetBuyVolumeUsd(
                history.getBuyVolumeUsd().subtract(history.getSellVolumeUsd())
            );
            
            return history;
        }
    }
}

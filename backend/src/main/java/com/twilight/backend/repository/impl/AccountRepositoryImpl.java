package com.twilight.backend.repository.impl;

import com.twilight.backend.model.AccountDetail;
import com.twilight.backend.repository.AccountRepository;
import com.twilight.backend.util.RandomValueGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * 账户数据访问实现类
 */
@Slf4j
@Repository
public class AccountRepositoryImpl implements AccountRepository {

    private final JdbcTemplate postgresJdbcTemplate;
    private final JdbcTemplate clickhouseJdbcTemplate;

    public AccountRepositoryImpl(
            @Qualifier("postgresqlJdbcTemplate") JdbcTemplate postgresJdbcTemplate,
            @Qualifier("clickhouseJdbcTemplate") JdbcTemplate clickhouseJdbcTemplate) {
        this.postgresJdbcTemplate = postgresJdbcTemplate;
        this.clickhouseJdbcTemplate = clickhouseJdbcTemplate;
    }

    @Override
    public AccountDetail findAccountDetailById(Long accountId) {
        log.info("获取账户详情, accountId: {}", accountId);
        
        try {
            // 1. 获取账户基础信息
            AccountDetail.AccountInfo accountInfo = getAccountInfoById(accountId);
            if (accountInfo == null) {
                log.warn("账户不存在: {}", accountId);
                return null;
            }
            
            // 2. 获取标签信息
            AccountDetail.LabelInfo labelInfo = getLabelInfoById(accountId);
            
            // 3. 获取资产持仓
            List<AccountDetail.Asset> assets = getAssetsById(accountId);
            
            // 4. 获取转账历史
            List<AccountDetail.TransferHistory> transferHistory = getTransferHistoryById(accountId);
            
            // 5. 计算资产统计
            AccountDetail.AssetStats assetStats = calculateAssetStats(assets);
            
            // 6. 计算转账统计
            AccountDetail.TransferStats transferStats = calculateTransferStats(transferHistory);
            
            // 7. 组装结果
            AccountDetail accountDetail = new AccountDetail();
            accountDetail.setAccountInfo(accountInfo);
            accountDetail.setLabelInfo(labelInfo);
            accountDetail.setAssets(assets);
            accountDetail.setTransferHistory(transferHistory);
            accountDetail.setAssetStats(assetStats);
            accountDetail.setTransferStats(transferStats);
            
            return accountDetail;
            
        } catch (Exception e) {
            log.error("获取账户详情失败, accountId: {}", accountId, e);
            return null;
        }
    }
    
    /**
     * 获取账户基础信息
     */
    private AccountDetail.AccountInfo getAccountInfoById(Long accountId) {
        try {
            String sql = """
                SELECT 
                    address,
                    entity,
                    create_time
                FROM account
                WHERE id = ?
                LIMIT 1
                """;
            
            return postgresJdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                AccountDetail.AccountInfo info = new AccountDetail.AccountInfo();
                info.setAddress(rs.getString("address"));
                info.setEntity(rs.getString("entity"));
                info.setCreatedAt(rs.getTimestamp("create_time").toString());
                return info;
            }, accountId);
            
        } catch (Exception e) {
            log.error("获取账户基础信息失败, accountId: {}", accountId, e);
            return null;
        }
    }
    
    /**
     * 获取标签信息
     */
    private AccountDetail.LabelInfo getLabelInfoById(Long accountId) {
        try {
            String sql = """
                SELECT tag_bitmap
                FROM account
                WHERE id = ?
                """;
            
            Integer tagBitmap = postgresJdbcTemplate.queryForObject(sql, Integer.class, accountId);
            
            AccountDetail.LabelInfo labelInfo = new AccountDetail.LabelInfo();
            labelInfo.setLabels(generateLabelsFromBitmap(tagBitmap != null ? tagBitmap : 0));
            
            return labelInfo;
            
        } catch (Exception e) {
            log.error("获取标签信息失败, accountId: {}", accountId, e);
            AccountDetail.LabelInfo labelInfo = new AccountDetail.LabelInfo();
            labelInfo.setLabels(new ArrayList<>());
            return labelInfo;
        }
    }
    
    /**
     * 获取资产持仓
     */
    private List<AccountDetail.Asset> getAssetsById(Long accountId) {
        try {
            // 从ClickHouse获取最新快照的资产数据
            String sql = """
                WITH latest_snapshot AS (
                    SELECT max(snapshot_id) AS max_snapshot_id 
                    FROM ch_account_balance_snapshot
                    WHERE account_id = ?
                )
                SELECT 
                    h.biz_id as token_id,
                    h.biz_name,
                    h.asset_type,
                    h.amount,
                    h.price_usd,
                    h.value_usd
                FROM ch_account_balance_snapshot h
                INNER JOIN latest_snapshot l ON h.snapshot_id = l.max_snapshot_id
                WHERE h.account_id = ?
                  AND h.value_usd > 0
                ORDER BY h.value_usd DESC
                """;
            
            List<AccountDetail.Asset> assets = clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                AccountDetail.Asset asset = new AccountDetail.Asset();
                
                asset.setTokenId(rs.getString("token_id"));
                asset.setSymbol(rs.getString("biz_name"));
                asset.setAssetType(rs.getString("asset_type"));
                
                BigDecimal amount = rs.getBigDecimal("amount");
                asset.setBalance(amount != null ? amount.toString() : "0");
                
                BigDecimal priceUsd = rs.getBigDecimal("price_usd");
                asset.setPriceUsd(priceUsd != null ? priceUsd.toString() : "0");
                
                BigDecimal valueUsd = rs.getBigDecimal("value_usd");
                asset.setValueUsd(valueUsd != null ? valueUsd.toString() : "0");
                
                // 获取symbol，这里先用tokenId，后续可以连接token表
                asset.setSymbol(rs.getString("biz_name"));
                
                return asset;
            }, accountId, accountId);
            
            return assets;
            
        } catch (Exception e) {
            log.error("获取资产持仓失败, accountId: {}", accountId, e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 获取转账历史
     */
    private List<AccountDetail.TransferHistory> getTransferHistoryById(Long accountId) {
        try {
            // 从ClickHouse获取交易历史
            String sql = """
                SELECT 
                    block_time,
                    block_id,
                    tx_hash,
                    side,
                    token_id,
                    qty,
                    value_usd
                FROM ch_account_trade_fact
                WHERE account_id = ?
                ORDER BY block_time DESC
                LIMIT 50
                """;
            
            return clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                AccountDetail.TransferHistory transfer = new AccountDetail.TransferHistory();
                
                transfer.setTimestamp(rs.getTimestamp("block_time").toString());
                transfer.setBlockNumber(rs.getLong("block_id"));
                transfer.setTxHash(rs.getString("tx_hash"));
                
                // 将side转换为direction
                String side = rs.getString("side");
                transfer.setDirection("buy".equals(side) ? "in" : "out");
                
                transfer.setTokenSymbol(generateSymbolFromTokenId(rs.getLong("token_id")));
                
                BigDecimal qty = rs.getBigDecimal("qty");
                transfer.setAmount(qty != null ? qty.toString() : "0");
                
                BigDecimal valueUsd = rs.getBigDecimal("value_usd");
                transfer.setValueUsd(valueUsd != null ? valueUsd.toString() : "0");
                
                return transfer;
            }, accountId);
            
        } catch (Exception e) {
            log.error("获取转账历史失败, accountId: {}", accountId, e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 计算资产统计
     */
    private AccountDetail.AssetStats calculateAssetStats(List<AccountDetail.Asset> assets) {
        AccountDetail.AssetStats stats = new AccountDetail.AssetStats();
        
        if (assets.isEmpty()) {
            stats.setTotalValueUsd("0");
            stats.setAssetCount(0);
            stats.setTopAssetSymbol("");
            stats.setTopAssetPercentage(0.0);
            return stats;
        }
        
        // 计算总价值
        BigDecimal totalValue = assets.stream()
            .map(asset -> new BigDecimal(asset.getValueUsd()))
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        
        stats.setTotalValueUsd(totalValue.toString());
        stats.setAssetCount(assets.size());
        
        // 找到价值最高的资产
        AccountDetail.Asset topAsset = assets.get(0); // 已经按价值排序
        stats.setTopAssetSymbol(topAsset.getSymbol());
        
        if (totalValue.compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal topAssetPercentage = new BigDecimal(topAsset.getValueUsd())
                .divide(totalValue, 4, RoundingMode.HALF_UP)
                .multiply(new BigDecimal("100"));
            stats.setTopAssetPercentage(topAssetPercentage.doubleValue());
        } else {
            stats.setTopAssetPercentage(0.0);
        }
        
        return stats;
    }
    
    /**
     * 计算转账统计
     */
    private AccountDetail.TransferStats calculateTransferStats(List<AccountDetail.TransferHistory> transfers) {
        AccountDetail.TransferStats stats = new AccountDetail.TransferStats();
        
        if (transfers.isEmpty()) {
            stats.setTotalTransfers(0);
            stats.setTransfersIn(0);
            stats.setTransfersOut(0);
            stats.setTotalVolumeIn("0");
            stats.setTotalVolumeOut("0");
            stats.setAvgTransactionValue("0");
            return stats;
        }
        
        stats.setTotalTransfers(transfers.size());
        
        long transfersIn = transfers.stream().mapToLong(t -> "in".equals(t.getDirection()) ? 1 : 0).sum();
        long transfersOut = transfers.stream().mapToLong(t -> "out".equals(t.getDirection()) ? 1 : 0).sum();
        
        stats.setTransfersIn((int) transfersIn);
        stats.setTransfersOut((int) transfersOut);
        
        BigDecimal totalVolumeIn = transfers.stream()
            .filter(t -> "in".equals(t.getDirection()))
            .map(t -> new BigDecimal(t.getValueUsd()))
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        
        BigDecimal totalVolumeOut = transfers.stream()
            .filter(t -> "out".equals(t.getDirection()))
            .map(t -> new BigDecimal(t.getValueUsd()))
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        
        stats.setTotalVolumeIn(totalVolumeIn.toString());
        stats.setTotalVolumeOut(totalVolumeOut.toString());
        
        BigDecimal totalVolume = totalVolumeIn.add(totalVolumeOut);
        if (transfers.size() > 0) {
            BigDecimal avgValue = totalVolume.divide(new BigDecimal(transfers.size()), 2, RoundingMode.HALF_UP);
            stats.setAvgTransactionValue(avgValue.toString());
        } else {
            stats.setAvgTransactionValue("0");
        }
        
        return stats;
    }
    
    /**
     * 根据位图生成标签列表
     */
    private List<String> generateLabelsFromBitmap(int tagBitmap) {
        List<String> labels = new ArrayList<>();
        
        if ((tagBitmap & 1) != 0) labels.add("fresh");
        if ((tagBitmap & 2) != 0) labels.add("whale");
        if ((tagBitmap & 4) != 0) labels.add("smart");
        if ((tagBitmap & 8) != 0) labels.add("cex");
        
        // 如果没有标签，默认给一个
        if (labels.isEmpty()) {
            labels.add("public");
        }
        
        return labels;
    }
    
    /**
     * 根据tokenId生成symbol（临时方法）
     */
    private String generateSymbolFromTokenId(Long tokenId) {
        // 这里可以后续优化为查询token表获取真实symbol
        String[] symbols = {"ETH", "USDT", "WBTC", "UNI", "LINK"};
        int index = (int) (tokenId % symbols.length);
        return symbols[index];
    }
}

package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 账户详情数据模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountDetail {
    
    /**
     * 账户基础信息
     */
    private AccountInfo accountInfo;
    
    /**
     * 标签信息
     */
    private LabelInfo labelInfo;
    
    /**
     * 资产持仓
     */
    private List<Asset> assets;
    
    /**
     * 转账历史
     */
    private List<TransferHistory> transferHistory;
    
    /**
     * 资产统计
     */
    private AssetStats assetStats;
    
    /**
     * 转账统计
     */
    private TransferStats transferStats;
    
    /**
     * 账户基础信息内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AccountInfo {
        private String address;
        private String entity;
        private String createdAt;
    }
    
    /**
     * 标签信息内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LabelInfo {
        private List<String> labels;
    }
    
    /**
     * 资产持仓内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Asset {
        private String tokenId;
        private String symbol;
        private String assetType;
        private String balance;
        private String priceUsd;
        private String valueUsd;
    }
    
    /**
     * 转账历史内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TransferHistory {
        private String timestamp;
        private Long blockNumber;
        private String txHash;
        private String direction;  // in/out
        private String tokenSymbol;
        private String amount;
        private String valueUsd;
    }
    
    /**
     * 资产统计内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AssetStats {
        private String totalValueUsd;
        private Integer assetCount;
        private String topAssetSymbol;
        private Double topAssetPercentage;
    }
    
    /**
     * 转账统计内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TransferStats {
        private Integer totalTransfers;
        private Integer transfersIn;
        private Integer transfersOut;
        private String totalVolumeIn;
        private String totalVolumeOut;
        private String avgTransactionValue;
    }
}

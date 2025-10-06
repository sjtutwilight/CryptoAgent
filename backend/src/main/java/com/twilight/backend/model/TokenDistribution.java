package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 代币分布数据模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TokenDistribution {
    
    private Long tokenId;
    private String timeRange;
    
    /**
     * 持有者统计
     */
    private HolderStats holderStats;
    
    /**
     * 标签分布
     */
    private List<TagDistribution> tagDistribution;
    
    /**
     * Top持币地址
     */
    private List<TopHolder> topHolders;
    
    /**
     * 持有者统计内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HolderStats {
        private Long holdersCount;
        private String totalValueUsd;
        private Double top2SharePercent;
        private Double concentrationIndex;
        private String concentrationLevel;
        private String avgHolderValueUsd;
        private String medianHolderValueUsd;
        private Double freshHolderSharePercent;
        private Concentration concentration;
    }
    
    /**
     * 集中度内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Concentration {
        private Double top2SharePercent;
        private Double giniCoefficient;
    }
    
    /**
     * 标签分布内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TagDistribution {
        private String tag;
        private String valueUsd;
        private Double sharePercent;
        private String change5min;
        private String changeAmount5min;
        private Long holdersCount;
        private String balance;
    }
    
    /**
     * Top持币地址内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopHolder {
        private Integer rank;
        private String accountId;
        private String address;
        private List<String> labels;
        private String balance;
        private String valueUsd;
        private Double ownershipPercent;
        private Integer firstSeenDays;
    }
}




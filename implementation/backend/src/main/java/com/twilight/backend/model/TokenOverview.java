package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * 代币概览数据模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TokenOverview {
    
    /**
     * 代币ID
     */
    private Long tokenId;
    
    /**
     * 时间范围
     */
    private String timeRange;
    
    /**
     * 基础信息
     */
    private BasicInfo basicInfo;
    
    /**
     * 宏观指标
     */
    private Metrics metrics;
    
    /**
     * 交易流分析
     */
    private TradeFlow tradeFlow;
    
    /**
     * 价格走势图表
     */
    private PriceChart priceChart;
    
    /**
     * Top买卖地址
     */
    private TopTraders topTraders;
    
    /**
     * 交易明细
     */
    private List<TradeDetail> recentTrades;

    private String windowTime;
    
    /**
     * 基础信息内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BasicInfo {
        private String symbol;
        private String name;
        private String chainName;
        private String tokenCategory;
        private String address;
        private Integer decimals;
        private Integer age;
        private Integer securityScore;
        private String issuer;
        private String description;
    }
    
    /**
     * 宏观指标内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Metrics {
        private String currentPrice;
        private String priceChangePercent;
        private String mcap;
        private String fdv;
        private String liquidity;
        private Double fdvMcapRatio;
        private Double mcapLiquidityRatio;
        private Double fdvLiquidityRatio;
    }
    
    /**
     * 交易流分析内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TradeFlow {
        /**
         * 交易量汇总统计
         */
        private Summary summary;
        
        /**
         * 标签净流入汇总
         */
        private Map<String, String> netFlowSummary;
        
        /**
         * 标签详细数据
         */
        private List<TagFlowDetail> tagFlowDetails;
    }
    
    /**
     * 交易汇总统计内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Summary {
        private String totalVolume;
        private String totalBuyVolume;
        private String totalSellVolume;
        private Integer totalTxCount;
        private Integer buyTxCount;
        private Integer sellTxCount;
        private Double buyPressure;
    }
    
    /**
     * 标签流向详情内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TagFlowDetail {
        private String tag;
        private String netFlowUsd;
        private String buyVolumeUsd;
        private String sellVolumeUsd;
        private Integer txCount;
        private Integer buyTxCount;
        private Integer sellTxCount;
        private String avgTxSize;
        private String timeWindow;
    }
    
    /**
     * 价格走势图表内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PriceChart {
        private String interval;
        private Integer dataPoints;
        private List<PriceData> priceData;
        private String currentPrice;
        private String priceChangePercent;
        private String change;
        private String highestPrice;
        private String lowestPrice;
        private String timeRange;
    }
    
    /**
     * 价格数据点内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PriceData {
        private String timestamp;
        private String price;
        private String volume;
    }
    
    /**
     * Top买卖地址内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopTraders {
        private List<TopTrader> topBuyers;
        private List<TopTrader> topSellers;
    }
    
    /**
     * Top交易者内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopTrader {
        private Integer rank;
        private String address;
        private List<String> labels;
        private String totalBuyVolume;
        private String totalSellVolume;
        private Integer buyTxCount;
        private Integer sellTxCount;
        private String avgBuySize;
        private String avgSellSize;
        private String lastBuyTime;
        private String lastSellTime;
        private Double profitability;
        private String reason;
        private Long accountId;
    }
    
    /**
     * 交易明细内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TradeDetail {
        private String timestamp;
        private String address;
        private List<String> labels;
        private String action; // "buy" or "sell"
        private String amount;
        private String value;
        private String price;
        private String txHash;
        private Long accountId;
    }
}

package com.twilight.backend.model;

import java.util.List;

/**
 * Token PnL分析数据模型
 */
public class TokenPnL {
    
    private Long tokenId;
    private String timeRange;
    
    // Top PnL排行榜
    private List<TopPnLItem> topPnL;
    
    // 宏观PnL数据  
    private MacroPnL macroPnL;
    
    // 核心指标
    private Indicators indicators;
    
    // 汇总统计
    private Summary summary;
    
    // 分页信息
    private Integer topLimit;
    
    // 构造器
    public TokenPnL() {}
    
    // Getters and Setters
    public Long getTokenId() { return tokenId; }
    public void setTokenId(Long tokenId) { this.tokenId = tokenId; }
    
    public String getTimeRange() { return timeRange; }
    public void setTimeRange(String timeRange) { this.timeRange = timeRange; }
    
    public List<TopPnLItem> getTopPnL() { return topPnL; }
    public void setTopPnL(List<TopPnLItem> topPnL) { this.topPnL = topPnL; }
    
    public MacroPnL getMacroPnL() { return macroPnL; }
    public void setMacroPnL(MacroPnL macroPnL) { this.macroPnL = macroPnL; }
    
    public Indicators getIndicators() { return indicators; }
    public void setIndicators(Indicators indicators) { this.indicators = indicators; }
    
    public Summary getSummary() { return summary; }
    public void setSummary(Summary summary) { this.summary = summary; }
    
    public Integer getTopLimit() { return topLimit; }
    public void setTopLimit(Integer topLimit) { this.topLimit = topLimit; }
    
    /**
     * Top PnL排行榜项目
     */
    public static class TopPnLItem {
        private Long accountId;
        private String address;
        private List<String> labels;
        private String totalPnlUsd;
        private String realizedPnlUsd;
        private String unrealizedPnlUsd;
        private Double totalRoi;
        private Double stillHoldingPercent;
        
        // 构造器
        public TopPnLItem() {}
        
        // Getters and Setters
        public Long getAccountId() { return accountId; }
        public void setAccountId(Long accountId) { this.accountId = accountId; }
        
        public String getAddress() { return address; }
        public void setAddress(String address) { this.address = address; }
        
        public List<String> getLabels() { return labels; }
        public void setLabels(List<String> labels) { this.labels = labels; }
        
        public String getTotalPnlUsd() { return totalPnlUsd; }
        public void setTotalPnlUsd(String totalPnlUsd) { this.totalPnlUsd = totalPnlUsd; }
        
        public String getRealizedPnlUsd() { return realizedPnlUsd; }
        public void setRealizedPnlUsd(String realizedPnlUsd) { this.realizedPnlUsd = realizedPnlUsd; }
        
        public String getUnrealizedPnlUsd() { return unrealizedPnlUsd; }
        public void setUnrealizedPnlUsd(String unrealizedPnlUsd) { this.unrealizedPnlUsd = unrealizedPnlUsd; }
        
        public Double getTotalRoi() { return totalRoi; }
        public void setTotalRoi(Double totalRoi) { this.totalRoi = totalRoi; }
        
        public Double getStillHoldingPercent() { return stillHoldingPercent; }
        public void setStillHoldingPercent(Double stillHoldingPercent) { this.stillHoldingPercent = stillHoldingPercent; }
    }
    
    /**
     * 宏观PnL数据
     */
    public static class MacroPnL {
        private String lastUpdated;
        
        public MacroPnL() {}
        
        public String getLastUpdated() { return lastUpdated; }
        public void setLastUpdated(String lastUpdated) { this.lastUpdated = lastUpdated; }
    }
    
    /**
     * 核心指标
     */
    public static class Indicators {
        private IndicatorValue NUPL;
        private IndicatorValue MVRV;
        private IndicatorValue SOPR;
        
        public Indicators() {}
        
        public IndicatorValue getNUPL() { return NUPL; }
        public void setNUPL(IndicatorValue NUPL) { this.NUPL = NUPL; }
        
        public IndicatorValue getMVRV() { return MVRV; }
        public void setMVRV(IndicatorValue MVRV) { this.MVRV = MVRV; }
        
        public IndicatorValue getSOPR() { return SOPR; }
        public void setSOPR(IndicatorValue SOPR) { this.SOPR = SOPR; }
    }
    
    /**
     * 指标值对象
     */
    public static class IndicatorValue {
        private Double value;
        private String description;
        private String interpretation;
        
        public IndicatorValue() {}
        
        public IndicatorValue(Double value, String description, String interpretation) {
            this.value = value;
            this.description = description;
            this.interpretation = interpretation;
        }
        
        public Double getValue() { return value; }
        public void setValue(Double value) { this.value = value; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        
        public String getInterpretation() { return interpretation; }
        public void setInterpretation(String interpretation) { this.interpretation = interpretation; }
    }
    
    /**
     * 汇总统计
     */
    public static class Summary {
        private String totalPnL;
        private String totalRealizedPnL;
        private String totalUnrealizedPnL;
        private Double profitablePercentage;
        private Integer profitableCount;
        private Double avgStillHoldingPercent;
        
        public Summary() {}
        
        public String getTotalPnL() { return totalPnL; }
        public void setTotalPnL(String totalPnL) { this.totalPnL = totalPnL; }
        
        public String getTotalRealizedPnL() { return totalRealizedPnL; }
        public void setTotalRealizedPnL(String totalRealizedPnL) { this.totalRealizedPnL = totalRealizedPnL; }
        
        public String getTotalUnrealizedPnL() { return totalUnrealizedPnL; }
        public void setTotalUnrealizedPnL(String totalUnrealizedPnL) { this.totalUnrealizedPnL = totalUnrealizedPnL; }
        
        public Double getProfitablePercentage() { return profitablePercentage; }
        public void setProfitablePercentage(Double profitablePercentage) { this.profitablePercentage = profitablePercentage; }
        
        public Integer getProfitableCount() { return profitableCount; }
        public void setProfitableCount(Integer profitableCount) { this.profitableCount = profitableCount; }
        
        public Double getAvgStillHoldingPercent() { return avgStillHoldingPercent; }
        public void setAvgStillHoldingPercent(Double avgStillHoldingPercent) { this.avgStillHoldingPercent = avgStillHoldingPercent; }
    }
}












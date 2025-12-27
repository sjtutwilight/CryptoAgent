package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 永续合约汇合面板指标（1分钟级）
 * 
 * 数据来源：
 * - 执行面聚合：从1秒ExecutionMetrics rollup到1分钟
 * - 语境面：从1分钟ContextMetrics直接join
 * 
 * 衍生指标：
 * - liquidity_regime：流动性制度分类（THICK/NORMAL/THIN）
 * - crowding_score：拥挤度得分（基于funding/basis/oi_delta的Z-score）
 * 
 * 输出目标：
 * - Kafka: perp.panel.1m（可选，初版可不做）
 * - ClickHouse: dws_perps_panel_1m
 * - 信号检测：TrendSignalDetector（拥挤度/清算风险）
 */
public class PanelMetrics implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // ===== 基础标识 =====
    private String symbol;           // 交易对符号（BTCUSDT）
    private String exchange;         // 交易所（binance, hyperliquid）
    private Long endTime;            // 分钟级窗口结束时间（Unix毫秒）
    
    // ===== 执行面聚合（从1s rollup） =====
    private Double avgSpreadBps;     // 平均点差（基点）
    private Double maxSpreadBps;     // 最大点差（基点）
    private BigDecimal avgDepth50k;  // 平均±50k深度（USD）
    private Double avgImpact50kBps;  // 平均50k冲击成本（基点）
    private Double avgImbalance;     // 平均不平衡
    private Double sumOfi;           // OFI总和
    private BigDecimal volumeUsd;    // 成交量（USD）
    private Integer tradeCount;      // 成交笔数
    
    // ===== 语境面（直接join） =====
    private BigDecimal markPrice;    // 标记价格
    private Double basisBps;         // 基差（基点）
    private BigDecimal fundingRate;  // 当前资金费率
    private BigDecimal fundingEma24h;// 24h资金费率EMA
    private BigDecimal oiUsd;        // 持仓量（USD）
    private BigDecimal oiDelta1m;    // 1分钟OI变化
    
    // ===== 衍生指标 =====
    private String liquidityRegime;  // 流动性制度：THICK/NORMAL/THIN
    private Double crowdingScore;    // 拥挤度得分（Z-score组合）
    
    // ===== 构造函数 =====
    public PanelMetrics() {}
    
    // ===== Getters and Setters =====
    
    public String getSymbol() {
        return symbol;
    }
    
    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }
    
    public String getExchange() {
        return exchange;
    }
    
    public void setExchange(String exchange) {
        this.exchange = exchange;
    }
    
    public Long getEndTime() {
        return endTime;
    }
    
    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }
    
    public Double getAvgSpreadBps() {
        return avgSpreadBps;
    }
    
    public void setAvgSpreadBps(Double avgSpreadBps) {
        this.avgSpreadBps = avgSpreadBps;
    }
    
    public Double getMaxSpreadBps() {
        return maxSpreadBps;
    }
    
    public void setMaxSpreadBps(Double maxSpreadBps) {
        this.maxSpreadBps = maxSpreadBps;
    }
    
    public BigDecimal getAvgDepth50k() {
        return avgDepth50k;
    }
    
    public void setAvgDepth50k(BigDecimal avgDepth50k) {
        this.avgDepth50k = avgDepth50k;
    }
    
    public Double getAvgImpact50kBps() {
        return avgImpact50kBps;
    }
    
    public void setAvgImpact50kBps(Double avgImpact50kBps) {
        this.avgImpact50kBps = avgImpact50kBps;
    }
    
    public Double getAvgImbalance() {
        return avgImbalance;
    }
    
    public void setAvgImbalance(Double avgImbalance) {
        this.avgImbalance = avgImbalance;
    }
    
    public Double getSumOfi() {
        return sumOfi;
    }
    
    public void setSumOfi(Double sumOfi) {
        this.sumOfi = sumOfi;
    }
    
    public BigDecimal getVolumeUsd() {
        return volumeUsd;
    }
    
    public void setVolumeUsd(BigDecimal volumeUsd) {
        this.volumeUsd = volumeUsd;
    }
    
    public Integer getTradeCount() {
        return tradeCount;
    }
    
    public void setTradeCount(Integer tradeCount) {
        this.tradeCount = tradeCount;
    }
    
    public BigDecimal getMarkPrice() {
        return markPrice;
    }
    
    public void setMarkPrice(BigDecimal markPrice) {
        this.markPrice = markPrice;
    }
    
    public Double getBasisBps() {
        return basisBps;
    }
    
    public void setBasisBps(Double basisBps) {
        this.basisBps = basisBps;
    }
    
    public BigDecimal getFundingRate() {
        return fundingRate;
    }
    
    public void setFundingRate(BigDecimal fundingRate) {
        this.fundingRate = fundingRate;
    }
    
    public BigDecimal getFundingEma24h() {
        return fundingEma24h;
    }
    
    public void setFundingEma24h(BigDecimal fundingEma24h) {
        this.fundingEma24h = fundingEma24h;
    }
    
    public BigDecimal getOiUsd() {
        return oiUsd;
    }
    
    public void setOiUsd(BigDecimal oiUsd) {
        this.oiUsd = oiUsd;
    }
    
    public BigDecimal getOiDelta1m() {
        return oiDelta1m;
    }
    
    public void setOiDelta1m(BigDecimal oiDelta1m) {
        this.oiDelta1m = oiDelta1m;
    }
    
    public String getLiquidityRegime() {
        return liquidityRegime;
    }
    
    public void setLiquidityRegime(String liquidityRegime) {
        this.liquidityRegime = liquidityRegime;
    }
    
    public Double getCrowdingScore() {
        return crowdingScore;
    }
    
    public void setCrowdingScore(Double crowdingScore) {
        this.crowdingScore = crowdingScore;
    }
    
    @Override
    public String toString() {
        return "PanelMetrics{" +
                "symbol='" + symbol + '\'' +
                ", exchange='" + exchange + '\'' +
                ", endTime=" + endTime +
                ", avgSpreadBps=" + avgSpreadBps +
                ", maxSpreadBps=" + maxSpreadBps +
                ", avgDepth50k=" + avgDepth50k +
                ", liquidityRegime='" + liquidityRegime + '\'' +
                ", crowdingScore=" + crowdingScore +
                ", volumeUsd=" + volumeUsd +
                ", markPrice=" + markPrice +
                ", fundingRate=" + fundingRate +
                ", oiDelta1m=" + oiDelta1m +
                '}';
    }
}







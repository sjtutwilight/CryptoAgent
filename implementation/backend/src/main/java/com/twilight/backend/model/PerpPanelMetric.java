package com.twilight.backend.model;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 永续合约汇合面板指标（分钟级）
 */
@Data
public class PerpPanelMetric {
    private String symbol;
    private String exchange;
    private LocalDateTime endTime;
    private String algoVersion;

    private Double avgSpreadBps;
    private Double maxSpreadBps;
    private BigDecimal avgDepth50k;
    private Double avgImpact50kBps;
    private Double avgImbalance;
    private Double sumOfi;
    private BigDecimal volumeUsd;
    private Long tradeCount;

    private BigDecimal markPrice;
    private Double basisBps;
    private BigDecimal fundingRate;
    private BigDecimal fundingEma24h;
    private BigDecimal oiUsd;
    private BigDecimal oiDelta1m;

    private String liquidityRegime;
    private Double crowdingScore;

    private LocalDateTime processTime;
}

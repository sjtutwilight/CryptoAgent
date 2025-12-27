package com.twilight.backend.model;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 永续合约语境面分钟级指标
 */
@Data
public class PerpContextMetric {
    private String symbol;
    private String exchange;
    private LocalDateTime endTime;
    private String algoVersion;

    private BigDecimal markPrice;
    private BigDecimal indexPrice;
    private Double basisBps;

    private BigDecimal fundingRate;
    private BigDecimal fundingRate8h;
    private BigDecimal fundingEma24h;
    private LocalDateTime nextFundingTime;

    private BigDecimal oi;
    private BigDecimal oiUsd;
    private BigDecimal oiDelta1m;
    private Double oiDeltaPct;
    private Boolean oiCarried;

    private LocalDateTime processTime;
}

package com.twilight.backend.model;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 标准K线指标记录
 */
@Data
public class KlineMetric {

    private String exchange;
    private String symbol;
    private String interval;

    private LocalDateTime startTime;
    private LocalDateTime closeTime;
    private LocalDateTime eventTime;
    private Boolean closed;
    private LocalDateTime ingestTime;

    private BigDecimal openPrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal closePrice;

    private BigDecimal baseVolume;
    private BigDecimal quoteVolume;
    private Long tradeCount;

    private BigDecimal amplitudePct;
    private BigDecimal changePct;

    private Integer maShortPeriod;
    private BigDecimal maShortValue;
    private Integer maMediumPeriod;
    private BigDecimal maMediumValue;
    private Integer maLongPeriod;
    private BigDecimal maLongValue;

    private BigDecimal emaShortValue;
    private BigDecimal emaLongValue;

    private String signalType;
    private BigDecimal signalStrength;
    private String signalDetail;
    private LocalDateTime signalTimestamp;

    private LocalDateTime processTime;
    private LocalDateTime createTime;
}

package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * K线指标快照
 * <p>
 * 该模型用于承载每一根K线在策略计算后的指标结果，后续会落地到ClickHouse供实时分析使用。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KlineMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    private String exchange;
    private String symbol;
    private String interval;

    private Long eventTime;
    private Long startTime;
    private Long closeTime;
    private Boolean closed;
    private Long ingestTime;

    private BigDecimal openPrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal closePrice;

    private BigDecimal baseVolume;
    private BigDecimal quoteVolume;
    private Integer tradeCount;

    private BigDecimal amplitudePercent;
    private BigDecimal changePercent;

    private Integer shortPeriod;
    private Integer mediumPeriod;
    private Integer longPeriod;

    private BigDecimal shortMa;
    private BigDecimal mediumMa;
    private BigDecimal longMa;

    private KlineSignal.SignalType signalType;
    private BigDecimal signalStrength;
    private String signalDetail;
    private Long signalTimestamp;
}

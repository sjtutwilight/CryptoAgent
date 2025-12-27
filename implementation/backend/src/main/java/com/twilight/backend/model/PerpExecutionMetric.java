package com.twilight.backend.model;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 永续合约执行面秒级指标
 */
@Data
public class PerpExecutionMetric {
    private String symbol;
    private String exchange;
    private LocalDateTime endTime;
    private String algoVersion;

    private BigDecimal midPrice;
    private Double spreadBps;
    private BigDecimal spreadAbs;

    private BigDecimal depth10k;
    private BigDecimal depth50k;
    private BigDecimal depth100k;

    private Double imbalanceTop5;
    private Double imbalanceTotal;

    private Double impact10kBps;
    private Double impact50kBps;
    private Double impact100kBps;

    private Double ofi;

    private Long tradeCount;
    private BigDecimal volumeUsd;
    private BigDecimal vwap;
    private BigDecimal buyVolumeUsd;
    private BigDecimal sellVolumeUsd;

    private Double illiqLambda;

    private LocalDateTime processTime;
}

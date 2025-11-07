package com.twilight.backend.model;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 永续合约异常信号
 */
@Data
public class PerpSignal {
    private String symbol;
    private String exchange;
    private LocalDateTime signalTime;
    private String signalType;
    private String signalLevel;

    private String metricName;
    private Double metricValue;
    private Double threshold;
    private String contextJson;

    private String algoVersion;
    private LocalDateTime processTime;
}

package com.twilight.backend.model;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * 指标类K线信号记录，如RSI、MACD
 */
@Data
public class KlineIndicatorMetric {

    private String exchange;
    private String symbol;
    private String interval;

    private LocalDateTime startTime;
    private LocalDateTime endTime;

    private String indicator;
    private String variant;
    private Double value;

    private List<KlineIndicatorComponent> components;
    private List<KlineIndicatorThreshold> thresholds;

    private String signalType;
    private Double signalStrength;
    private String signalDetail;

    private Map<String, String> extraTags;

    private LocalDateTime processTime;
    private LocalDateTime createTime;
}

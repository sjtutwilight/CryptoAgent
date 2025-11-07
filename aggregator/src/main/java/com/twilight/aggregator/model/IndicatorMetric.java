package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 通用技术指标快照
 * <p>
 * 每个指标处理器在计算完成后会生成一条指标记录，通过侧输出流写入ClickHouse。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IndicatorMetric implements Serializable {
    private static final long serialVersionUID = 1L;

    private String exchange;
    private String symbol;
    private String interval;

    private Long eventTime;
    private Long startTime;
    private Long endTime;
    private Long ingestTime;

    private String indicator; // 指标名称，如 RSI、MACD
    private String variant;   // 参数组合标识，如 period=14

    private BigDecimal value; // 主指标值
    private Map<String, BigDecimal> components;  // 其它组成部分（如MACD柱、上轨/下轨）
    private Map<String, BigDecimal> thresholds;  // 阈值信息（如超买/超卖等）

    private KlineSignal.SignalType signalType;
    private BigDecimal signalStrength;
    private String signalDetail;
    private Long signalTimestamp;

    private Long processTime;
}

package com.twilight.aggregator.process.kline;

import org.apache.flink.util.OutputTag;

import com.twilight.aggregator.model.IndicatorMetric;
import com.twilight.aggregator.model.KlineMetrics;

/**
 * 统一管理K线处理侧输出标签，便于各个处理器复用。
 */
public final class IndicatorOutputTags {
    private IndicatorOutputTags() {
    }

    public static final OutputTag<KlineMetrics> KLINE_METRICS_TAG =
            new OutputTag<KlineMetrics>("kline-metrics") {};

    public static final OutputTag<IndicatorMetric> INDICATOR_METRICS_TAG =
            new OutputTag<IndicatorMetric>("indicator-metrics") {};
}

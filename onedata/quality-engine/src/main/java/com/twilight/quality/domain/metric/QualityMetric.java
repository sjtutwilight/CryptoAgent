package com.twilight.quality.domain.metric;

import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

/**
 * 质量指标数据模型
 * 用于存储到ClickHouse的时序指标
 */
@Data
@Builder
public class QualityMetric {
    
    /**
     * 指标ID（UUID）
     */
    @Builder.Default
    private String metricId = UUID.randomUUID().toString();
    
    /**
     * 业务域
     */
    private DataDomain domain;
    
    /**
     * 流标识（如symbol、chain_id等）
     */
    private String streamKey;
    
    /**
     * 质量维度
     */
    private QualityDimension dimension;
    
    /**
     * 规则名称
     */
    private String ruleName;
    
    /**
     * 指标值
     */
    private Double value;
    
    /**
     * 阈值
     */
    private Double threshold;
    
    /**
     * 是否通过检测
     */
    private Boolean passed;
    
    /**
     * 窗口开始时间（毫秒）
     */
    private Long windowStart;
    
    /**
     * 窗口结束时间（毫秒）
     */
    private Long windowEnd;
    
    /**
     * 消息数量
     */
    private Long messageCount;
    
    /**
     * 采集时间
     */
    @Builder.Default
    private Instant collectedAt = Instant.now();
    
    /**
     * 从RuleResult创建QualityMetric
     */
    public static QualityMetric fromRuleResult(com.twilight.quality.domain.rule.RuleResult result) {
        return QualityMetric.builder()
                .domain(result.getDomain())
                .streamKey(result.getStreamKey())
                .dimension(result.getDimension())
                .ruleName(result.getRuleName())
                .value(result.getMetricValue())
                .threshold(result.getThreshold())
                .passed(result.isPassed())
                .windowStart(result.getWindowStart())
                .windowEnd(result.getWindowEnd())
                .messageCount(result.getMessageCount())
                .collectedAt(result.getEvaluatedAt() != null ? result.getEvaluatedAt() : Instant.now())
                .build();
    }
}


package com.twilight.quality.domain.rule;

import com.twilight.quality.domain.enums.AlertLevel;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

/**
 * 规则检测结果
 */
@Data
@Builder
public class RuleResult {
    
    /**
     * 规则名称
     */
    private String ruleName;
    
    /**
     * 业务域
     */
    private DataDomain domain;
    
    /**
     * 质量维度
     */
    private QualityDimension dimension;
    
    /**
     * 流标识（如symbol、chain_id等）
     */
    private String streamKey;
    
    /**
     * 是否通过检测
     */
    private boolean passed;
    
    /**
     * 告警级别（仅当passed=false时有意义）
     */
    private AlertLevel alertLevel;
    
    /**
     * 指标值
     */
    private Double metricValue;
    
    /**
     * 阈值
     */
    private Double threshold;
    
    /**
     * 检测消息/描述
     */
    private String message;
    
    /**
     * 上下文信息（用于告警详情）
     */
    private Map<String, Object> context;
    
    /**
     * 检测时间
     */
    private Instant evaluatedAt;
    
    /**
     * 窗口开始时间（聚合规则）
     */
    private Long windowStart;
    
    /**
     * 窗口结束时间（聚合规则）
     */
    private Long windowEnd;
    
    /**
     * 消息数量（聚合规则）
     */
    private Long messageCount;
    
    /**
     * 创建通过结果的便捷方法
     */
    public static RuleResult pass(String ruleName, DataDomain domain, 
                                   QualityDimension dimension, String streamKey) {
        return RuleResult.builder()
                .ruleName(ruleName)
                .domain(domain)
                .dimension(dimension)
                .streamKey(streamKey)
                .passed(true)
                .evaluatedAt(Instant.now())
                .build();
    }
    
    /**
     * 创建失败结果的便捷方法
     */
    public static RuleResult fail(String ruleName, DataDomain domain,
                                   QualityDimension dimension, String streamKey,
                                   AlertLevel level, String message) {
        return RuleResult.builder()
                .ruleName(ruleName)
                .domain(domain)
                .dimension(dimension)
                .streamKey(streamKey)
                .passed(false)
                .alertLevel(level)
                .message(message)
                .evaluatedAt(Instant.now())
                .build();
    }
}


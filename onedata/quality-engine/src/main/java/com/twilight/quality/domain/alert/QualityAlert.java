package com.twilight.quality.domain.alert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.quality.domain.enums.AlertLevel;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import com.twilight.quality.domain.rule.RuleResult;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * 质量告警事件模型
 */
@Data
@Builder
public class QualityAlert {
    
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    /**
     * 告警ID
     */
    @Builder.Default
    private String alertId = UUID.randomUUID().toString();
    
    /**
     * 告警级别
     */
    private AlertLevel level;
    
    /**
     * 业务域
     */
    private DataDomain domain;
    
    /**
     * 流标识
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
     * 告警消息
     */
    private String message;
    
    /**
     * 指标值
     */
    private Double metricValue;
    
    /**
     * 阈值
     */
    private Double threshold;
    
    /**
     * 上下文JSON
     */
    private String contextJson;
    
    /**
     * 告警时间（毫秒）
     */
    @Builder.Default
    private Long alertTime = System.currentTimeMillis();
    
    /**
     * 处理时间（毫秒）
     */
    @Builder.Default
    private Long processTime = System.currentTimeMillis();
    
    /**
     * 从RuleResult创建告警
     */
    public static QualityAlert fromRuleResult(RuleResult result) {
        String contextJson = null;
        if (result.getContext() != null && !result.getContext().isEmpty()) {
            try {
                contextJson = OBJECT_MAPPER.writeValueAsString(result.getContext());
            } catch (JsonProcessingException e) {
                contextJson = "{}";
            }
        }
        
        return QualityAlert.builder()
                .level(result.getAlertLevel())
                .domain(result.getDomain())
                .streamKey(result.getStreamKey())
                .dimension(result.getDimension())
                .ruleName(result.getRuleName())
                .message(result.getMessage())
                .metricValue(result.getMetricValue())
                .threshold(result.getThreshold())
                .contextJson(contextJson)
                .alertTime(result.getEvaluatedAt() != null 
                        ? result.getEvaluatedAt().toEpochMilli() 
                        : System.currentTimeMillis())
                .build();
    }
    
    /**
     * 转换为JSON字符串
     */
    public String toJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}


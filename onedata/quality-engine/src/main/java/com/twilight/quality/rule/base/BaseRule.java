package com.twilight.quality.rule.base;

import com.fasterxml.jackson.databind.JsonNode;
import com.twilight.quality.domain.enums.AlertLevel;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 规则基类
 * 提供通用的规则实现逻辑
 */
public abstract class BaseRule implements QualityRule<JsonNode> {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    protected final String ruleName;
    protected final QualityDimension dimension;
    protected final Set<DataDomain> supportedDomains;
    protected AlertLevel defaultAlertLevel = AlertLevel.WARNING;
    protected boolean enabled = true;
    protected Map<String, Object> config = new HashMap<>();
    
    protected BaseRule(String ruleName, QualityDimension dimension, DataDomain... domains) {
        this.ruleName = ruleName;
        this.dimension = dimension;
        this.supportedDomains = new HashSet<>(Arrays.asList(domains));
    }
    
    @Override
    public String getRuleName() {
        return ruleName;
    }
    
    @Override
    public QualityDimension getDimension() {
        return dimension;
    }
    
    @Override
    public Set<DataDomain> getSupportedDomains() {
        return supportedDomains;
    }
    
    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    @Override
    public void configure(Map<String, Object> config) {
        if (config != null) {
            this.config.putAll(config);
            
            // 解析通用配置
            if (config.containsKey("enabled")) {
                this.enabled = Boolean.parseBoolean(config.get("enabled").toString());
            }
            if (config.containsKey("alert_level")) {
                this.defaultAlertLevel = AlertLevel.fromString(config.get("alert_level").toString());
            }
            
            // 调用子类的配置解析
            parseConfig(config);
        }
    }
    
    /**
     * 子类实现的配置解析方法
     */
    protected void parseConfig(Map<String, Object> config) {
        // 子类可覆盖
    }
    
    /**
     * 创建通过结果
     */
    protected RuleResult pass(RuleContext context) {
        return RuleResult.pass(ruleName, context.getDomain(), dimension, context.getStreamKey());
    }
    
    /**
     * 创建失败结果
     */
    protected RuleResult fail(RuleContext context, String message) {
        return RuleResult.fail(ruleName, context.getDomain(), dimension, 
                context.getStreamKey(), defaultAlertLevel, message);
    }
    
    /**
     * 创建失败结果（带指标值）
     */
    protected RuleResult fail(RuleContext context, String message, 
                              Double metricValue, Double threshold) {
        return RuleResult.builder()
                .ruleName(ruleName)
                .domain(context.getDomain())
                .dimension(dimension)
                .streamKey(context.getStreamKey())
                .passed(false)
                .alertLevel(defaultAlertLevel)
                .message(message)
                .metricValue(metricValue)
                .threshold(threshold)
                .evaluatedAt(java.time.Instant.now())
                .build();
    }
    
    /**
     * 创建失败结果（带上下文）
     */
    protected RuleResult fail(RuleContext context, String message, 
                              Double metricValue, Double threshold,
                              Map<String, Object> ctx) {
        return RuleResult.builder()
                .ruleName(ruleName)
                .domain(context.getDomain())
                .dimension(dimension)
                .streamKey(context.getStreamKey())
                .passed(false)
                .alertLevel(defaultAlertLevel)
                .message(message)
                .metricValue(metricValue)
                .threshold(threshold)
                .context(ctx)
                .evaluatedAt(java.time.Instant.now())
                .build();
    }
    
    /**
     * 安全获取JsonNode的文本值
     */
    protected String getText(JsonNode node, String field) {
        JsonNode child = node.get(field);
        return child != null && !child.isNull() ? child.asText() : null;
    }
    
    /**
     * 安全获取JsonNode的长整型值
     */
    protected Long getLong(JsonNode node, String field) {
        JsonNode child = node.get(field);
        return child != null && !child.isNull() && child.isNumber() ? child.asLong() : null;
    }
    
    /**
     * 安全获取JsonNode的双精度值
     */
    protected Double getDouble(JsonNode node, String field) {
        JsonNode child = node.get(field);
        return child != null && !child.isNull() && child.isNumber() ? child.asDouble() : null;
    }
    
    /**
     * 检查字段是否存在且非空
     */
    protected boolean hasField(JsonNode node, String field) {
        JsonNode child = node.get(field);
        return child != null && !child.isNull();
    }
    
    /**
     * 获取配置值
     */
    @SuppressWarnings("unchecked")
    protected <T> T getConfig(String key, T defaultValue) {
        Object value = config.get(key);
        return value != null ? (T) value : defaultValue;
    }
}


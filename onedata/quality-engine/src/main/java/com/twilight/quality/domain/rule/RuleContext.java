package com.twilight.quality.domain.rule;

import com.twilight.quality.domain.enums.DataDomain;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 规则执行上下文
 * 包含规则执行所需的环境信息
 */
@Data
@Builder
public class RuleContext {
    
    /**
     * 业务域
     */
    private DataDomain domain;
    
    /**
     * 流标识
     */
    private String streamKey;
    
    /**
     * 消息接收时间
     */
    private Instant receiveTime;
    
    /**
     * 消息事件时间（从消息中提取）
     */
    private Long eventTime;
    
    /**
     * 原始消息JSON
     */
    private String rawMessage;
    
    /**
     * 扩展属性
     */
    @Builder.Default
    private Map<String, Object> attributes = new HashMap<>();
    
    /**
     * 添加属性
     */
    public RuleContext addAttribute(String key, Object value) {
        this.attributes.put(key, value);
        return this;
    }
    
    /**
     * 获取属性
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {
        return (T) this.attributes.get(key);
    }
    
    /**
     * 获取属性，带默认值
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key, T defaultValue) {
        Object value = this.attributes.get(key);
        return value != null ? (T) value : defaultValue;
    }
}


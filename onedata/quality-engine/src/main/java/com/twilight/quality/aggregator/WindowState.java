package com.twilight.quality.aggregator;

import com.twilight.quality.domain.enums.DataDomain;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 窗口状态
 * 用于聚合规则的窗口数据累积
 */
@Data
public class WindowState {
    
    /**
     * 业务域
     */
    private DataDomain domain;
    
    /**
     * 流标识
     */
    private String streamKey;
    
    /**
     * 规则名称
     */
    private String ruleName;
    
    /**
     * 窗口开始时间（毫秒）
     */
    private Long windowStart;
    
    /**
     * 窗口结束时间（毫秒）
     */
    private Long windowEnd;
    
    /**
     * 消息计数
     */
    private AtomicLong messageCount = new AtomicLong(0);
    
    /**
     * 最后消息时间
     */
    private Long lastMessageTime;
    
    /**
     * 最大延迟（毫秒）
     */
    private Long maxDelayMs;
    
    /**
     * 总延迟（用于计算平均延迟）
     */
    private AtomicLong totalDelayMs = new AtomicLong(0);
    
    /**
     * 扩展状态（规则特定数据）
     */
    private Map<String, Object> attributes = new HashMap<>();
    
    public WindowState(DataDomain domain, String streamKey, String ruleName, 
                       Long windowStart, Long windowEnd) {
        this.domain = domain;
        this.streamKey = streamKey;
        this.ruleName = ruleName;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }
    
    /**
     * 增加消息计数
     */
    public long incrementCount() {
        return messageCount.incrementAndGet();
    }
    
    /**
     * 获取消息数量
     */
    public long getMessageCount() {
        return messageCount.get();
    }
    
    /**
     * 更新延迟统计
     */
    public void updateDelay(long delayMs) {
        totalDelayMs.addAndGet(delayMs);
        if (maxDelayMs == null || delayMs > maxDelayMs) {
            maxDelayMs = delayMs;
        }
    }
    
    /**
     * 获取平均延迟
     */
    public double getAvgDelayMs() {
        long count = messageCount.get();
        return count > 0 ? (double) totalDelayMs.get() / count : 0;
    }
    
    /**
     * 设置属性
     */
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }
    
    /**
     * 获取属性
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {
        return (T) attributes.get(key);
    }
    
    /**
     * 获取属性，带默认值
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key, T defaultValue) {
        Object value = attributes.get(key);
        return value != null ? (T) value : defaultValue;
    }
    
    /**
     * 原子增加属性值（Long类型）
     */
    public long incrementAttribute(String key, long delta) {
        Long current = getAttribute(key, 0L);
        long newValue = current + delta;
        setAttribute(key, newValue);
        return newValue;
    }
    
    /**
     * 原子增加属性值（Double类型）
     */
    public double incrementAttribute(String key, double delta) {
        Double current = getAttribute(key, 0.0);
        double newValue = current + delta;
        setAttribute(key, newValue);
        return newValue;
    }
    
    /**
     * 生成窗口状态的唯一Key
     */
    public String getWindowKey() {
        return String.format("%s:%s:%s:%d", domain.getDomainId(), streamKey, ruleName, windowStart);
    }
    
    /**
     * 判断窗口是否已过期
     */
    public boolean isExpired(long currentTimeMs) {
        return currentTimeMs > windowEnd;
    }
}


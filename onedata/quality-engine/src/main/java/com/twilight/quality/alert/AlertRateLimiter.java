package com.twilight.quality.alert;

import com.twilight.quality.config.AlertConfig;
import com.twilight.quality.domain.alert.QualityAlert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 告警限流器
 * 防止告警风暴，限制每个规则在时间窗口内的告警数量
 */
@Component
public class AlertRateLimiter {
    
    private static final Logger log = LoggerFactory.getLogger(AlertRateLimiter.class);
    
    private final AlertConfig alertConfig;
    
    /**
     * 限流计数器
     * Key: ruleName:streamKey:windowStart
     * Value: 计数器
     */
    private final Map<String, WindowCounter> counters = new ConcurrentHashMap<>();
    
    public AlertRateLimiter(AlertConfig alertConfig) {
        this.alertConfig = alertConfig;
    }
    
    /**
     * 尝试获取告警配额
     * 
     * @param alert 告警
     * @return true-允许发送，false-被限流
     */
    public boolean tryAcquire(QualityAlert alert) {
        int windowSeconds = alertConfig.getRateLimit().getWindowSeconds();
        int maxAlerts = alertConfig.getRateLimit().getMaxAlertsPerRule();
        
        long currentTime = System.currentTimeMillis();
        long windowStart = (currentTime / (windowSeconds * 1000L)) * (windowSeconds * 1000L);
        
        String key = buildKey(alert.getRuleName(), alert.getStreamKey(), windowStart);
        
        WindowCounter counter = counters.compute(key, (k, v) -> {
            if (v == null || v.windowStart != windowStart) {
                // 新窗口，重置计数器
                return new WindowCounter(windowStart);
            }
            return v;
        });
        
        int count = counter.count.incrementAndGet();
        
        if (count > maxAlerts) {
            // 超过限制，记录一次（避免日志风暴）
            if (count == maxAlerts + 1) {
                log.warn("告警被限流: rule={}, streamKey={}, 当前窗口已发送 {} 条", 
                        alert.getRuleName(), alert.getStreamKey(), maxAlerts);
            }
            return false;
        }
        
        return true;
    }
    
    /**
     * 构建限流Key
     */
    private String buildKey(String ruleName, String streamKey, long windowStart) {
        return String.format("%s:%s:%d", ruleName, streamKey, windowStart);
    }
    
    /**
     * 清理过期的计数器
     */
    public void cleanup() {
        long currentTime = System.currentTimeMillis();
        int windowSeconds = alertConfig.getRateLimit().getWindowSeconds();
        long expireThreshold = currentTime - (windowSeconds * 2000L);
        
        counters.entrySet().removeIf(entry -> entry.getValue().windowStart < expireThreshold);
    }
    
    /**
     * 窗口计数器
     */
    private static class WindowCounter {
        final long windowStart;
        final AtomicInteger count;
        
        WindowCounter(long windowStart) {
            this.windowStart = windowStart;
            this.count = new AtomicInteger(0);
        }
    }
}


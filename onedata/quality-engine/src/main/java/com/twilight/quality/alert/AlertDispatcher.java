package com.twilight.quality.alert;

import com.twilight.quality.alert.channel.AlertChannel;
import com.twilight.quality.config.AlertConfig;
import com.twilight.quality.domain.alert.QualityAlert;
import com.twilight.quality.domain.enums.AlertLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * 告警分发器
 * 负责将告警分发到各个告警通道
 */
@Component
public class AlertDispatcher {
    
    private static final Logger log = LoggerFactory.getLogger(AlertDispatcher.class);
    
    private final AlertConfig alertConfig;
    private final AlertRateLimiter rateLimiter;
    private final List<AlertChannel> channels;
    
    public AlertDispatcher(AlertConfig alertConfig, 
                           AlertRateLimiter rateLimiter,
                           List<AlertChannel> channels) {
        this.alertConfig = alertConfig;
        this.rateLimiter = rateLimiter;
        this.channels = channels != null ? channels : new ArrayList<>();
    }
    
    @PostConstruct
    public void init() {
        log.info("告警分发器初始化，已注册 {} 个告警通道", channels.size());
        for (AlertChannel channel : channels) {
            log.info("  - {} (启用: {})", channel.getName(), channel.isEnabled());
        }
    }
    
    /**
     * 分发告警
     */
    public void dispatch(QualityAlert alert) {
        if (alert == null) {
            return;
        }
        
        // 1. 限流检查
        if (!rateLimiter.tryAcquire(alert)) {
            log.debug("告警被限流: rule={}, domain={}", alert.getRuleName(), alert.getDomain());
            return;
        }
        
        // 2. 分发到各通道
        for (AlertChannel channel : channels) {
            try {
                if (channel.isEnabled() && shouldSendToChannel(alert, channel)) {
                    channel.send(alert);
                }
            } catch (Exception e) {
                log.error("告警发送失败 channel={}: {}", channel.getName(), e.getMessage(), e);
            }
        }
        
        log.info("📢 告警分发: [{}] {} - {} - {}", 
                alert.getLevel(), alert.getDomain(), alert.getRuleName(), alert.getMessage());
    }
    
    /**
     * 判断是否应该发送到指定通道
     */
    private boolean shouldSendToChannel(QualityAlert alert, AlertChannel channel) {
        AlertLevel minLevel = channel.getMinLevel();
        return alert.getLevel().isAtLeast(minLevel);
    }
    
    /**
     * 获取已注册的通道数量
     */
    public int getChannelCount() {
        return channels.size();
    }
}


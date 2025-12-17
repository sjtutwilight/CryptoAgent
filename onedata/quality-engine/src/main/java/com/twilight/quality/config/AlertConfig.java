package com.twilight.quality.config;

import com.twilight.quality.domain.enums.AlertLevel;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 告警配置
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "quality.alert")
public class AlertConfig {
    
    /**
     * 告警通道配置
     */
    private Channels channels = new Channels();
    
    /**
     * 限流配置
     */
    private RateLimit rateLimit = new RateLimit();
    
    @Data
    public static class Channels {
        /**
         * Kafka告警通道
         */
        private KafkaChannel kafka = new KafkaChannel();
        
        /**
         * Webhook告警通道
         */
        private WebhookChannel webhook = new WebhookChannel();
    }
    
    @Data
    public static class KafkaChannel {
        /**
         * 是否启用
         */
        private boolean enabled = true;
        
        /**
         * 告警Topic
         */
        private String topic = "quality.alerts";
        
        /**
         * 最低告警级别
         */
        private String minLevel = "WARNING";
        
        public AlertLevel getMinAlertLevel() {
            return AlertLevel.fromString(minLevel);
        }
    }
    
    @Data
    public static class WebhookChannel {
        /**
         * 是否启用
         */
        private boolean enabled = false;
        
        /**
         * Webhook URL
         */
        private String url;
        
        /**
         * 最低告警级别
         */
        private String minLevel = "CRITICAL";
        
        /**
         * 超时时间（毫秒）
         */
        private int timeoutMs = 5000;
        
        public AlertLevel getMinAlertLevel() {
            return AlertLevel.fromString(minLevel);
        }
    }
    
    @Data
    public static class RateLimit {
        /**
         * 限流窗口（秒）
         */
        private int windowSeconds = 60;
        
        /**
         * 每规则每窗口最大告警数
         */
        private int maxAlertsPerRule = 5;
    }
}


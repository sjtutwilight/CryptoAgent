package com.twilight.quality.alert.channel;

import com.twilight.quality.config.AlertConfig;
import com.twilight.quality.domain.alert.QualityAlert;
import com.twilight.quality.domain.enums.AlertLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Webhook告警通道
 * 将告警发送到外部Webhook URL
 */
@Component
public class WebhookAlertChannel implements AlertChannel {
    
    private static final Logger log = LoggerFactory.getLogger(WebhookAlertChannel.class);
    
    private final AlertConfig alertConfig;
    private final WebClient webClient;
    
    public WebhookAlertChannel(AlertConfig alertConfig) {
        this.alertConfig = alertConfig;
        this.webClient = WebClient.builder()
                .defaultHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .build();
    }
    
    @Override
    public String getName() {
        return "webhook";
    }
    
    @Override
    public boolean isEnabled() {
        AlertConfig.WebhookChannel config = alertConfig.getChannels().getWebhook();
        return config.isEnabled() && config.getUrl() != null && !config.getUrl().isEmpty();
    }
    
    @Override
    public AlertLevel getMinLevel() {
        return alertConfig.getChannels().getWebhook().getMinAlertLevel();
    }
    
    @Override
    public void send(QualityAlert alert) {
        AlertConfig.WebhookChannel config = alertConfig.getChannels().getWebhook();
        String url = config.getUrl();
        int timeoutMs = config.getTimeoutMs();
        
        if (url == null || url.isEmpty()) {
            log.warn("Webhook URL未配置，跳过发送");
            return;
        }
        
        webClient.post()
                .uri(url)
                .bodyValue(alert.toJson())
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofMillis(timeoutMs))
                .doOnSuccess(response -> log.debug("Webhook发送成功: {}", url))
                .doOnError(error -> log.error("Webhook发送失败: {}, error={}", url, error.getMessage()))
                .onErrorResume(error -> Mono.empty())
                .subscribe();
    }
}


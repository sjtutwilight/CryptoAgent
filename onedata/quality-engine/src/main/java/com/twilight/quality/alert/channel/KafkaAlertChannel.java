package com.twilight.quality.alert.channel;

import com.twilight.quality.config.AlertConfig;
import com.twilight.quality.domain.alert.QualityAlert;
import com.twilight.quality.domain.enums.AlertLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka告警通道
 * 将告警发送到Kafka topic
 */
@Component
public class KafkaAlertChannel implements AlertChannel {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaAlertChannel.class);
    
    private final AlertConfig alertConfig;
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    public KafkaAlertChannel(AlertConfig alertConfig, 
                              KafkaTemplate<String, String> kafkaTemplate) {
        this.alertConfig = alertConfig;
        this.kafkaTemplate = kafkaTemplate;
    }
    
    @Override
    public String getName() {
        return "kafka";
    }
    
    @Override
    public boolean isEnabled() {
        return alertConfig.getChannels().getKafka().isEnabled();
    }
    
    @Override
    public AlertLevel getMinLevel() {
        return alertConfig.getChannels().getKafka().getMinAlertLevel();
    }
    
    @Override
    public void send(QualityAlert alert) {
        String topic = alertConfig.getChannels().getKafka().getTopic();
        String key = alert.getDomain().getDomainId() + ":" + alert.getRuleName();
        String value = alert.toJson();
        
        kafkaTemplate.send(topic, key, value)
                .addCallback(
                        result -> log.debug("告警发送成功: topic={}, key={}", topic, key),
                        ex -> log.error("告警发送失败: topic={}, key={}, error={}", 
                                topic, key, ex.getMessage())
                );
    }
}


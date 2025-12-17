package com.twilight.quality.alert.channel;

import com.twilight.quality.alert.AlertPersistenceService;
import com.twilight.quality.domain.alert.QualityAlert;
import com.twilight.quality.domain.enums.AlertLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 持久化告警通道
 * 将所有告警保存到PostgreSQL
 */
@Component
public class PersistenceAlertChannel implements AlertChannel {
    
    private static final Logger log = LoggerFactory.getLogger(PersistenceAlertChannel.class);
    
    private final AlertPersistenceService persistenceService;
    
    public PersistenceAlertChannel(AlertPersistenceService persistenceService) {
        this.persistenceService = persistenceService;
    }
    
    @Override
    public String getName() {
        return "persistence";
    }
    
    @Override
    public boolean isEnabled() {
        return true; // 始终启用
    }
    
    @Override
    public AlertLevel getMinLevel() {
        return AlertLevel.INFO; // 保存所有级别的告警
    }
    
    @Override
    public void send(QualityAlert alert) {
        persistenceService.saveAsync(alert);
    }
}


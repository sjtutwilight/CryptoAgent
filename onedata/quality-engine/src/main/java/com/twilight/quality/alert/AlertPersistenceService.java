package com.twilight.quality.alert;

import com.twilight.quality.domain.alert.QualityAlert;
import com.twilight.quality.domain.entity.AlertRecord;
import com.twilight.quality.repository.AlertRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * 告警持久化服务
 * 将告警记录保存到PostgreSQL
 */
@Service
public class AlertPersistenceService {
    
    private static final Logger log = LoggerFactory.getLogger(AlertPersistenceService.class);
    
    private final AlertRecordRepository alertRecordRepository;
    
    public AlertPersistenceService(AlertRecordRepository alertRecordRepository) {
        this.alertRecordRepository = alertRecordRepository;
    }
    
    /**
     * 异步保存告警记录
     */
    @Async
    public void saveAsync(QualityAlert alert) {
        try {
            AlertRecord record = AlertRecord.fromAlert(alert);
            alertRecordRepository.save(record);
            log.debug("告警记录已保存: {}", alert.getAlertId());
        } catch (Exception e) {
            log.error("保存告警记录失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 同步保存告警记录
     */
    public AlertRecord save(QualityAlert alert) {
        AlertRecord record = AlertRecord.fromAlert(alert);
        return alertRecordRepository.save(record);
    }
}


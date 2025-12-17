package com.twilight.quality.domain.entity;

import com.twilight.quality.domain.alert.QualityAlert;
import com.twilight.quality.domain.enums.AlertLevel;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.Instant;

/**
 * 告警记录实体（PostgreSQL）
 * 用于持久化告警历史
 */
@Data
@NoArgsConstructor
@Entity
@Table(name = "quality_alert_records", indexes = {
        @Index(name = "idx_alert_domain", columnList = "domain"),
        @Index(name = "idx_alert_level", columnList = "level"),
        @Index(name = "idx_alert_time", columnList = "alertTime"),
        @Index(name = "idx_alert_rule", columnList = "ruleName")
})
public class AlertRecord {
    
    @Id
    @Column(length = 36)
    private String alertId;
    
    @Enumerated(EnumType.STRING)
    @Column(length = 20)
    private AlertLevel level;
    
    @Column(length = 50)
    private String domain;
    
    @Column(length = 100)
    private String streamKey;
    
    @Column(length = 50)
    private String dimension;
    
    @Column(length = 100)
    private String ruleName;
    
    @Column(length = 500)
    private String message;
    
    private Double metricValue;
    
    private Double threshold;
    
    @Column(columnDefinition = "TEXT")
    private String contextJson;
    
    private Instant alertTime;
    
    private Instant processTime;
    
    @Column(updatable = false)
    private Instant createdAt;
    
    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
    
    /**
     * 从QualityAlert创建记录
     */
    public static AlertRecord fromAlert(QualityAlert alert) {
        AlertRecord record = new AlertRecord();
        record.setAlertId(alert.getAlertId());
        record.setLevel(alert.getLevel());
        record.setDomain(alert.getDomain() != null ? alert.getDomain().getDomainId() : null);
        record.setStreamKey(alert.getStreamKey());
        record.setDimension(alert.getDimension() != null ? alert.getDimension().getCode() : null);
        record.setRuleName(alert.getRuleName());
        record.setMessage(alert.getMessage());
        record.setMetricValue(alert.getMetricValue());
        record.setThreshold(alert.getThreshold());
        record.setContextJson(alert.getContextJson());
        record.setAlertTime(alert.getAlertTime() != null 
                ? Instant.ofEpochMilli(alert.getAlertTime()) : Instant.now());
        record.setProcessTime(alert.getProcessTime() != null 
                ? Instant.ofEpochMilli(alert.getProcessTime()) : Instant.now());
        return record;
    }
}


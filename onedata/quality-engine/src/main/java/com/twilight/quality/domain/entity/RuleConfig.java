package com.twilight.quality.domain.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.Instant;

/**
 * 规则配置实体（PostgreSQL）
 * 用于动态管理规则配置
 */
@Data
@NoArgsConstructor
@Entity
@Table(name = "quality_rule_configs", indexes = {
        @Index(name = "idx_rule_name", columnList = "ruleName", unique = true),
        @Index(name = "idx_rule_domain", columnList = "domain"),
        @Index(name = "idx_rule_enabled", columnList = "enabled")
})
public class RuleConfig {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    /**
     * 规则名称（唯一）
     */
    @Column(length = 100, nullable = false, unique = true)
    private String ruleName;
    
    /**
     * 业务域
     */
    @Column(length = 50)
    private String domain;
    
    /**
     * 质量维度
     */
    @Column(length = 50)
    private String dimension;
    
    /**
     * 是否启用
     */
    private Boolean enabled = true;
    
    /**
     * 告警级别
     */
    @Column(length = 20)
    private String alertLevel;
    
    /**
     * 规则配置JSON
     */
    @Column(columnDefinition = "TEXT")
    private String configJson;
    
    /**
     * 规则描述
     */
    @Column(length = 500)
    private String description;
    
    /**
     * 创建时间
     */
    @Column(updatable = false)
    private Instant createdAt;
    
    /**
     * 更新时间
     */
    private Instant updatedAt;
    
    @PrePersist
    public void prePersist() {
        Instant now = Instant.now();
        if (createdAt == null) {
            createdAt = now;
        }
        updatedAt = now;
    }
    
    @PreUpdate
    public void preUpdate() {
        updatedAt = Instant.now();
    }
}


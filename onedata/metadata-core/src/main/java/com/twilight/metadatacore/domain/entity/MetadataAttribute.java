package com.twilight.metadatacore.domain.entity;

import java.time.Instant;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.Table;

@Entity
@Table(name = "metadata_attr")
public class MetadataAttribute {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private UUID entityId;

    @Column(nullable = false)
    private String key;

    @Column(columnDefinition = "text")
    private String valueJson;

    private String level;

    @Column(nullable = false)
    private Instant createdAt;

    public Long getId() {
        return id;
    }

    public UUID getEntityId() {
        return entityId;
    }

    public void setEntityId(UUID entityId) {
        this.entityId = entityId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValueJson() {
        return valueJson;
    }

    public void setValueJson(String valueJson) {
        this.valueJson = valueJson;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    @PrePersist
    public void prePersist() {
        this.createdAt = Instant.now();
    }
}

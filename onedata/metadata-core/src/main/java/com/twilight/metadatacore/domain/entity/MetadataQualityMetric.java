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
@Table(name = "metadata_quality")
public class MetadataQualityMetric {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private UUID entityId;

    private Double completeness;
    private Double freshness;
    private Double schemaDrift;

    @Column(nullable = false)
    private Instant collectedAt;

    public Long getId() {
        return id;
    }

    public UUID getEntityId() {
        return entityId;
    }

    public void setEntityId(UUID entityId) {
        this.entityId = entityId;
    }

    public Double getCompleteness() {
        return completeness;
    }

    public void setCompleteness(Double completeness) {
        this.completeness = completeness;
    }

    public Double getFreshness() {
        return freshness;
    }

    public void setFreshness(Double freshness) {
        this.freshness = freshness;
    }

    public Double getSchemaDrift() {
        return schemaDrift;
    }

    public void setSchemaDrift(Double schemaDrift) {
        this.schemaDrift = schemaDrift;
    }

    public Instant getCollectedAt() {
        return collectedAt;
    }

    public void setCollectedAt(Instant collectedAt) {
        this.collectedAt = collectedAt;
    }

    @PrePersist
    public void prePersist() {
        if (this.collectedAt == null) {
            this.collectedAt = Instant.now();
        }
    }
}

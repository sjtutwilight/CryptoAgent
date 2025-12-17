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
@Table(name = "metadata_lineage")
public class MetadataLineage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private UUID upstreamId;

    @Column(nullable = false)
    private UUID downstreamId;

    private String relationType;
    private Double confidence;

    @Column(nullable = false)
    private Instant createdAt;

    public Long getId() {
        return id;
    }

    public UUID getUpstreamId() {
        return upstreamId;
    }

    public void setUpstreamId(UUID upstreamId) {
        this.upstreamId = upstreamId;
    }

    public UUID getDownstreamId() {
        return downstreamId;
    }

    public void setDownstreamId(UUID downstreamId) {
        this.downstreamId = downstreamId;
    }

    public String getRelationType() {
        return relationType;
    }

    public void setRelationType(String relationType) {
        this.relationType = relationType;
    }

    public Double getConfidence() {
        return confidence;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    @PrePersist
    public void prePersist() {
        this.createdAt = Instant.now();
    }
}

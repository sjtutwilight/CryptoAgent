package com.twilight.metadatacore.domain.dto;

import java.time.Instant;

public class MetadataQualityView {

    private final Double completeness;
    private final Double freshness;
    private final Double schemaDrift;
    private final Instant collectedAt;

    public MetadataQualityView(Double completeness, Double freshness, Double schemaDrift, Instant collectedAt) {
        this.completeness = completeness;
        this.freshness = freshness;
        this.schemaDrift = schemaDrift;
        this.collectedAt = collectedAt;
    }

    public Double getCompleteness() {
        return completeness;
    }

    public Double getFreshness() {
        return freshness;
    }

    public Double getSchemaDrift() {
        return schemaDrift;
    }

    public Instant getCollectedAt() {
        return collectedAt;
    }
}

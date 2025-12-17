package com.twilight.metadatacore.domain.dto;

import java.time.Instant;

public class MetadataAttributeView {

    private final String key;
    private final String valueJson;
    private final String level;
    private final Instant createdAt;

    public MetadataAttributeView(String key, String valueJson, String level, Instant createdAt) {
        this.key = key;
        this.valueJson = valueJson;
        this.level = level;
        this.createdAt = createdAt;
    }

    public String getKey() {
        return key;
    }

    public String getValueJson() {
        return valueJson;
    }

    public String getLevel() {
        return level;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}

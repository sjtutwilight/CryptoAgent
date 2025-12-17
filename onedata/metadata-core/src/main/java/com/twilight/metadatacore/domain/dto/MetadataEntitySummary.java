package com.twilight.metadatacore.domain.dto;

import com.twilight.metadatacore.domain.enums.MetadataStatus;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public class MetadataEntitySummary {

    private UUID id;
    private String name;
    private String type;
    private String domain;
    private String platform;
    private String locator;
    private MetadataStatus status;
    private Instant updatedAt;
    private Set<String> tags;

    public MetadataEntitySummary(UUID id,
                                 String name,
                                 String type,
                                 String domain,
                                 String platform,
                                 String locator,
                                 MetadataStatus status,
                                 Instant updatedAt,
                                 Set<String> tags) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.domain = domain;
        this.platform = platform;
        this.locator = locator;
        this.status = status;
        this.updatedAt = updatedAt;
        this.tags = tags;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getDomain() {
        return domain;
    }

    public String getPlatform() {
        return platform;
    }

    public String getLocator() {
        return locator;
    }

    public MetadataStatus getStatus() {
        return status;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Set<String> getTags() {
        return tags;
    }
}

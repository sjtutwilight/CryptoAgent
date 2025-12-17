package com.twilight.metadatacore.domain.dto;

import com.twilight.metadatacore.domain.entity.MetadataEntity;
import com.twilight.metadatacore.domain.enums.MetadataStatus;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class MetadataEntityDetail {

    private final UUID id;
    private final String name;
    private final String type;
    private final String domain;
    private final String platform;
    private final String locator;
    private final String version;
    private final MetadataStatus status;
    private final String protocol;
    private final String chainId;
    private final String contractAddress;
    private final String cluster;
    private final String dbName;
    private final String topic;
    private final String jobId;
    private final String description;
    private final Instant updatedAt;
    private final Set<String> tags;
    private final List<MetadataAttributeView> attributes;
    private final List<String> recentEvents;
    private final MetadataQualityView quality;

    public MetadataEntityDetail(MetadataEntity entity,
                                Set<String> tags,
                                List<MetadataAttributeView> attributes,
                                List<String> recentEvents,
                                MetadataQualityView quality) {
        this.id = entity.getId();
        this.name = entity.getName();
        this.type = entity.getType();
        this.domain = entity.getDomain();
        this.platform = entity.getPlatform();
        this.locator = entity.getLocator();
        this.version = entity.getVersion();
        this.status = entity.getStatus();
        this.protocol = entity.getProtocol();
        this.chainId = entity.getChainId();
        this.contractAddress = entity.getContractAddress();
        this.cluster = entity.getCluster();
        this.dbName = entity.getDbName();
        this.topic = entity.getTopic();
        this.jobId = entity.getJobId();
        this.description = entity.getDescription();
        this.updatedAt = entity.getUpdatedAt();
        this.tags = tags;
        this.attributes = attributes;
        this.recentEvents = recentEvents;
        this.quality = quality;
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

    public String getVersion() {
        return version;
    }

    public MetadataStatus getStatus() {
        return status;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getChainId() {
        return chainId;
    }

    public String getContractAddress() {
        return contractAddress;
    }

    public String getCluster() {
        return cluster;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTopic() {
        return topic;
    }

    public String getJobId() {
        return jobId;
    }

    public String getDescription() {
        return description;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Set<String> getTags() {
        return tags;
    }

    public List<MetadataAttributeView> getAttributes() {
        return attributes;
    }

    public List<String> getRecentEvents() {
        return recentEvents;
    }

    public MetadataQualityView getQuality() {
        return quality;
    }
}

package com.twilight.metadatacore.ingestion;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class MetadataEnvelope {

    private MetadataEntityPayload entity;
    private List<MetadataAttributePayload> attributes;
    private List<TagPayload> tags;
    private List<LineagePayload> lineage;
    private MetadataQualityPayload quality;
    private Instant occurredAt;

    public MetadataEntityPayload getEntity() {
        return entity;
    }

    public void setEntity(MetadataEntityPayload entity) {
        this.entity = entity;
    }

    public List<MetadataAttributePayload> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<MetadataAttributePayload> attributes) {
        this.attributes = attributes;
    }

    public List<TagPayload> getTags() {
        return tags;
    }

    public void setTags(List<TagPayload> tags) {
        this.tags = tags;
    }

    public List<LineagePayload> getLineage() {
        return lineage;
    }

    public void setLineage(List<LineagePayload> lineage) {
        this.lineage = lineage;
    }

    public MetadataQualityPayload getQuality() {
        return quality;
    }

    public void setQuality(MetadataQualityPayload quality) {
        this.quality = quality;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    public static class MetadataEntityPayload {
        private UUID id;
        private String type;
        private String name;
        private String domain;
        private String platform;
        private String locator;
        private String version;
        private String status;
        private String protocol;
        private String chainId;
        private String contractAddress;
        private String cluster;
        private String dbName;
        private String topic;
        private String jobId;
        private String description;

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        public String getPlatform() {
            return platform;
        }

        public void setPlatform(String platform) {
            this.platform = platform;
        }

        public String getLocator() {
            return locator;
        }

        public void setLocator(String locator) {
            this.locator = locator;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public String getChainId() {
            return chainId;
        }

        public void setChainId(String chainId) {
            this.chainId = chainId;
        }

        public String getContractAddress() {
            return contractAddress;
        }

        public void setContractAddress(String contractAddress) {
            this.contractAddress = contractAddress;
        }

        public String getCluster() {
            return cluster;
        }

        public void setCluster(String cluster) {
            this.cluster = cluster;
        }

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }

    public static class MetadataAttributePayload {
        private UUID entityId;
        private String key;
        private String value;
        private String level;

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

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }
    }

    public static class TagPayload {
        private UUID entityId;
        private String value;

        public UUID getEntityId() {
            return entityId;
        }

        public void setEntityId(UUID entityId) {
            this.entityId = entityId;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static class LineagePayload {
        private UUID upstreamId;
        private UUID downstreamId;
        private String relationType;
        private Double confidence;

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
    }

    public static class MetadataQualityPayload {
        private UUID entityId;
        private Double completeness;
        private Double freshness;
        private Double schemaDrift;
        private Instant collectedAt;

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
    }
}

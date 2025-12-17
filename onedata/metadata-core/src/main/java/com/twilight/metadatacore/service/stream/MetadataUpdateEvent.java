package com.twilight.metadatacore.service.stream;

import java.time.Instant;
import java.util.UUID;

public class MetadataUpdateEvent {

    private final UUID entityId;
    private final String changeType;
    private final Instant occurredAt;

    public MetadataUpdateEvent(UUID entityId, String changeType, Instant occurredAt) {
        this.entityId = entityId;
        this.changeType = changeType;
        this.occurredAt = occurredAt;
    }

    public UUID getEntityId() {
        return entityId;
    }

    public String getChangeType() {
        return changeType;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }
}

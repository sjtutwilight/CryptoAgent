package com.twilight.metadatacore.domain.dto;

public class DomainStats {

    private final String domain;
    private final long totalEntities;
    private final long activeEntities;
    private final long criticalEntities;

    public DomainStats(String domain, long totalEntities, long activeEntities, long criticalEntities) {
        this.domain = domain;
        this.totalEntities = totalEntities;
        this.activeEntities = activeEntities;
        this.criticalEntities = criticalEntities;
    }

    public String getDomain() {
        return domain;
    }

    public long getTotalEntities() {
        return totalEntities;
    }

    public long getActiveEntities() {
        return activeEntities;
    }

    public long getCriticalEntities() {
        return criticalEntities;
    }
}

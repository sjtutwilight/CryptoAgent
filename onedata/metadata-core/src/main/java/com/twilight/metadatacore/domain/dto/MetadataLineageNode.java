package com.twilight.metadatacore.domain.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MetadataLineageNode {

    private UUID id;
    private String name;
    private String type;
    private String relationType;
    private Double confidence;
    private final List<MetadataLineageNode> children = new ArrayList<>();

    public MetadataLineageNode(UUID id, String name, String type, String relationType, Double confidence) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.relationType = relationType;
        this.confidence = confidence;
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

    public String getRelationType() {
        return relationType;
    }

    public Double getConfidence() {
        return confidence;
    }

    public List<MetadataLineageNode> getChildren() {
        return children;
    }

    public void addChild(MetadataLineageNode child) {
        this.children.add(child);
    }
}

package com.twilight.metadatacore.service.query;

import com.twilight.metadatacore.config.MetadataProperties;
import com.twilight.metadatacore.domain.dto.MetadataLineageNode;
import com.twilight.metadatacore.domain.entity.MetadataEntity;
import com.twilight.metadatacore.domain.entity.MetadataLineage;
import com.twilight.metadatacore.exception.MetadataNotFoundException;
import com.twilight.metadatacore.repository.MetadataEntityRepository;
import com.twilight.metadatacore.repository.MetadataLineageRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class MetadataLineageService {

    private final MetadataEntityRepository entityRepository;
    private final MetadataLineageRepository lineageRepository;
    private final MetadataProperties properties;

    public MetadataLineageService(MetadataEntityRepository entityRepository,
                                  MetadataLineageRepository lineageRepository,
                                  MetadataProperties properties) {
        this.entityRepository = entityRepository;
        this.lineageRepository = lineageRepository;
        this.properties = properties;
    }

    public MetadataLineageNode traverse(UUID entityId, Direction direction) {
        MetadataEntity entity = entityRepository.findById(entityId)
                .orElseThrow(() -> new MetadataNotFoundException("Metadata entity not found: " + entityId));
        MetadataLineageNode root = new MetadataLineageNode(entity.getId(), entity.getName(),
                entity.getType(), null, null);
        populateChildren(root, direction, 1);
        return root;
    }

    private void populateChildren(MetadataLineageNode node, Direction direction, int depth) {
        if (depth > properties.getLineage().getMaxDepth()) {
            return;
        }
        List<MetadataLineage> relations = direction == Direction.UPSTREAM
                ? lineageRepository.findByDownstreamId(node.getId())
                : lineageRepository.findByUpstreamId(node.getId());

        for (MetadataLineage relation : relations) {
            UUID targetId = direction == Direction.UPSTREAM ? relation.getUpstreamId() : relation.getDownstreamId();
            entityRepository.findById(targetId).ifPresent(target -> {
                MetadataLineageNode child = new MetadataLineageNode(target.getId(), target.getName(),
                        target.getType(), relation.getRelationType(), relation.getConfidence());
                node.addChild(child);
                populateChildren(child, direction, depth + 1);
            });
        }
    }

    public enum Direction {
        UPSTREAM,
        DOWNSTREAM
    }
}

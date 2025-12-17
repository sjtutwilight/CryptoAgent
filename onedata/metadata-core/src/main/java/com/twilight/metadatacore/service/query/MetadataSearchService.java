package com.twilight.metadatacore.service.query;

import com.twilight.metadatacore.domain.dto.MetadataEntitySummary;
import com.twilight.metadatacore.domain.dto.MetadataSearchRequest;
import com.twilight.metadatacore.domain.entity.MetadataEntity;
import com.twilight.metadatacore.repository.MetadataEntityRepository;
import com.twilight.metadatacore.repository.MetadataTagRepository;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

@Service
public class MetadataSearchService {

    private final MetadataEntityRepository entityRepository;
    private final MetadataTagRepository tagRepository;
    private final MetadataSpecificationBuilder specificationBuilder;

    public MetadataSearchService(MetadataEntityRepository entityRepository,
                                 MetadataTagRepository tagRepository,
                                 MetadataSpecificationBuilder specificationBuilder) {
        this.entityRepository = entityRepository;
        this.tagRepository = tagRepository;
        this.specificationBuilder = specificationBuilder;
    }

    public Page<MetadataEntitySummary> search(MetadataSearchRequest request) {
        Set<UUID> filteredEntityIds = resolveTagFilter(request);
        Specification<MetadataEntity> spec = specificationBuilder.build(request, filteredEntityIds);
        Pageable pageable = buildPageable(request);
        Page<MetadataEntity> entities = entityRepository.findAll(spec, pageable);

        List<MetadataEntitySummary> summaries = entities.getContent().stream()
                .map(entity -> new MetadataEntitySummary(
                        entity.getId(),
                        entity.getName(),
                        entity.getType(),
                        entity.getDomain(),
                        entity.getPlatform(),
                        entity.getLocator(),
                        entity.getStatus(),
                        entity.getUpdatedAt(),
                        tagRepository.findTagValues(entity.getId())))
                .collect(Collectors.toList());

        return new PageImpl<>(summaries, pageable, entities.getTotalElements());
    }

    private Set<UUID> resolveTagFilter(MetadataSearchRequest request) {
        if (request.getTags() == null || request.getTags().isEmpty()) {
            return Collections.emptySet();
        }
        return tagRepository.findEntityIdsByTags(Set.copyOf(request.getTags()));
    }

    private Pageable buildPageable(MetadataSearchRequest request) {
        Sort.Direction direction = request.getSortDirection() == MetadataSearchRequest.SortDirection.ASC
                ? Sort.Direction.ASC : Sort.Direction.DESC;
        Sort sort = Sort.by(direction, request.getSortBy());
        return PageRequest.of(request.getPage(), request.getSize(), sort);
    }
}

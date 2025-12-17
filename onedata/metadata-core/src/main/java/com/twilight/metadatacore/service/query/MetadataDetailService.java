package com.twilight.metadatacore.service.query;

import com.twilight.metadatacore.domain.dto.MetadataAttributeView;
import com.twilight.metadatacore.domain.dto.MetadataEntityDetail;
import com.twilight.metadatacore.domain.dto.MetadataQualityView;
import com.twilight.metadatacore.domain.entity.MetadataAttribute;
import com.twilight.metadatacore.domain.entity.MetadataEntity;
import com.twilight.metadatacore.domain.entity.MetadataQualityMetric;
import com.twilight.metadatacore.exception.MetadataNotFoundException;
import com.twilight.metadatacore.repository.MetadataAttributeRepository;
import com.twilight.metadatacore.repository.MetadataEntityRepository;
import com.twilight.metadatacore.repository.MetadataEventRepository;
import com.twilight.metadatacore.repository.MetadataQualityRepository;
import com.twilight.metadatacore.repository.MetadataTagRepository;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
public class MetadataDetailService {

    private final MetadataEntityRepository entityRepository;
    private final MetadataAttributeRepository attributeRepository;
    private final MetadataTagRepository tagRepository;
    private final MetadataEventRepository eventRepository;
    private final MetadataQualityRepository qualityRepository;

    public MetadataDetailService(MetadataEntityRepository entityRepository,
                                 MetadataAttributeRepository attributeRepository,
                                 MetadataTagRepository tagRepository,
                                 MetadataEventRepository eventRepository,
                                 MetadataQualityRepository qualityRepository) {
        this.entityRepository = entityRepository;
        this.attributeRepository = attributeRepository;
        this.tagRepository = tagRepository;
        this.eventRepository = eventRepository;
        this.qualityRepository = qualityRepository;
    }

    @Cacheable(cacheNames = "metadata:detail", key = "#entityId")
    public MetadataEntityDetail getDetail(UUID entityId) {
        MetadataEntity entity = entityRepository.findById(entityId)
                .orElseThrow(() -> new MetadataNotFoundException("Metadata entity not found: " + entityId));
        Set<String> tags = tagRepository.findTagValues(entityId);
        List<MetadataAttributeView> attributeViews = attributeRepository.findByEntityId(entityId)
                .stream()
                .map(this::mapAttribute)
                .collect(Collectors.toList());

        List<String> recentEvents = eventRepository.findTop10ByEntityIdOrderByOccurredAtDesc(entityId)
                .stream()
                .map(MetadataDetailService::truncatePayload)
                .collect(Collectors.toList());

        MetadataQualityView qualityView = qualityRepository
                .findTopByEntityIdOrderByCollectedAtDesc(entityId)
                .map(this::mapQuality)
                .orElse(null);

        return new MetadataEntityDetail(entity, tags, attributeViews, recentEvents, qualityView);
    }

    @CacheEvict(cacheNames = "metadata:detail", key = "#entityId")
    public void evict(UUID entityId) {
        // Cache eviction handled by annotation
    }

    private MetadataAttributeView mapAttribute(MetadataAttribute attribute) {
        return new MetadataAttributeView(attribute.getKey(), attribute.getValueJson(),
                attribute.getLevel(), attribute.getCreatedAt());
    }

    private MetadataQualityView mapQuality(MetadataQualityMetric metric) {
        return new MetadataQualityView(metric.getCompleteness(), metric.getFreshness(),
                metric.getSchemaDrift(), metric.getCollectedAt());
    }

    private static String truncatePayload(com.twilight.metadatacore.domain.entity.MetadataEvent event) {
        String payload = event.getPayload();
        if (payload == null) {
            return "";
        }
        return payload.length() > 512 ? payload.substring(0, 512) + "..." : payload;
    }
}

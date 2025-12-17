package com.twilight.metadatacore.service.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.metadatacore.domain.entity.MetadataAttribute;
import com.twilight.metadatacore.domain.entity.MetadataEntity;
import com.twilight.metadatacore.domain.entity.MetadataEvent;
import com.twilight.metadatacore.domain.entity.MetadataLineage;
import com.twilight.metadatacore.domain.entity.MetadataQualityMetric;
import com.twilight.metadatacore.domain.entity.MetadataTag;
import com.twilight.metadatacore.domain.enums.MetadataStatus;
import com.twilight.metadatacore.ingestion.MetadataEnvelope;
import com.twilight.metadatacore.ingestion.MetadataEnvelope.LineagePayload;
import com.twilight.metadatacore.ingestion.MetadataEnvelope.MetadataAttributePayload;
import com.twilight.metadatacore.ingestion.MetadataEnvelope.MetadataEntityPayload;
import com.twilight.metadatacore.ingestion.MetadataEnvelope.MetadataQualityPayload;
import com.twilight.metadatacore.ingestion.MetadataEnvelope.TagPayload;
import com.twilight.metadatacore.repository.MetadataAttributeRepository;
import com.twilight.metadatacore.repository.MetadataEntityRepository;
import com.twilight.metadatacore.repository.MetadataEventRepository;
import com.twilight.metadatacore.repository.MetadataLineageRepository;
import com.twilight.metadatacore.repository.MetadataQualityRepository;
import com.twilight.metadatacore.repository.MetadataTagRepository;
import com.twilight.metadatacore.service.query.MetadataDetailService;
import com.twilight.metadatacore.service.stream.MetadataUpdatePublisher;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class MetadataIngestionService {

    private static final Logger log = LoggerFactory.getLogger(MetadataIngestionService.class);

    private final ObjectMapper objectMapper;
    private final MetadataEntityRepository entityRepository;
    private final MetadataAttributeRepository attributeRepository;
    private final MetadataTagRepository tagRepository;
    private final MetadataLineageRepository lineageRepository;
    private final MetadataQualityRepository qualityRepository;
    private final MetadataEventRepository eventRepository;
    private final MetadataUpdatePublisher updatePublisher;
    private final MetadataDetailService detailService;

    public MetadataIngestionService(ObjectMapper objectMapper,
                                    MetadataEntityRepository entityRepository,
                                    MetadataAttributeRepository attributeRepository,
                                    MetadataTagRepository tagRepository,
                                    MetadataLineageRepository lineageRepository,
                                    MetadataQualityRepository qualityRepository,
                                    MetadataEventRepository eventRepository,
                                    MetadataUpdatePublisher updatePublisher,
                                    MetadataDetailService detailService) {
        this.objectMapper = objectMapper;
        this.entityRepository = entityRepository;
        this.attributeRepository = attributeRepository;
        this.tagRepository = tagRepository;
        this.lineageRepository = lineageRepository;
        this.qualityRepository = qualityRepository;
        this.eventRepository = eventRepository;
        this.updatePublisher = updatePublisher;
        this.detailService = detailService;
    }

    @KafkaListener(id = "metadata-core-listener", topics = "#{@metadataTopics}")
    public void onMessage(String payload, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        try {
            MetadataEnvelope envelope = objectMapper.readValue(payload, MetadataEnvelope.class);
            ingestEnvelope(envelope, topic);
        } catch (Exception e) {
            log.error("Failed to process metadata payload from topic {}", topic, e);
        }
    }

    @Transactional
    protected void ingestEnvelope(MetadataEnvelope envelope, String topic) {
        if (envelope.getEntity() == null) {
            log.warn("Skipping metadata payload without entity body. topic={}, occurredAt={}",
                    topic, envelope.getOccurredAt());
            return;
        }

        MetadataEntityPayload entityPayload = envelope.getEntity();
        UUID entityId = entityPayload.getId() != null ? entityPayload.getId() : UUID.randomUUID();
        MetadataEntity entity = entityRepository.findById(entityId).orElseGet(MetadataEntity::new);
        entity.setId(entityId);
        entity.setType(entityPayload.getType());
        entity.setName(entityPayload.getName());
        entity.setDomain(entityPayload.getDomain());
        entity.setPlatform(entityPayload.getPlatform());
        entity.setLocator(entityPayload.getLocator());
        entity.setVersion(entityPayload.getVersion());
        entity.setStatus(resolveStatus(entityPayload.getStatus()));
        entity.setProtocol(entityPayload.getProtocol());
        entity.setChainId(entityPayload.getChainId());
        entity.setContractAddress(entityPayload.getContractAddress());
        entity.setCluster(entityPayload.getCluster());
        entity.setDbName(entityPayload.getDbName());
        entity.setTopic(entityPayload.getTopic());
        entity.setJobId(entityPayload.getJobId());
        entity.setDescription(entityPayload.getDescription());
        entityRepository.save(entity);

        replaceAttributes(entityId, envelope.getAttributes());
        replaceTags(entityId, envelope.getTags());
        replaceLineage(entityId, envelope.getLineage());
        upsertQuality(envelope.getQuality());

        MetadataEvent event = new MetadataEvent();
        event.setEntityId(entityId);
        event.setChangeType("UPSERT");
        event.setPayload(payloadSnapshot(envelope));
        event.setOccurredAt(envelope.getOccurredAt() != null ? envelope.getOccurredAt() : Instant.now());
        eventRepository.save(event);

        detailService.evict(entityId);
        updatePublisher.publishUpdate(entityId, event.getChangeType(), event.getOccurredAt());
    }

    private MetadataStatus resolveStatus(String status) {
        if (status == null) {
            return MetadataStatus.UNKNOWN;
        }
        try {
            return MetadataStatus.valueOf(status.toUpperCase());
        } catch (IllegalArgumentException ex) {
            return MetadataStatus.UNKNOWN;
        }
    }

    private void replaceAttributes(UUID entityId, List<MetadataAttributePayload> attributes) {
        attributeRepository.deleteByEntityId(entityId);
        if (attributes == null) {
            return;
        }
        List<MetadataAttribute> toSave = attributes.stream()
                .filter(Objects::nonNull)
                .map(payload -> {
                    MetadataAttribute attribute = new MetadataAttribute();
                    attribute.setEntityId(entityId);
                    attribute.setKey(payload.getKey());
                    attribute.setValueJson(payload.getValue());
                    attribute.setLevel(payload.getLevel());
                    return attribute;
                })
                .collect(Collectors.toList());
        attributeRepository.saveAll(toSave);
    }

    private void replaceTags(UUID entityId, List<TagPayload> tags) {
        tagRepository.deleteByEntityId(entityId);
        if (tags == null) {
            return;
        }
        List<MetadataTag> toSave = tags.stream()
                .filter(Objects::nonNull)
                .map(tagPayload -> {
                    MetadataTag tag = new MetadataTag();
                    tag.setEntityId(entityId);
                    tag.setTag(tagPayload.getValue());
                    return tag;
                })
                .collect(Collectors.toList());
        tagRepository.saveAll(toSave);
    }

    private void replaceLineage(UUID entityId, List<LineagePayload> lineagePayloads) {
        lineageRepository.deleteByUpstreamIdOrDownstreamId(entityId, entityId);
        if (lineagePayloads == null) {
            return;
        }
        List<MetadataLineage> toSave = lineagePayloads.stream()
                .filter(Objects::nonNull)
                .map(payload -> {
                    MetadataLineage lineage = new MetadataLineage();
                    lineage.setUpstreamId(payload.getUpstreamId());
                    lineage.setDownstreamId(payload.getDownstreamId());
                    lineage.setRelationType(payload.getRelationType());
                    lineage.setConfidence(payload.getConfidence());
                    return lineage;
                })
                .collect(Collectors.toList());
        lineageRepository.saveAll(toSave);
    }

    private void upsertQuality(MetadataQualityPayload qualityPayload) {
        if (qualityPayload == null || qualityPayload.getEntityId() == null) {
            return;
        }
        MetadataQualityMetric metric = new MetadataQualityMetric();
        metric.setEntityId(qualityPayload.getEntityId());
        metric.setCompleteness(qualityPayload.getCompleteness());
        metric.setFreshness(qualityPayload.getFreshness());
        metric.setSchemaDrift(qualityPayload.getSchemaDrift());
        metric.setCollectedAt(qualityPayload.getCollectedAt());
        qualityRepository.save(metric);
    }

    private String payloadSnapshot(MetadataEnvelope envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            return "{\"error\":\"unable to serialize\"}";
        }
    }
}

package com.twilight.metadatacore.repository;

import com.twilight.metadatacore.domain.entity.MetadataQualityMetric;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MetadataQualityRepository extends JpaRepository<MetadataQualityMetric, Long> {

    Optional<MetadataQualityMetric> findTopByEntityIdOrderByCollectedAtDesc(UUID entityId);
}

package com.twilight.metadatacore.repository;

import com.twilight.metadatacore.domain.entity.MetadataEvent;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MetadataEventRepository extends JpaRepository<MetadataEvent, Long> {
    List<MetadataEvent> findTop10ByEntityIdOrderByOccurredAtDesc(UUID entityId);
}

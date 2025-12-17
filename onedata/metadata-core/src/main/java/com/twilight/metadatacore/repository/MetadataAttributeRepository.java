package com.twilight.metadatacore.repository;

import com.twilight.metadatacore.domain.entity.MetadataAttribute;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MetadataAttributeRepository extends JpaRepository<MetadataAttribute, Long> {
    List<MetadataAttribute> findByEntityId(UUID entityId);

    void deleteByEntityId(UUID entityId);
}

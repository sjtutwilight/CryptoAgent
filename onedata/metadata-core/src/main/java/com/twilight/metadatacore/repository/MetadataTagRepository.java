package com.twilight.metadatacore.repository;

import com.twilight.metadatacore.domain.entity.MetadataTag;
import java.util.Set;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface MetadataTagRepository extends JpaRepository<MetadataTag, Long> {

    @Query("select t.tag from MetadataTag t where t.entityId = :entityId")
    Set<String> findTagValues(UUID entityId);

    @Query("select t.entityId from MetadataTag t where t.tag in :tags")
    Set<UUID> findEntityIdsByTags(Set<String> tags);

    void deleteByEntityId(UUID entityId);
}

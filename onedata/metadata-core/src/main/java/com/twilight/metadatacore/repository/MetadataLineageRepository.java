package com.twilight.metadatacore.repository;

import com.twilight.metadatacore.domain.entity.MetadataLineage;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MetadataLineageRepository extends JpaRepository<MetadataLineage, Long> {

    List<MetadataLineage> findByUpstreamId(UUID upstreamId);

    List<MetadataLineage> findByDownstreamId(UUID downstreamId);

    void deleteByUpstreamIdOrDownstreamId(UUID upstreamId, UUID downstreamId);
}

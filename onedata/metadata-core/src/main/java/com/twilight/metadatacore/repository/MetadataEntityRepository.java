package com.twilight.metadatacore.repository;

import com.twilight.metadatacore.domain.entity.MetadataEntity;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface MetadataEntityRepository
        extends JpaRepository<MetadataEntity, UUID>, JpaSpecificationExecutor<MetadataEntity> {

    @Query("select count(m) from MetadataEntity m where m.domain = :domain")
    long countByDomain(@Param("domain") String domain);

    @Query("select count(m) from MetadataEntity m where m.domain = :domain and m.status = com.twilight.metadatacore.domain.enums.MetadataStatus.ACTIVE")
    long countActiveByDomain(@Param("domain") String domain);

    @Query("select count(m) from MetadataEntity m where m.domain = :domain and m.status in (com.twilight.metadatacore.domain.enums.MetadataStatus.FAILED, com.twilight.metadatacore.domain.enums.MetadataStatus.DEPRECATED)")
    long countCriticalByDomain(@Param("domain") String domain);
}

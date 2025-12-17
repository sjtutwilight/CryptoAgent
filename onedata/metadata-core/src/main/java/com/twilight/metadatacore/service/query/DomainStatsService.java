package com.twilight.metadatacore.service.query;

import com.twilight.metadatacore.domain.dto.DomainStats;
import com.twilight.metadatacore.repository.MetadataEntityRepository;
import org.springframework.stereotype.Service;

@Service
public class DomainStatsService {

    private final MetadataEntityRepository entityRepository;

    public DomainStatsService(MetadataEntityRepository entityRepository) {
        this.entityRepository = entityRepository;
    }

    public DomainStats getStats(String domain) {
        long total = entityRepository.countByDomain(domain);
        long active = entityRepository.countActiveByDomain(domain);
        long critical = entityRepository.countCriticalByDomain(domain);
        return new DomainStats(domain, total, active, critical);
    }
}

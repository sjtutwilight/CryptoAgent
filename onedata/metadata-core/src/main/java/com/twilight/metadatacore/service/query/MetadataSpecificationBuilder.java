package com.twilight.metadatacore.service.query;

import com.twilight.metadatacore.domain.dto.MetadataSearchRequest;
import com.twilight.metadatacore.domain.entity.MetadataEntity;
import java.util.Set;
import java.util.UUID;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class MetadataSpecificationBuilder {

    public Specification<MetadataEntity> build(MetadataSearchRequest request, Set<UUID> entityIdsByTag) {
        Specification<MetadataEntity> spec = Specification.where(null);

        if (StringUtils.hasText(request.getKeyword())) {
            spec = spec.and(nameOrLocatorLike(request.getKeyword()));
        }
        if (StringUtils.hasText(request.getDomain())) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("domain"), request.getDomain()));
        }
        if (StringUtils.hasText(request.getType())) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("type"), request.getType()));
        }
        if (StringUtils.hasText(request.getPlatform())) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("platform"), request.getPlatform()));
        }
        if (request.getStatus() != null) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("status"), request.getStatus()));
        }
        if (entityIdsByTag != null && !entityIdsByTag.isEmpty()) {
            spec = spec.and((root, query, cb) -> root.get("id").in(entityIdsByTag));
        } else if (request.getTags() != null && !request.getTags().isEmpty()) {
            spec = spec.and((root, query, cb) -> cb.disjunction());
        }
        return spec;
    }

    private Specification<MetadataEntity> nameOrLocatorLike(String keyword) {
        return (root, query, cb) -> {
            String likeExpression = "%" + keyword.toLowerCase() + "%";
            return cb.or(
                    cb.like(cb.lower(root.get("name")), likeExpression),
                    cb.like(cb.lower(root.get("locator")), likeExpression));
        };
    }
}

package com.twilight.quality.repository;

import com.twilight.quality.domain.entity.RuleConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * 规则配置Repository
 */
@Repository
public interface RuleConfigRepository extends JpaRepository<RuleConfig, Long> {
    
    /**
     * 根据规则名称查找
     */
    Optional<RuleConfig> findByRuleName(String ruleName);
    
    /**
     * 根据业务域查找
     */
    List<RuleConfig> findByDomain(String domain);
    
    /**
     * 查找所有启用的规则
     */
    List<RuleConfig> findByEnabledTrue();
    
    /**
     * 根据业务域查找启用的规则
     */
    List<RuleConfig> findByDomainAndEnabledTrue(String domain);
    
    /**
     * 检查规则名称是否存在
     */
    boolean existsByRuleName(String ruleName);
}


package com.twilight.quality.repository;

import com.twilight.quality.domain.entity.AlertRecord;
import com.twilight.quality.domain.enums.AlertLevel;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/**
 * 告警记录Repository
 */
@Repository
public interface AlertRecordRepository extends JpaRepository<AlertRecord, String> {
    
    /**
     * 按业务域查询
     */
    Page<AlertRecord> findByDomainOrderByAlertTimeDesc(String domain, Pageable pageable);
    
    /**
     * 按告警级别查询
     */
    Page<AlertRecord> findByLevelOrderByAlertTimeDesc(AlertLevel level, Pageable pageable);
    
    /**
     * 按规则名称查询
     */
    Page<AlertRecord> findByRuleNameOrderByAlertTimeDesc(String ruleName, Pageable pageable);
    
    /**
     * 按时间范围查询
     */
    Page<AlertRecord> findByAlertTimeBetweenOrderByAlertTimeDesc(
            Instant start, Instant end, Pageable pageable);
    
    /**
     * 组合查询
     */
    @Query("SELECT a FROM AlertRecord a WHERE " +
           "(:domain IS NULL OR a.domain = :domain) AND " +
           "(:level IS NULL OR a.level = :level) AND " +
           "(:ruleName IS NULL OR a.ruleName = :ruleName) AND " +
           "(:start IS NULL OR a.alertTime >= :start) AND " +
           "(:end IS NULL OR a.alertTime <= :end) " +
           "ORDER BY a.alertTime DESC")
    Page<AlertRecord> findByConditions(
            @Param("domain") String domain,
            @Param("level") AlertLevel level,
            @Param("ruleName") String ruleName,
            @Param("start") Instant start,
            @Param("end") Instant end,
            Pageable pageable);
    
    /**
     * 统计各级别告警数量
     */
    @Query("SELECT a.level, COUNT(a) FROM AlertRecord a " +
           "WHERE a.alertTime >= :since " +
           "GROUP BY a.level")
    List<Object[]> countByLevelSince(@Param("since") Instant since);
    
    /**
     * 统计各域告警数量
     */
    @Query("SELECT a.domain, COUNT(a) FROM AlertRecord a " +
           "WHERE a.alertTime >= :since " +
           "GROUP BY a.domain")
    List<Object[]> countByDomainSince(@Param("since") Instant since);
    
    /**
     * 统计各规则告警数量
     */
    @Query("SELECT a.ruleName, COUNT(a) FROM AlertRecord a " +
           "WHERE a.alertTime >= :since " +
           "GROUP BY a.ruleName " +
           "ORDER BY COUNT(a) DESC")
    List<Object[]> countByRuleSince(@Param("since") Instant since);
}


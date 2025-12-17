package com.twilight.quality.api;

import com.twilight.quality.aggregator.WindowManager;
import com.twilight.quality.domain.entity.AlertRecord;
import com.twilight.quality.domain.enums.AlertLevel;
import com.twilight.quality.repository.AlertRecordRepository;
import com.twilight.quality.rule.RuleRegistry;
import com.twilight.quality.sink.QualityMetricSink;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据质量API控制器
 */
@RestController
@RequestMapping("/api/quality")
public class QualityController {
    
    private final RuleRegistry ruleRegistry;
    private final WindowManager windowManager;
    private final QualityMetricSink metricSink;
    private final AlertRecordRepository alertRecordRepository;
    
    public QualityController(RuleRegistry ruleRegistry,
                              WindowManager windowManager,
                              QualityMetricSink metricSink,
                              AlertRecordRepository alertRecordRepository) {
        this.ruleRegistry = ruleRegistry;
        this.windowManager = windowManager;
        this.metricSink = metricSink;
        this.alertRecordRepository = alertRecordRepository;
    }
    
    /**
     * 获取系统状态概览
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new HashMap<>();
        
        // 规则统计
        status.put("rules", ruleRegistry.getStats());
        
        // 窗口统计
        status.put("windows", windowManager.getStats());
        
        // 指标落库统计
        Map<String, Object> metricStats = new HashMap<>();
        metricStats.put("savedCount", metricSink.getSavedCount());
        metricStats.put("errorCount", metricSink.getErrorCount());
        metricStats.put("queueSize", metricSink.getQueueSize());
        status.put("metrics", metricStats);
        
        return ResponseEntity.ok(status);
    }
    
    /**
     * 获取所有规则列表
     */
    @GetMapping("/rules")
    public ResponseEntity<List<Map<String, Object>>> getRules() {
        List<Map<String, Object>> rules = ruleRegistry.getAllRules().stream()
                .map(rule -> {
                    Map<String, Object> info = new HashMap<>();
                    info.put("name", rule.getRuleName());
                    info.put("description", rule.getDescription());
                    info.put("dimension", rule.getDimension());
                    info.put("domains", rule.getSupportedDomains());
                    info.put("enabled", rule.isEnabled());
                    info.put("isAggregate", rule.isAggregateRule());
                    return info;
                })
                .toList();
        
        return ResponseEntity.ok(rules);
    }
    
    /**
     * 查询告警记录
     */
    @GetMapping("/alerts")
    public ResponseEntity<Page<AlertRecord>> getAlerts(
            @RequestParam(required = false) String domain,
            @RequestParam(required = false) String level,
            @RequestParam(required = false) String ruleName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        
        Pageable pageable = PageRequest.of(page, size);
        
        AlertLevel alertLevel = level != null ? AlertLevel.fromString(level) : null;
        Instant startInstant = start != null ? start.atZone(ZoneId.systemDefault()).toInstant() : null;
        Instant endInstant = end != null ? end.atZone(ZoneId.systemDefault()).toInstant() : null;
        
        Page<AlertRecord> alerts = alertRecordRepository.findByConditions(
                domain, alertLevel, ruleName, startInstant, endInstant, pageable);
        
        return ResponseEntity.ok(alerts);
    }
    
    /**
     * 获取告警统计
     */
    @GetMapping("/alerts/stats")
    public ResponseEntity<Map<String, Object>> getAlertStats(
            @RequestParam(defaultValue = "24") int hours) {
        
        Instant since = Instant.now().minusSeconds(hours * 3600L);
        
        Map<String, Object> stats = new HashMap<>();
        
        // 按级别统计
        List<Object[]> byLevel = alertRecordRepository.countByLevelSince(since);
        Map<String, Long> levelCounts = new HashMap<>();
        for (Object[] row : byLevel) {
            levelCounts.put(row[0].toString(), (Long) row[1]);
        }
        stats.put("byLevel", levelCounts);
        
        // 按域统计
        List<Object[]> byDomain = alertRecordRepository.countByDomainSince(since);
        Map<String, Long> domainCounts = new HashMap<>();
        for (Object[] row : byDomain) {
            if (row[0] != null) {
                domainCounts.put(row[0].toString(), (Long) row[1]);
            }
        }
        stats.put("byDomain", domainCounts);
        
        // 按规则统计（Top 10）
        List<Object[]> byRule = alertRecordRepository.countByRuleSince(since);
        Map<String, Long> ruleCounts = new HashMap<>();
        int count = 0;
        for (Object[] row : byRule) {
            if (count >= 10) break;
            ruleCounts.put(row[0].toString(), (Long) row[1]);
            count++;
        }
        stats.put("byRule", ruleCounts);
        
        stats.put("timeRange", Map.of(
                "since", since.toString(),
                "hours", hours
        ));
        
        return ResponseEntity.ok(stats);
    }
    
    /**
     * 获取单个告警详情
     */
    @GetMapping("/alerts/{alertId}")
    public ResponseEntity<AlertRecord> getAlert(@PathVariable String alertId) {
        return alertRecordRepository.findById(alertId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * 健康检查端点
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("timestamp", Instant.now().toString());
        health.put("activeRules", ruleRegistry.getEnabledRules().size());
        health.put("activeWindows", windowManager.getActiveWindowCount());
        return ResponseEntity.ok(health);
    }
}


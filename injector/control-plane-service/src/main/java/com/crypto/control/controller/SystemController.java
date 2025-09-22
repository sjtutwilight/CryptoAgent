package com.crypto.control.controller;

import com.crypto.control.service.RateLimiterService;
import com.crypto.control.service.TaskSchedulerService;
import com.crypto.control.service.TimerProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 系统监控和管理REST API控制器
 */
@Slf4j
@RestController
@RequestMapping("/system")
public class SystemController {
    
    @Autowired
    private TaskSchedulerService taskSchedulerService;
    
    @Autowired
    private TimerProducerService timerProducerService;
    
    @Autowired
    private RateLimiterService rateLimiterService;
    
    /**
     * 健康检查
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("timestamp", LocalDateTime.now());
        health.put("service", "control-plane-service");
        health.put("version", "1.0.0");
        
        try {
            // 检查数据库连接
            TaskSchedulerService.TaskStatistics stats = taskSchedulerService.getTaskStatistics();
            health.put("database", "UP");
            health.put("totalTasks", stats.totalTasks());
            
            // 检查定时器状态
            TimerProducerService.TimerStatus timerStatus = timerProducerService.getTimerStatus();
            health.put("timer", timerStatus.enabled() ? "UP" : "DOWN");
            
        } catch (Exception e) {
            log.error("健康检查异常: {}", e.getMessage(), e);
            health.put("status", "DOWN");
            health.put("error", e.getMessage());
            return ResponseEntity.status(503).body(health);
        }
        
        return ResponseEntity.ok(health);
    }
    
    /**
     * 获取系统统计信息
     */
    @GetMapping("/stats")
    public ResponseEntity<SystemStats> getSystemStats() {
        try {
            TaskSchedulerService.TaskStatistics taskStats = taskSchedulerService.getTaskStatistics();
            TimerProducerService.TimerStatus timerStatus = timerProducerService.getTimerStatus();
            
            SystemStats stats = new SystemStats(
                    taskStats,
                    timerStatus,
                    LocalDateTime.now()
            );
            
            return ResponseEntity.ok(stats);
            
        } catch (Exception e) {
            log.error("获取系统统计信息失败: {}", e.getMessage(), e);
            return ResponseEntity.status(500).build();
        }
    }
    
    /**
     * 获取限流状态
     */
    @GetMapping("/rate-limit/{key}")
    public ResponseEntity<RateLimiterService.RateLimitStatus> getRateLimitStatus(
            @PathVariable String key) {
        try {
            RateLimiterService.RateLimitStatus status = rateLimiterService.getCurrentStatus(key);
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            log.error("获取限流状态失败: key={}, error={}", key, e.getMessage(), e);
            return ResponseEntity.status(500).build();
        }
    }
    
    /**
     * 清理过期记录
     */
    @PostMapping("/cleanup")
    public ResponseEntity<Map<String, Object>> cleanup() {
        try {
            rateLimiterService.cleanupExpiredRecords();
            
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("message", "清理完成");
            result.put("timestamp", LocalDateTime.now());
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            log.error("清理操作失败: {}", e.getMessage(), e);
            
            Map<String, Object> result = new HashMap<>();
            result.put("success", false);
            result.put("message", "清理失败: " + e.getMessage());
            result.put("timestamp", LocalDateTime.now());
            
            return ResponseEntity.status(500).body(result);
        }
    }
    
    /**
     * 系统统计信息
     */
    public record SystemStats(
            TaskSchedulerService.TaskStatistics taskStatistics,
            TimerProducerService.TimerStatus timerStatus,
            LocalDateTime timestamp
    ) {}
}
package com.crypto.control.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 任务实体类
 * 对应数据库中的tasks表
 */
@Entity
@Table(name = "tasks")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Task {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "task_id", unique = true, nullable = false, length = 64)
    private String taskId;
    
    @Column(name = "data_source_id", nullable = false, length = 64)
    private String dataSourceId;
    
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    @Builder.Default
    private TaskStatus status = TaskStatus.PENDING;
    
    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;
    
    @Column(name = "max_retry_count", nullable = false)
    @Builder.Default
    private Integer maxRetryCount = 3;

    @Column(name = "task_type", length = 64)
    private String taskType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb")
    private Map<String, Object> payload;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata", columnDefinition = "jsonb")
    private Map<String, Object> metadata;
    
    @Column(name = "scheduled_time", nullable = false)
    private LocalDateTime scheduledTime;    
    @Column(name = "started_at")
    private LocalDateTime startedAt;
    
    @Column(name = "completed_at")
    private LocalDateTime completedAt;
    
    @Column(name = "status_code")
    private Integer statusCode;
    
    @Column(name = "message", columnDefinition = "TEXT")
    private String message;
    
    @Column(name = "duration_ms")
    private Long durationMs;
    
    @Column(name = "data_size")
    private Integer dataSize;
    
    @Column(name = "cost", nullable = false)
    @Builder.Default
    private Integer cost = 1;
    
    @Column(name = "priority", nullable = false)
    @Builder.Default
    private Integer priority = 5;
    
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
    
    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }
    
    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }    
    /**
     * 任务状态枚举
     * 对应项目文档中定义的5种状态
     */
    public enum TaskStatus {
        PENDING,    // 提交任务后
        PROCESSING, // 处理中
        SUCCESS,    // 任务成功
        RETRY,      // tasks.status为可重试任务
        FAILED      // 不可重试任务
    }
    
    /**
     * 检查任务是否可以重试
     */
    public boolean canRetry() {
        return this.retryCount < this.maxRetryCount && 
               (this.status == TaskStatus.FAILED || this.status == TaskStatus.RETRY);
    }
    
    /**
     * 增加重试次数
     */
    public void incrementRetryCount() {
        this.retryCount++;
    }
    
    /**
     * 检查任务是否已完成（成功或最终失败）
     */
    public boolean isCompleted() {
        return this.status == TaskStatus.SUCCESS || 
               (this.status == TaskStatus.FAILED && !canRetry());
    }
}

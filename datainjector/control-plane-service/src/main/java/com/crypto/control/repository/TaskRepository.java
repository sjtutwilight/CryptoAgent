package com.crypto.control.repository;

import com.crypto.control.model.Task;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * 任务Repository接口
 */
@Repository
public interface TaskRepository extends JpaRepository<Task, Long> {
    
    /**
     * 根据任务ID查找任务
     */
    Optional<Task> findByTaskId(String taskId);
    
    /**
     * 根据状态查找任务
     */
    List<Task> findByStatus(Task.TaskStatus status);
    
    /**
     * 根据状态和调度时间查找任务（用于定时器）
     */
    @Query("SELECT t FROM  Task t WHERE t.status IN :statuses AND t.scheduledTime <= :scheduledTime ORDER BY t.priority ASC, t.scheduledTime ASC")
    List<Task> findTasksToExecute(@Param("statuses") List<Task.TaskStatus> statuses, 
                                  @Param("scheduledTime") LocalDateTime scheduledTime, 
                                  Pageable pageable);
    
    /**
     * 根据状态统计任务数量
     */
    long countByStatus(Task.TaskStatus status);

    /**
     * 根据状态和调度时间查找任务（分页）
     */
    Page<Task> findByStatusAndScheduledTimeBefore(Task.TaskStatus status, 
                                                  LocalDateTime scheduledTime, 
                                                  Pageable pageable);

    /**
     * 批量更新任务状态
     */
    @Modifying
    @Query("UPDATE Task t SET t.status = :status WHERE t.taskId IN :taskIds")
    void updateStatusByTaskIds(@Param("taskIds") List<String> taskIds, 
                              @Param("status") Task.TaskStatus status);

    /**
     * 查找数据源的任务
     */
    List<Task> findByDataSourceIdAndStatus(String dataSourceId, Task.TaskStatus status);

    /**
     * 查找重试次数少于最大重试次数的失败任务
     */
    @Query("SELECT t FROM Task t WHERE t.status = :status AND t.retryCount < t.maxRetryCount AND t.scheduledTime <= :now")
    List<Task> findRetryableTasks(@Param("status") Task.TaskStatus status, 
                                 @Param("now") LocalDateTime now,
                                 Pageable pageable);
}
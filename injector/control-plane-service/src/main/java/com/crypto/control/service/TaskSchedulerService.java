package com.crypto.control.service;

import com.crypto.control.dto.TaskCreateRequest;
import com.crypto.control.dto.TaskResponse;
import com.crypto.control.config.DataSourceConfigProperties;
import com.crypto.control.model.Task;
import com.crypto.control.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.crypto.control.config.DataSourceConfigProperties;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 任务调度服务
 * 负责任务的创建、调度、状态更新等核心逻辑
 */
@Slf4j
@Service
public class TaskSchedulerService {
    
    @Autowired
    private TaskRepository taskRepository;
    
    @Autowired
    private DataSourceConfigProperties dataSourceConfigProperties;
    @Value("${app.task.max-retry-count:3}")
    private int defaultMaxRetryCount;

    /**
     * 创建新任务
     */
    @Transactional
    public TaskResponse createTask(TaskCreateRequest request) {
        // 验证数据源配置
        DataSourceConfigProperties.DataSourceConfig dataSourceConfig = dataSourceConfigProperties.getDataSourceConfig(request.getDataSourceId());
        
        // 生成任务ID
        String taskId = generateTaskId();
        
        // 确定调度时间
        LocalDateTime scheduledTime = request.getScheduledTime() != null 
            ? request.getScheduledTime() 
            : LocalDateTime.now();
        
        // 确定最大重试次数
        int maxRetryCount = request.getMaxRetryCount() != null 
            ? request.getMaxRetryCount() 
            : dataSourceConfig.getMaxRetryCount();
        
        // 创建任务实体
        Task task = Task.builder()
                .taskId(taskId)
                .dataSourceId(request.getDataSourceId())
                .baseUrl(request.getBaseUrl()) 
                .method(request.getMethod())
                .params(request.getParams())
                .headers(request.getHeaders())
                .apiKey(request.getApiKey())
                .scheduledTime(scheduledTime)
                .status(Task.TaskStatus.PENDING)
                .retryCount(0)
                .cost(request.getCost())
                .priority(request.getPriority())
                .maxRetryCount(maxRetryCount)
                .dataSourceId(request.getDataSourceId())
                .build();
        
        // 保存任务
        task = taskRepository.save(task);
        
        log.info("任务创建成功: taskId={}, dataSourceId={}, scheduledTime={}", 
                taskId, request.getDataSourceId(), scheduledTime);
        
        return TaskResponse.fromTask(task);
    }    
    /**
     * 标记任务为处理中
     */
    @Transactional
    public void markTaskAsProcessing(String taskId) {
        Optional<Task> taskOpt = taskRepository.findByTaskId(taskId);
        if (taskOpt.isEmpty()) {
            log.warn("任务不存在: taskId={}", taskId);
            return;
        }
        
        Task task = taskOpt.get();
        if (task.getStatus() != Task.TaskStatus.PENDING && task.getStatus() != Task.TaskStatus.RETRY) {
            log.warn("任务状态不正确，无法标记为处理中: taskId={}, status={}", 
                    taskId, task.getStatus());
            return;
        }
        
        task.setStatus(Task.TaskStatus.PROCESSING);
        task.setStartedAt(LocalDateTime.now());
        taskRepository.save(task);
        
        log.debug("任务标记为处理中: taskId={}", taskId);
    }
    
    /**
     * 根据任务ID查询任务
     */
    @Transactional(readOnly = true)
    public Optional<TaskResponse> getTask(String taskId) {
        return taskRepository.findByTaskId(taskId)
                .map(TaskResponse::fromTask);
    }
    
    /**
     * 根据任务ID查询任务实体
     */
    @Transactional(readOnly = true)
    public Optional<Task> getTaskByTaskId(String taskId) {
        return taskRepository.findByTaskId(taskId);
    }    
    /**
     * 更新任务
     */
    @Transactional
    public Task updateTask(Task task) {
        task.setUpdatedAt(LocalDateTime.now());
        return taskRepository.save(task);
    }
    
    /**
     * 获取待调度的任务列表
     */
    @Transactional(readOnly = true)
    public List<Task> getPendingTasks(LocalDateTime beforeTime, int limit) {
        Pageable pageable = PageRequest.of(0, limit);
        Page<Task> tasks = taskRepository.findByStatusAndScheduledTimeBefore(
                Task.TaskStatus.PENDING, beforeTime, pageable);
        return tasks.getContent();
    }
    
    /**
     * 获取数据源配置
     */
    private DataSourceConfigProperties.DataSourceConfig getDataSourceConfig(String dataSourceId) {
        return dataSourceConfigProperties.getDataSourceConfig(dataSourceId);
    }
    
    /**
     * 生成任务ID
     */
    private String generateTaskId() {
        return "task-" + UUID.randomUUID().toString().replace("-", "");
    }
    
    /**
     * 任务统计信息
     */
    public record TaskStatistics(
            long totalTasks,
            long pendingTasks,
            long processingTasks,
            long successTasks,
            long failedTasks,
            long retryTasks
    ) {}    
    /**
     * 获取任务统计信息
     */
    @Transactional(readOnly = true)
    public TaskStatistics getTaskStatistics() {
        long totalTasks = taskRepository.count();
        long pendingTasks = taskRepository.countByStatus(Task.TaskStatus.PENDING);
        long processingTasks = taskRepository.countByStatus(Task.TaskStatus.PROCESSING);
        long successTasks = taskRepository.countByStatus(Task.TaskStatus.SUCCESS);
        long failedTasks = taskRepository.countByStatus(Task.TaskStatus.FAILED);
        long retryTasks = taskRepository.countByStatus(Task.TaskStatus.RETRY);
        
        return new TaskStatistics(totalTasks, pendingTasks, processingTasks, 
                                successTasks, failedTasks, retryTasks);
    }
}
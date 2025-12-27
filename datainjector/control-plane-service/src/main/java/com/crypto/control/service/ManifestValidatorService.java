package com.crypto.control.service;

import com.crypto.control.dto.ManifestReport;
import com.crypto.control.model.Task;
import com.crypto.control.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Manifest 校验服务
 * 负责接收 Worker 上报的 Manifest 并进行完整性校验
 */
@Slf4j
@Service
public class ManifestValidatorService {
    
    @Autowired
    private TaskRepository taskRepository;
    
    /**
     * 校验并处理 Manifest 上报
     * 
     * @param manifest Manifest 报告
     * @return 校验结果
     */
    @Transactional
    public ValidationResult validateAndProcess(ManifestReport manifest) {
        log.info("开始校验 Manifest: taskId={}, status={}, totalRecords={}, totalFiles={}", 
                manifest.getTaskId(), manifest.getStatus(), manifest.getTotalRecords(), manifest.getTotalFiles());
        
        // 1. 查找关联任务
        Optional<Task> taskOpt = taskRepository.findByTaskId(manifest.getTaskId());
        if (taskOpt.isEmpty()) {
            log.warn("任务不存在: taskId={}", manifest.getTaskId());
            return ValidationResult.failure("任务不存在");
        }
        
        Task task = taskOpt.get();
        
        // 2. 基础校验
        ValidationResult basicResult = validateBasic(manifest);
        if (!basicResult.isValid()) {
            updateTaskStatus(task, Task.TaskStatus.FAILED, basicResult.getMessage());
            return basicResult;
        }
        
        // 3. 文件完整性校验（可选，需要访问文件系统）
        if (manifest.getFiles() != null && !manifest.getFiles().isEmpty()) {
            ValidationResult fileResult = validateFiles(manifest);
            if (!fileResult.isValid()) {
                updateTaskStatus(task, Task.TaskStatus.FAILED, fileResult.getMessage());
                return fileResult;
            }
        }
        
        // 4. 记录数校验
        ValidationResult recordResult = validateRecordCount(manifest);
        if (!recordResult.isValid()) {
            updateTaskStatus(task, Task.TaskStatus.FAILED, recordResult.getMessage());
            return recordResult;
        }
        
        // 5. 更新任务状态为成功
        updateTaskStatus(task, Task.TaskStatus.SUCCESS, "Manifest 校验通过");
        
        log.info("Manifest 校验通过: taskId={}, totalRecords={}", manifest.getTaskId(), manifest.getTotalRecords());
        return ValidationResult.success("校验通过");
    }
    
    /**
     * 基础校验
     */
    private ValidationResult validateBasic(ManifestReport manifest) {
        // 检查状态
        if (!"completed".equals(manifest.getStatus())) {
            return ValidationResult.failure("任务状态非 completed: " + manifest.getStatus());
        }
        
        // 检查必要字段
        if (manifest.getTotalRecords() == null || manifest.getTotalRecords() < 0) {
            return ValidationResult.failure("totalRecords 无效");
        }
        
        if (manifest.getTotalFiles() == null || manifest.getTotalFiles() < 0) {
            return ValidationResult.failure("totalFiles 无效");
        }
        
        return ValidationResult.success("基础校验通过");
    }
    
    /**
     * 文件完整性校验
     * 注意：此方法需要访问文件系统，仅在控制面与 Worker 共享存储时可用
     */
    private ValidationResult validateFiles(ManifestReport manifest) {
        // 获取输出目录（从任务 payload 中提取）
        // 这里简化处理，实际应从任务配置中获取
        String outputDir = extractOutputDir(manifest);
        if (outputDir == null) {
            log.warn("无法获取输出目录，跳过文件校验");
            return ValidationResult.success("跳过文件校验");
        }
        
        for (ManifestReport.FileEntry fileEntry : manifest.getFiles()) {
            String filePath = outputDir + File.separator + fileEntry.getFilename();
            File file = new File(filePath);
            
            // 检查文件是否存在
            if (!file.exists()) {
                return ValidationResult.failure("文件不存在: " + fileEntry.getFilename());
            }
            
            // 检查文件大小
            if (file.length() != fileEntry.getSizeBytes()) {
                return ValidationResult.failure(String.format("文件大小不匹配: %s (expected=%d, actual=%d)", 
                        fileEntry.getFilename(), fileEntry.getSizeBytes(), file.length()));
            }
            
            // 校验 checksum（如果提供）
            if (fileEntry.getChecksum() != null && !fileEntry.getChecksum().isEmpty()) {
                try {
                    String actualChecksum = calculateChecksum(filePath, "MD5");
                    if (!actualChecksum.equalsIgnoreCase(fileEntry.getChecksum())) {
                        return ValidationResult.failure(String.format("文件校验和不匹配: %s", fileEntry.getFilename()));
                    }
                } catch (Exception e) {
                    log.warn("计算校验和失败: {}", fileEntry.getFilename(), e);
                }
            }
        }
        
        return ValidationResult.success("文件校验通过");
    }
    
    /**
     * 记录数校验
     */
    private ValidationResult validateRecordCount(ManifestReport manifest) {
        if (manifest.getFiles() == null || manifest.getFiles().isEmpty()) {
            return ValidationResult.success("无文件需要校验");
        }
        
        // 计算所有文件的记录数总和
        long sumRecords = manifest.getFiles().stream()
                .mapToLong(f -> f.getRecordCount() != null ? f.getRecordCount() : 0)
                .sum();
        
        // 与声明的总记录数对比
        if (sumRecords != manifest.getTotalRecords()) {
            return ValidationResult.failure(String.format("记录数不匹配: sum(files)=%d, totalRecords=%d", 
                    sumRecords, manifest.getTotalRecords()));
        }
        
        return ValidationResult.success("记录数校验通过");
    }
    
    /**
     * 更新任务状态
     */
    private void updateTaskStatus(Task task, Task.TaskStatus status, String message) {
        task.setStatus(status);
        task.setMessage(message);
        task.setCompletedAt(LocalDateTime.now());
        task.setUpdatedAt(LocalDateTime.now());
        taskRepository.save(task);
        
        log.info("任务状态已更新: taskId={}, status={}, message={}", 
                task.getTaskId(), status, message);
    }
    
    /**
     * 从 Manifest 中提取输出目录
     */
    private String extractOutputDir(ManifestReport manifest) {
        // 简化实现：从自定义字段中提取
        if (manifest.getCustomFields() != null) {
            Object outputDir = manifest.getCustomFields().get("output_dir");
            if (outputDir != null) {
                return outputDir.toString();
            }
        }
        return null;
    }
    
    /**
     * 计算文件校验和
     */
    private String calculateChecksum(String filePath, String algorithm) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        try (FileInputStream fis = new FileInputStream(filePath)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
        }
        
        byte[] hashBytes = digest.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    /**
     * 校验结果
     */
    public static class ValidationResult {
        private final boolean valid;
        private final String message;
        
        private ValidationResult(boolean valid, String message) {
            this.valid = valid;
            this.message = message;
        }
        
        public static ValidationResult success(String message) {
            return new ValidationResult(true, message);
        }
        
        public static ValidationResult failure(String message) {
            return new ValidationResult(false, message);
        }
        
        public boolean isValid() {
            return valid;
        }
        
        public String getMessage() {
            return message;
        }
    }
}



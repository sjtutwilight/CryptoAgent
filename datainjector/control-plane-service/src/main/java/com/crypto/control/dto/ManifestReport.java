package com.crypto.control.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Manifest 上报 DTO
 * Worker 完成批量拉取后上报的数据完整性清单
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ManifestReport {
    
    private String version;         // Manifest 版本
    private String taskId;          // 关联的任务 ID
    private String dataSource;      // 数据源标识
    private LocalDateTime createdAt;   // 任务开始时间
    private LocalDateTime completedAt; // 任务完成时间
    private String status;          // completed/partial/failed
    
    // 统计信息
    private Long totalRecords;      // 总记录数
    private Integer totalFiles;     // 总文件数
    private List<FileEntry> files;  // 文件列表
    
    // 自定义字段
    private Map<String, Object> customFields;
    
    /**
     * 文件条目
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class FileEntry {
        private String filename;     // 文件名
        private Long recordCount;    // 记录数
        private Long sizeBytes;      // 文件大小（字节）
        private String checksum;     // 校验和
    }
}



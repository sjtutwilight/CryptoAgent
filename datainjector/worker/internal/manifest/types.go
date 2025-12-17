package manifest

import "time"

// Manifest 数据完整性清单，用于离线数据批量拉取场景
type Manifest struct {
	Version     string    `json:"version"`      // Manifest 版本，如 "1.0"
	TaskID      string    `json:"task_id"`      // 关联的任务 ID
	DataSource  string    `json:"data_source"`  // 数据源标识
	CreatedAt   time.Time `json:"created_at"`   // 任务开始时间
	CompletedAt time.Time `json:"completed_at"` // 任务完成时间
	Status      string    `json:"status"`       // completed/partial/failed

	// 通用统计信息
	TotalRecords int64       `json:"total_records"` // 总记录数
	TotalFiles   int         `json:"total_files"`   // 总文件数
	Files        []FileEntry `json:"files"`         // 文件列表

	// 可扩展的自定义字段（配置驱动）
	CustomFields map[string]any `json:"custom_fields,omitempty"`
}

// FileEntry 单个数据文件的元信息
type FileEntry struct {
	Filename    string `json:"filename"`     // 文件名
	RecordCount int64  `json:"record_count"` // 该文件的记录数
	SizeBytes   int64  `json:"size_bytes"`   // 文件大小（字节）
	Checksum    string `json:"checksum,omitempty"` // 文件校验和（MD5/SHA256）
}

// CursorState 游标状态，用于断点续传
type CursorState struct {
	TaskID                string    `json:"task_id"`                  // 任务 ID
	NextOffset            string    `json:"next_offset"`              // 下一页游标
	CurrentFileIndex      int       `json:"current_file_index"`       // 当前文件索引
	RecordsInCurrentFile  int64     `json:"records_in_current_file"`  // 当前文件已写入记录数
	TotalRecords          int64     `json:"total_records"`            // 累计总记录数
	FilesWritten          []string  `json:"files_written"`            // 已写入的文件列表
	LastUpdated           time.Time `json:"last_updated"`             // 最后更新时间
}


package types

import "encoding/json"

// ConnectionPoolConfig 连接池配置
type ConnectionPoolConfig struct {
	Enabled          bool `json:"enabled"`            // 是否启用
	MaxIdleConns     int  `json:"max_idle_conns"`     // 最大空闲连接数
	MaxConnsPerHost  int  `json:"max_conns_per_host"` // 每个主机最大连接数
}

// DataMessage 数据消息（输出到Kafka）
type DataMessage struct {
	WorkerID     string                 `json:"worker_id"`      // Worker ID
	TaskID       string                 `json:"task_id"`        // 任务ID
	DataSourceID string                 `json:"data_source_id"` // 数据源ID
	Timestamp    int64                  `json:"timestamp"`      // 时间戳
	Sequence     interface{}            `json:"sequence"`       // 序列号（通用类型）
	Data         json.RawMessage        `json:"data"`           // 原始数据
	Metadata     map[string]interface{} `json:"metadata"`       // 元数据
}

// FailureReport 失败报告（上报控制面）
type FailureReport struct {
	WorkerID     string                 `json:"worker_id"`      // Worker ID
	TaskID       string                 `json:"task_id"`        // 任务ID
	DataSourceID string                 `json:"data_source_id"` // 数据源ID
	Timestamp    int64                  `json:"timestamp"`      // 失败时间戳
	ErrorType    string                 `json:"error_type"`     // 错误类型
	ErrorMessage string                 `json:"error_message"`  // 错误信息
	RetryCount   int                    `json:"retry_count"`    // 已重试次数
	LastSequence interface{}            `json:"last_sequence"`  // 最后成功的序列号
}

// SequenceReport 序列号报告（用于缺失检测）
type SequenceReport struct {
	WorkerID     string      `json:"worker_id"`      // Worker ID
	TaskID       string      `json:"task_id"`        // 任务ID
	DataSourceID string      `json:"data_source_id"` // 数据源ID
	Timestamp    int64       `json:"timestamp"`      // 报告时间戳
	Sequences    []interface{} `json:"sequences"`    // 已接收的序列号列表
}

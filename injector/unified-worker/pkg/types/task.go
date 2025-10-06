package types

import "time"

// TaskConfig 任务配置
type TaskConfig struct {
	TaskID       string       `json:"task_id"`        // 任务ID
	TaskType     TaskType     `json:"task_type"`      // 任务类型
	Protocol     ProtocolType `json:"protocol"`       // 协议类型
	DataSourceID string       `json:"data_source_id"` // 数据源ID

	// 数据源元数据（由控制面下发）
	DataSource *DataSourceMetadata `json:"data_source"` // 数据源完整元数据

	// 协议配置（废弃，使用data_source.endpoint）
	ProtocolConfig map[string]interface{} `json:"protocol_config,omitempty"`

	// 任务特定配置
	TaskSpecificConfig TaskSpecificConfig `json:"task_specific_config"`

	// 输出配置
	OutputTopic string `json:"output_topic"` // Kafka输出topic

	// 序列号配置
	SequenceField string `json:"sequence_field"` // 序列号字段名（如"number"表示block.number）

	// 是否上报到控制面
	ReportToControlPlane bool `json:"report_to_control_plane"`

	// 重试配置
	RetryConfig RetryConfig `json:"retry_config"`
}

// TaskSpecificConfig 任务特定配置
type TaskSpecificConfig struct {
	// 长连接订阅配置
	Subscription *SubscriptionConfig `json:"subscription,omitempty"`

	// 轮询配置
	Polling *PollingConfig `json:"polling,omitempty"`

	// 命令式配置
	OneTime *OneTimeConfig `json:"one_time,omitempty"`
}

// SubscriptionConfig 订阅配置
type SubscriptionConfig struct {
	Topics []string `json:"topics"` // 订阅主题（如["newHeads"]）
}

// PollingConfig 轮询配置
type PollingConfig struct {
	Interval time.Duration          `json:"interval"` // 轮询间隔
	Method   string                 `json:"method"`   // 方法名（如"eth_getBlockByNumber"）
	Params   map[string]interface{} `json:"params"`   // 方法参数
}

// OneTimeConfig 单次调用配置
type OneTimeConfig struct {
	Method string                 `json:"method"` // 方法名
	Params map[string]interface{} `json:"params"` // 方法参数
}

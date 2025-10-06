package config

// Config 全局配置
type Config struct {
	Worker WorkerConfig `mapstructure:"worker"`
	Kafka  KafkaConfig  `mapstructure:"kafka"`
}

// WorkerConfig Worker配置
type WorkerConfig struct {
	ID    string       `mapstructure:"id"`    // Worker ID
	Roles []RoleConfig `mapstructure:"roles"` // 角色配置列表
}

// RoleConfig 角色配置
type RoleConfig struct {
	RoleID               string                 `mapstructure:"role_id"`                 // 角色ID
	Protocol             string                 `mapstructure:"protocol"`                // 协议类型
	TaskType             string                 `mapstructure:"task_type"`               // 任务类型
	DataSourceID         string                 `mapstructure:"data_source_id"`          // 数据源ID
	ProtocolConfig       map[string]interface{} `mapstructure:"protocol_config"`         // 协议配置
	TaskConfig           map[string]interface{} `mapstructure:"task_config"`             // 任务配置
	RuntimeConfig        RuntimeConfigYAML      `mapstructure:"runtime"`                 // Runtime配置
	OutputTopic          string                 `mapstructure:"output_topic"`            // 输出topic
	SequenceField        string                 `mapstructure:"sequence_field"`          // 序列号字段
	ReportToControlPlane bool                   `mapstructure:"report_to_control_plane"` // 是否上报控制面
}

// RuntimeConfigYAML Runtime配置（YAML格式）
type RuntimeConfigYAML struct {
	Connection     ConnectionConfigYAML     `mapstructure:"connection"`
	Reconnect      ReconnectConfigYAML      `mapstructure:"reconnect"`
	Heartbeat      HeartbeatConfigYAML      `mapstructure:"heartbeat"`
	RateLimit      RateLimitConfigYAML      `mapstructure:"rate_limit"`
	ConnectionPool ConnectionPoolConfigYAML `mapstructure:"connection_pool"`
}

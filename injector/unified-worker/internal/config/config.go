package config

// Config 全局配置
type Config struct {
	Roles []RoleConfig `mapstructure:"roles"` // 角色配置列表
}

// RoleConfig 角色配置
type RoleConfig struct {
	RoleID           string                 `mapstructure:"role_id"`            // 角色ID
	Protocol         string                 `mapstructure:"protocol"`           // 协议类型
	TaskType         string                 `mapstructure:"task_type"`          // 任务类型
	PollingInterval  int                    `mapstructure:"polling_interval"`   // 轮询间隔(秒)
	Method           string                 `mapstructure:"method"`             // 方法名(polling任务用)
	DataSourceConfig map[string]interface{} `mapstructure:"data_source_config"` // 数据源配置
	Resource         ResourceConfigYAML     `mapstructure:"resource"`           // 资源配置
	Handler          map[string]interface{} `mapstructure:"handler"`            // 处理器配置
}

// ResourceConfigYAML 资源配置（只包含存在的配置项）
type ResourceConfigYAML struct {
	Connection     *ConnectionConfigYAML     `mapstructure:"connection"`      // 连接配置(可选)
	Reconnect      *ReconnectConfigYAML      `mapstructure:"reconnect"`       // 重连配置(可选)
	Heartbeat      *HeartbeatConfigYAML      `mapstructure:"heartbeat"`       // 心跳配置(可选)
	RateLimit      *RateLimitConfigYAML      `mapstructure:"rate_limit"`      // 限流配置(可选)
	ConnectionPool *ConnectionPoolConfigYAML `mapstructure:"connection_pool"` // 连接池配置(可选)
}

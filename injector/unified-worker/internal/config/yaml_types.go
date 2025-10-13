package config

// ConnectionConfigYAML 连接配置
type ConnectionConfigYAML struct {
	TimeoutSeconds      int `mapstructure:"timeout_seconds"`
	ReadTimeoutSeconds  int `mapstructure:"read_timeout_seconds"`
	WriteTimeoutSeconds int `mapstructure:"write_timeout_seconds"`
}

// ReconnectConfigYAML 重连配置
type ReconnectConfigYAML struct {
	MaxRetries         int `mapstructure:"max_retries"`
	BackoffBaseSeconds int `mapstructure:"backoff_base_seconds"`
	BackoffMaxSeconds  int `mapstructure:"backoff_max_seconds"`
}

// HeartbeatConfigYAML 心跳配置
type HeartbeatConfigYAML struct {
	IntervalSeconds int `mapstructure:"interval_seconds"`
	TimeoutSeconds  int `mapstructure:"timeout_seconds"`
}

// RateLimitConfigYAML 限流配置
type RateLimitConfigYAML struct {
	Capacity   int     `mapstructure:"capacity"`
	RefillRate float64 `mapstructure:"refill_rate"`
	RefillUnit string  `mapstructure:"refill_unit"`
}

// ConnectionPoolConfigYAML 连接池配置
type ConnectionPoolConfigYAML struct {
	MaxIdleConns    int `mapstructure:"max_idle_conns"`
	MaxConnsPerHost int `mapstructure:"max_conns_per_host"`
}

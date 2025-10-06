package types

import "time"

// RetryConfig 重试配置
type RetryConfig struct {
	MaxRetries   int           `json:"max_retries"`   // 最大重试次数（本地）
	BackoffBase  time.Duration `json:"backoff_base"`  // 退避基数
	BackoffMax   time.Duration `json:"backoff_max"`   // 最大退避时间
}

// RuntimeConfig Runtime能力配置
type RuntimeConfig struct {
	// 连接管理配置
	Connection ConnectionConfig `json:"connection"`
	
	// 重连管理配置
	Reconnect ReconnectConfig `json:"reconnect"`
	
	// 心跳管理配置
	Heartbeat HeartbeatConfig `json:"heartbeat"`
	
	// 限流配置
	RateLimit RateLimitConfig `json:"rate_limit"`
	
	// 连接池配置
	ConnectionPool ConnectionPoolConfig `json:"connection_pool"`
}

// ConnectionConfig 连接配置
type ConnectionConfig struct {
	Timeout         time.Duration `json:"timeout"`          // 连接超时
	ReadTimeout     time.Duration `json:"read_timeout"`     // 读超时
	WriteTimeout    time.Duration `json:"write_timeout"`    // 写超时
}

// ReconnectConfig 重连配置
type ReconnectConfig struct {
	Enabled     bool          `json:"enabled"`      // 是否启用
	MaxRetries  int           `json:"max_retries"`  // 最大重试次数（-1表示无限）
	BackoffBase time.Duration `json:"backoff_base"` // 退避基数
	BackoffMax  time.Duration `json:"backoff_max"`  // 最大退避时间
}

// HeartbeatConfig 心跳配置
type HeartbeatConfig struct {
	Enabled  bool          `json:"enabled"`  // 是否启用
	Interval time.Duration `json:"interval"` // 心跳间隔
	Timeout  time.Duration `json:"timeout"`  // 心跳超时
}

// RateLimitConfig 限流配置
type RateLimitConfig struct {
	Enabled     bool    `json:"enabled"`      // 是否启用
	Capacity    int     `json:"capacity"`     // 令牌桶容量
	RefillRate  float64 `json:"refill_rate"`  // 每秒补充速率
	RefillUnit  string  `json:"refill_unit"`  // 补充单位（second/minute）
}

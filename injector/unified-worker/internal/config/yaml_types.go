package config

// ConnectionConfigYAML 连接配置
type ConnectionConfigYAML struct {
	TimeoutSeconds      int `mapstructure:"timeout_seconds"`
	ReadTimeoutSeconds  int `mapstructure:"read_timeout_seconds"`
	WriteTimeoutSeconds int `mapstructure:"write_timeout_seconds"`
}

// ReconnectConfigYAML 重连配置
type ReconnectConfigYAML struct {
	Enabled            bool `mapstructure:"enabled"`
	MaxRetries         int  `mapstructure:"max_retries"`
	BackoffBaseSeconds int  `mapstructure:"backoff_base_seconds"`
	BackoffMaxSeconds  int  `mapstructure:"backoff_max_seconds"`
}

// HeartbeatConfigYAML 心跳配置
type HeartbeatConfigYAML struct {
	Enabled         bool `mapstructure:"enabled"`
	IntervalSeconds int  `mapstructure:"interval_seconds"`
	TimeoutSeconds  int  `mapstructure:"timeout_seconds"`
}

// RateLimitConfigYAML 限流配置
type RateLimitConfigYAML struct {
	Enabled    bool    `mapstructure:"enabled"`
	Capacity   int     `mapstructure:"capacity"`
	RefillRate float64 `mapstructure:"refill_rate"`
	RefillUnit string  `mapstructure:"refill_unit"`
}

// ConnectionPoolConfigYAML 连接池配置
type ConnectionPoolConfigYAML struct {
	Enabled         bool `mapstructure:"enabled"`
	MaxIdleConns    int  `mapstructure:"max_idle_conns"`
	MaxConnsPerHost int  `mapstructure:"max_conns_per_host"`
}

// KafkaConfig Kafka配置
type KafkaConfig struct {
	Brokers       []string      `mapstructure:"brokers"`
	ConsumerGroup string        `mapstructure:"consumer_group"`
	TaskTopic     string        `mapstructure:"task_topic"`      // 消费任务的topic
	FailureTopic  string        `mapstructure:"failure_topic"`   // 失败上报topic
	SequenceTopic string        `mapstructure:"sequence_topic"`  // 序列号上报topic
	Producer      ProducerConfig `mapstructure:"producer"`
}

// ProducerConfig Kafka生产者配置
type ProducerConfig struct {
	Compression string `mapstructure:"compression"`
	BatchSize   int    `mapstructure:"batch_size"`
}

package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
	"unified-worker/pkg/types"
)

// Load 加载配置文件
func Load(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	// 设置默认值
	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	return &config, nil
}

// setDefaults 设置默认值
func setDefaults(v *viper.Viper) {
	// Worker默认值
	v.SetDefault("worker.id", "worker-1")

	// Kafka默认值
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.consumer_group", "unified-worker-group")
	v.SetDefault("kafka.task_topic", "worker.tasks")
	v.SetDefault("kafka.failure_topic", "worker.failures")
	v.SetDefault("kafka.sequence_topic", "worker.sequences")
	v.SetDefault("kafka.producer.compression", "snappy")
	v.SetDefault("kafka.producer.batch_size", 16384)
}

// ConvertToRuntimeConfig 将YAML配置转换为Runtime配置
func ConvertToRuntimeConfig(yaml RuntimeConfigYAML) types.RuntimeConfig {
	return types.RuntimeConfig{
		Connection: types.ConnectionConfig{
			Timeout:      time.Duration(yaml.Connection.TimeoutSeconds) * time.Second,
			ReadTimeout:  time.Duration(yaml.Connection.ReadTimeoutSeconds) * time.Second,
			WriteTimeout: time.Duration(yaml.Connection.WriteTimeoutSeconds) * time.Second,
		},
		Reconnect: types.ReconnectConfig{
			Enabled:     yaml.Reconnect.Enabled,
			MaxRetries:  yaml.Reconnect.MaxRetries,
			BackoffBase: time.Duration(yaml.Reconnect.BackoffBaseSeconds) * time.Second,
			BackoffMax:  time.Duration(yaml.Reconnect.BackoffMaxSeconds) * time.Second,
		},
		Heartbeat: types.HeartbeatConfig{
			Enabled:  yaml.Heartbeat.Enabled,
			Interval: time.Duration(yaml.Heartbeat.IntervalSeconds) * time.Second,
			Timeout:  time.Duration(yaml.Heartbeat.TimeoutSeconds) * time.Second,
		},
		RateLimit: types.RateLimitConfig{
			Enabled:    yaml.RateLimit.Enabled,
			Capacity:   yaml.RateLimit.Capacity,
			RefillRate: yaml.RateLimit.RefillRate,
			RefillUnit: yaml.RateLimit.RefillUnit,
		},
		ConnectionPool: types.ConnectionPoolConfig{
			Enabled:         yaml.ConnectionPool.Enabled,
			MaxIdleConns:    yaml.ConnectionPool.MaxIdleConns,
			MaxConnsPerHost: yaml.ConnectionPool.MaxConnsPerHost,
		},
	}
}

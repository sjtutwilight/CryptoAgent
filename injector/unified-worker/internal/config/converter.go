package config

import (
	"time"

	"unified-worker/pkg/types"
)

// ConvertResourceConfig 转换资源配置为RuntimeConfig
// 只有在YAML中存在的配置项才会被转换(指针非nil),未配置的默认不启用
func ConvertResourceConfig(yamlConfig ResourceConfigYAML) types.RuntimeConfig {
	runtimeCfg := types.RuntimeConfig{}

	// 连接配置(如果存在)
	if yamlConfig.Connection != nil {
		runtimeCfg.Connection = types.ConnectionConfig{
			Timeout:      time.Duration(yamlConfig.Connection.TimeoutSeconds) * time.Second,
			ReadTimeout:  time.Duration(yamlConfig.Connection.ReadTimeoutSeconds) * time.Second,
			WriteTimeout: time.Duration(yamlConfig.Connection.WriteTimeoutSeconds) * time.Second,
		}
	}

	// 重连配置(如果存在则启用)
	if yamlConfig.Reconnect != nil {
		runtimeCfg.Reconnect = types.ReconnectConfig{
			Enabled:     true, // 只要配置了就启用
			MaxRetries:  yamlConfig.Reconnect.MaxRetries,
			BackoffBase: yamlConfig.Reconnect.BackoffBaseSeconds,
			BackoffMax:  yamlConfig.Reconnect.BackoffMaxSeconds,
		}
	}

	// 心跳配置(如果存在则启用)
	if yamlConfig.Heartbeat != nil {
		runtimeCfg.Heartbeat = types.HeartbeatConfig{
			Enabled:  true, // 只要配置了就启用
			Interval: yamlConfig.Heartbeat.IntervalSeconds,
			Timeout:  yamlConfig.Heartbeat.TimeoutSeconds,
		}
	}

	// 限流配置(如果存在则启用)
	if yamlConfig.RateLimit != nil {
		runtimeCfg.RateLimit = types.RateLimitConfig{
			Enabled:    true, // 只要配置了就启用
			Capacity:   yamlConfig.RateLimit.Capacity,
			RefillRate: yamlConfig.RateLimit.RefillRate,
			RefillUnit: yamlConfig.RateLimit.RefillUnit,
		}
	}

	// 连接池配置(如果存在则启用)
	if yamlConfig.ConnectionPool != nil {
		runtimeCfg.ConnectionPool = types.ConnectionPoolConfig{
			Enabled:         true, // 只要配置了就启用
			MaxIdleConns:    yamlConfig.ConnectionPool.MaxIdleConns,
			MaxConnsPerHost: yamlConfig.ConnectionPool.MaxConnsPerHost,
		}
	}

	return runtimeCfg
}

// ConvertToRoleV2Config 将YAML配置转换为role_v2.RoleConfig
func ConvertToRoleV2Config(yamlConfig RoleConfig) map[string]interface{} {
	config := make(map[string]interface{})

	// 基本信息
	config["role_id"] = yamlConfig.RoleID
	config["protocol"] = yamlConfig.Protocol
	config["task_type"] = yamlConfig.TaskType

	// 协议配置(从data_source_config提取)
	if yamlConfig.DataSourceConfig != nil {
		config["protocol_config"] = yamlConfig.DataSourceConfig
	}

	// 任务配置
	taskConfig := make(map[string]interface{})
	if yamlConfig.PollingInterval > 0 {
		taskConfig["polling_interval"] = yamlConfig.PollingInterval
	}
	if yamlConfig.Method != "" {
		taskConfig["method"] = yamlConfig.Method
	}
	config["task_config"] = taskConfig

	// 资源配置(转换为map供ResourceManager使用)
	resourceConfig := make(map[string]interface{})

	if yamlConfig.Resource.RateLimit != nil {
		resourceConfig["rate_limit"] = map[string]interface{}{
			"capacity":    yamlConfig.Resource.RateLimit.Capacity,
			"refill_rate": yamlConfig.Resource.RateLimit.RefillRate,
			"refill_unit": yamlConfig.Resource.RateLimit.RefillUnit,
		}
	}

	if yamlConfig.Resource.ConnectionPool != nil {
		resourceConfig["connection_pool"] = map[string]interface{}{
			"max_idle_conns":     yamlConfig.Resource.ConnectionPool.MaxIdleConns,
			"max_conns_per_host": yamlConfig.Resource.ConnectionPool.MaxConnsPerHost,
		}
	}

	if yamlConfig.Resource.Reconnect != nil {
		resourceConfig["reconnect"] = map[string]interface{}{
			"max_retries":          yamlConfig.Resource.Reconnect.MaxRetries,
			"backoff_base_seconds": yamlConfig.Resource.Reconnect.BackoffBaseSeconds,
			"backoff_max_seconds":  yamlConfig.Resource.Reconnect.BackoffMaxSeconds,
		}
	}

	if yamlConfig.Resource.Heartbeat != nil {
		resourceConfig["heartbeat"] = map[string]interface{}{
			"interval_seconds": yamlConfig.Resource.Heartbeat.IntervalSeconds,
			"timeout_seconds":  yamlConfig.Resource.Heartbeat.TimeoutSeconds,
		}
	}

	if yamlConfig.Resource.Connection != nil {
		resourceConfig["connection"] = map[string]interface{}{
			"timeout_seconds":       yamlConfig.Resource.Connection.TimeoutSeconds,
			"read_timeout_seconds":  yamlConfig.Resource.Connection.ReadTimeoutSeconds,
			"write_timeout_seconds": yamlConfig.Resource.Connection.WriteTimeoutSeconds,
		}
	}

	config["resources_config"] = resourceConfig

	// 处理器配置
	if yamlConfig.Handler != nil {
		config["handlers_config"] = convertHandlerConfig(yamlConfig.Handler)
	}

	return config
}

// convertHandlerConfig 转换处理器配置
func convertHandlerConfig(handlerMap map[string]interface{}) []map[string]interface{} {
	handlers := []map[string]interface{}{}

	// 解析器
	if parser, ok := handlerMap["parser"].(string); ok {
		handlers = append(handlers, map[string]interface{}{
			"type": "parser",
			"name": parser,
		})
	}

	// 序列检测器和补数据(reorder_detach)
	if reorderDetach, ok := handlerMap["reorder_detach"].(map[string]interface{}); ok {
		// 1. 序列检测器
		if sequenceField, ok := reorderDetach["sequence_field"].(string); ok {
			handlers = append(handlers, map[string]interface{}{
				"type": "sequence",
				"config": map[string]interface{}{
					"field": sequenceField,
				},
			})
		}

		// 2. 缺失检测器(如果有refill_config就创建)
		if refillConfig, ok := reorderDetach["refill_config"].(map[string]interface{}); ok {
			handlers = append(handlers, map[string]interface{}{
				"type": "missing_detector",
				"config": map[string]interface{}{
					"sequence_field": reorderDetach["sequence_field"],
					"threshold":      5,   // 默认值
					"max_gap":        100, // 默认值
				},
			})

			// 3. 补数据器
			handlers = append(handlers, map[string]interface{}{
				"type": "refiller",
				"config": map[string]interface{}{
					"method":  "websocket",
					"url":     refillConfig["url"],
					"max_gap": 100, // 默认值
				},
			})
		}
	}

	// Kafka Sink
	if kafkaSink, ok := handlerMap["kafka_sink"].(map[string]interface{}); ok {
		handlers = append(handlers, map[string]interface{}{
			"type":   "kafka_sink",
			"config": kafkaSink,
		})
	}

	return handlers
}

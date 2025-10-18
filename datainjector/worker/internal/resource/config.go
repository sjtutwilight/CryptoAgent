package resource

import (
	"fmt"
	"time"
)

// ParseRateLimitConfig 从配置中解析限流配置
func ParseRateLimitConfig(cfg map[string]any) (RateLimitConfig, error) {
	config := RateLimitConfig{
		Capacity:   100,  // 默认容量
		RefillRate: 10.0, // 默认每秒10个令牌
	}

	if cfg == nil {
		return config, nil
	}

	if capacity, ok := cfg["capacity"]; ok {
		switch v := capacity.(type) {
		case int:
			config.Capacity = v
		case int64:
			config.Capacity = int(v)
		case float64:
			config.Capacity = int(v)
		default:
			return config, fmt.Errorf("invalid capacity type: %T", capacity)
		}
	}

	if refillRate, ok := cfg["refill_rate"]; ok {
		switch v := refillRate.(type) {
		case int:
			config.RefillRate = float64(v)
		case int64:
			config.RefillRate = float64(v)
		case float64:
			config.RefillRate = v
		default:
			return config, fmt.Errorf("invalid refill_rate type: %T", refillRate)
		}
	}

	return config, nil
}

// ParseHTTPPoolConfig 从配置中解析 HTTP 连接池配置
func ParseHTTPPoolConfig(cfg map[string]any) (HTTPPoolConfig, error) {
	config := HTTPPoolConfig{
		MaxConns:       100,
		MaxIdle:        10,
		IdleTimeout:    90 * time.Second,
		ConnectTimeout: 10 * time.Second,
		RequestTimeout: 30 * time.Second,
	}

	if cfg == nil {
		return config, nil
	}

	if maxConns, ok := cfg["max_conns"]; ok {
		switch v := maxConns.(type) {
		case int:
			config.MaxConns = v
		case int64:
			config.MaxConns = int(v)
		case float64:
			config.MaxConns = int(v)
		}
	}

	if maxIdle, ok := cfg["max_idle"]; ok {
		switch v := maxIdle.(type) {
		case int:
			config.MaxIdle = v
		case int64:
			config.MaxIdle = int(v)
		case float64:
			config.MaxIdle = int(v)
		}
	}

	if idleTimeout, ok := cfg["idle_timeout_ms"]; ok {
		switch v := idleTimeout.(type) {
		case int:
			config.IdleTimeout = time.Duration(v) * time.Millisecond
		case int64:
			config.IdleTimeout = time.Duration(v) * time.Millisecond
		case float64:
			config.IdleTimeout = time.Duration(v) * time.Millisecond
		}
	}

	if connTimeout, ok := cfg["connect_timeout_ms"]; ok {
		switch v := connTimeout.(type) {
		case int:
			config.ConnectTimeout = time.Duration(v) * time.Millisecond
		case int64:
			config.ConnectTimeout = time.Duration(v) * time.Millisecond
		case float64:
			config.ConnectTimeout = time.Duration(v) * time.Millisecond
		}
	}

	if reqTimeout, ok := cfg["request_timeout_ms"]; ok {
		switch v := reqTimeout.(type) {
		case int:
			config.RequestTimeout = time.Duration(v) * time.Millisecond
		case int64:
			config.RequestTimeout = time.Duration(v) * time.Millisecond
		case float64:
			config.RequestTimeout = time.Duration(v) * time.Millisecond
		}
	}

	return config, nil
}


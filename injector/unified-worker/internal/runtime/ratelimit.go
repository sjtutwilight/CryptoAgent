package runtime

import (
	"context"
	"sync"
	"time"

	"unified-worker/pkg/types"
)

// RateLimiter 限流器接口
type RateLimiter interface {
	// Allow 检查是否允许通过
	Allow(ctx context.Context) bool
	// Close 关闭限流器
	Close() error
}

// TokenBucketRateLimiter 令牌桶限流器
type TokenBucketRateLimiter struct {
	capacity   int       // 桶容量
	tokens     float64   // 当前令牌数
	refillRate float64   // 每秒补充速率
	lastRefill time.Time // 上次补充时间
	mu         sync.Mutex
}

// NewTokenBucketRateLimiter 创建令牌桶限流器
func NewTokenBucketRateLimiter(config types.RateLimitConfig) (*TokenBucketRateLimiter, error) {
	if !config.Enabled {
		return nil, nil // 未启用限流
	}

	// 根据refill_unit计算每秒补充速率
	refillRate := config.RefillRate
	if config.RefillUnit == "minute" {
		refillRate = config.RefillRate / 60.0
	}

	return &TokenBucketRateLimiter{
		capacity:   config.Capacity,
		tokens:     float64(config.Capacity), // 初始满桶
		refillRate: refillRate,
		lastRefill: time.Now(),
	}, nil
}

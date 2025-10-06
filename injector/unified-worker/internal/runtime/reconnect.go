package runtime

import (
	"context"
	"math"
	"time"

	"unified-worker/pkg/types"
)

// ReconnectManager 重连管理器
type ReconnectManager struct {
	config      types.ReconnectConfig
	retryCount  int
	lastAttempt time.Time
}

// NewReconnectManager 创建重连管理器
func NewReconnectManager(config types.ReconnectConfig) *ReconnectManager {
	return &ReconnectManager{
		config:      config,
		retryCount:  0,
		lastAttempt: time.Now(),
	}
}

// ShouldRetry 判断是否应该重试
func (r *ReconnectManager) ShouldRetry() bool {
	if !r.config.Enabled {
		return false
	}

	// 检查是否达到最大重试次数（-1表示无限重试）
	if r.config.MaxRetries >= 0 && r.retryCount >= r.config.MaxRetries {
		return false
	}

	return true
}

// GetBackoffDuration 获取退避时间
func (r *ReconnectManager) GetBackoffDuration() time.Duration {
	// 指数退避: base * 2^retryCount
	backoff := float64(r.config.BackoffBase) * math.Pow(2, float64(r.retryCount))

	// 限制最大退避时间
	if backoff > float64(r.config.BackoffMax) {
		backoff = float64(r.config.BackoffMax)
	}

	return time.Duration(backoff)
}

// Wait 等待退避时间
func (r *ReconnectManager) Wait(ctx context.Context) error {
	duration := r.GetBackoffDuration()

	select {
	case <-time.After(duration):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

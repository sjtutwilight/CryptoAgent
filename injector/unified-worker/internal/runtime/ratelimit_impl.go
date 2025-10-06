package runtime

import (
	"context"
	"time"
)

// Allow 检查是否允许通过（消耗1个令牌）
func (r *TokenBucketRateLimiter) Allow(ctx context.Context) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// 补充令牌
	now := time.Now()
	elapsed := now.Sub(r.lastRefill).Seconds()
	r.tokens += elapsed * r.refillRate
	
	// 限制最大令牌数
	if r.tokens > float64(r.capacity) {
		r.tokens = float64(r.capacity)
	}
	
	r.lastRefill = now
	
	// 检查是否有足够令牌
	if r.tokens >= 1.0 {
		r.tokens -= 1.0
		return true
	}
	
	return false
}

// Close 关闭限流器
func (r *TokenBucketRateLimiter) Close() error {
	return nil // 无需特殊清理
}

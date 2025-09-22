package ratelimit

import (
	"sync"
	"time"
)

// TokenBucket 令牌桶接口
type TokenBucket interface {
	// TakeToken 尝试获取指定数量的令牌，返回是否成功
	TakeToken(tokens int) bool
	// WaitForToken 等待直到可以获取指定数量的令牌
	WaitForToken(tokens int) time.Duration
	// AvailableTokens 获取当前可用令牌数
	AvailableTokens() int
	// Capacity 获取桶容量
	Capacity() int
}

// tokenBucket 令牌桶实现
type tokenBucket struct {
	mu             sync.Mutex
	capacity       int           // 桶容量
	tokens         float64       // 当前令牌数（支持小数）
	refillRate     float64       // 每次补充的令牌数（支持小数）
	refillInterval time.Duration // 补充间隔
	lastRefill     time.Time     // 最后补充时间
	stopChan       chan struct{} // 停止信号
	refillTicker   *time.Ticker  // 补充定时器
}

// NewTokenBucket 创建新的令牌桶
func NewTokenBucket(capacity int, refillRate float64, refillInterval time.Duration) TokenBucket {
	tb := &tokenBucket{
		capacity:       capacity,
		tokens:         float64(capacity), // 初始化时桶是满的
		refillRate:     refillRate,
		refillInterval: refillInterval,
		lastRefill:     time.Now(),
		stopChan:       make(chan struct{}),
	}
	
	// 启动令牌补充定时器
	tb.startRefillTimer()
	
	return tb
}

// startRefillTimer 启动令牌补充定时器
func (tb *tokenBucket) startRefillTimer() {
	tb.refillTicker = time.NewTicker(tb.refillInterval)
	
	go func() {
		for {
			select {
			case <-tb.refillTicker.C:
				tb.refillTokens()
			case <-tb.stopChan:
				tb.refillTicker.Stop()
				return
			}
		}
	}()
}

// refillTokens 补充令牌
func (tb *tokenBucket) refillTokens() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	
	// 计算应该补充的令牌数
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill)
	
	// 计算在elapsed时间内应该补充多少令牌
	intervals := float64(elapsed) / float64(tb.refillInterval)
	tokensToAdd := intervals * tb.refillRate
	
	if tokensToAdd > 0 {
		tb.tokens = minFloat(float64(tb.capacity), tb.tokens+tokensToAdd)
		tb.lastRefill = now
	}
}

// TakeToken 尝试获取指定数量的令牌
func (tb *tokenBucket) TakeToken(tokens int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	
	// 先尝试补充令牌
	tb.refillTokensLocked()
	
	if tb.tokens >= float64(tokens) {
		tb.tokens -= float64(tokens)
		return true
	}
	
	return false
}

// WaitForToken 等待直到可以获取指定数量的令牌
func (tb *tokenBucket) WaitForToken(tokens int) time.Duration {
	start := time.Now()
	
	for {
		if tb.TakeToken(tokens) {
			return time.Since(start)
		}
		
		// 计算需要等待的时间
		tb.mu.Lock()
		neededTokens := float64(tokens) - tb.tokens
		waitTime := time.Duration(neededTokens/tb.refillRate) * tb.refillInterval
		tb.mu.Unlock()
		
		// 等待一小段时间后重试
		if waitTime > 0 {
			sleepTime := waitTime
			if sleepTime > tb.refillInterval {
				sleepTime = tb.refillInterval
			}
			time.Sleep(sleepTime)
		} else {
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// AvailableTokens 获取当前可用令牌数
func (tb *tokenBucket) AvailableTokens() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	
	tb.refillTokensLocked()
	return int(tb.tokens)
}

// Capacity 获取桶容量
func (tb *tokenBucket) Capacity() int {
	return tb.capacity
}

// refillTokensLocked 在已加锁状态下补充令牌
func (tb *tokenBucket) refillTokensLocked() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill)
	
	intervals := float64(elapsed) / float64(tb.refillInterval)
	tokensToAdd := intervals * tb.refillRate
	
	if tokensToAdd > 0 {
		tb.tokens = minFloat(float64(tb.capacity), tb.tokens+tokensToAdd)
		tb.lastRefill = now
	}
}

// Stop 停止令牌桶
func (tb *tokenBucket) Stop() {
	close(tb.stopChan)
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// minFloat 返回两个浮点数中的较小值
func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// TokenBucketManager 令牌桶管理器
type TokenBucketManager struct {
	buckets    map[string]TokenBucket
	mu         sync.RWMutex
	refillInterval time.Duration
}

// NewTokenBucketManager 创建令牌桶管理器
func NewTokenBucketManager(refillInterval time.Duration) *TokenBucketManager {
	return &TokenBucketManager{
		buckets:    make(map[string]TokenBucket),
		refillInterval: refillInterval,
	}
}

// GetBucket 获取或创建指定数据源的令牌桶
func (tbm *TokenBucketManager) GetBucket(dataSourceID string, capacity int, refillRate float64) TokenBucket {
	tbm.mu.Lock()
	defer tbm.mu.Unlock()
	
	if bucket, exists := tbm.buckets[dataSourceID]; exists {
		return bucket
	}
	
	// 创建新的令牌桶
	bucket := NewTokenBucket(capacity, refillRate, tbm.refillInterval)
	tbm.buckets[dataSourceID] = bucket
	
	return bucket
}

// RemoveBucket 移除指定数据源的令牌桶
func (tbm *TokenBucketManager) RemoveBucket(dataSourceID string) {
	tbm.mu.Lock()
	defer tbm.mu.Unlock()
	
	if bucket, exists := tbm.buckets[dataSourceID]; exists {
		if tb, ok := bucket.(*tokenBucket); ok {
			tb.Stop()
		}
		delete(tbm.buckets, dataSourceID)
	}
}

// GetAllBuckets 获取所有令牌桶的状态
func (tbm *TokenBucketManager) GetAllBuckets() map[string]map[string]int {
	tbm.mu.RLock()
	defer tbm.mu.RUnlock()
	
	result := make(map[string]map[string]int)
	
	for dataSourceID, bucket := range tbm.buckets {
		result[dataSourceID] = map[string]int{
			"available": bucket.AvailableTokens(),
			"capacity":  bucket.Capacity(),
		}
	}
	
	return result
}
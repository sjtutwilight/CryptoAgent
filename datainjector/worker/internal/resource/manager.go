package resource

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Manager 全局资源管理器（单例）
type Manager struct {
	mu           sync.RWMutex
	rateLimiters map[string]*RateLimiter
	httpPools    map[string]*HTTPPool
}

var (
	globalManager     *Manager
	globalManagerOnce sync.Once
)

// GetManager 获取全局资源管理器实例
func GetManager() *Manager {
	globalManagerOnce.Do(func() {
		globalManager = &Manager{
			rateLimiters: make(map[string]*RateLimiter),
			httpPools:    make(map[string]*HTTPPool),
		}
	})
	return globalManager
}

// GetOrCreateRateLimiter 获取或创建限流器
func (m *Manager) GetOrCreateRateLimiter(datasourceID string, config RateLimitConfig) *RateLimiter {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limiter, ok := m.rateLimiters[datasourceID]; ok {
		return limiter
	}

	limiter := NewRateLimiter(datasourceID, config)
	m.rateLimiters[datasourceID] = limiter
	log.Printf("[ResourceManager] Created rate limiter for datasource '%s': capacity=%d, refill_rate=%.2f/s",
		datasourceID, config.Capacity, config.RefillRate)
	return limiter
}

// GetRateLimiter 获取已存在的限流器
func (m *Manager) GetRateLimiter(datasourceID string) (*RateLimiter, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	limiter, ok := m.rateLimiters[datasourceID]
	return limiter, ok
}

// GetOrCreateHTTPPool 获取或创建 HTTP 连接池
func (m *Manager) GetOrCreateHTTPPool(datasourceID string, config HTTPPoolConfig) *HTTPPool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if pool, ok := m.httpPools[datasourceID]; ok {
		return pool
	}

	pool := NewHTTPPool(datasourceID, config)
	m.httpPools[datasourceID] = pool
	log.Printf("[ResourceManager] Created HTTP pool for datasource '%s': max_conns=%d, max_idle=%d",
		datasourceID, config.MaxConns, config.MaxIdle)
	return pool
}

// GetHTTPPool 获取已存在的 HTTP 连接池
func (m *Manager) GetHTTPPool(datasourceID string) (*HTTPPool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pool, ok := m.httpPools[datasourceID]
	return pool, ok
}

// RateLimitConfig 限流配置
type RateLimitConfig struct {
	Capacity   int     // 令牌桶容量
	RefillRate float64 // 每秒补充速率
}

// RateLimiter 限流器（基于令牌桶算法）
type RateLimiter struct {
	datasourceID string
	limiter      *rate.Limiter
	capacity     int
	refillRate   float64
}

// NewRateLimiter 创建限流器
func NewRateLimiter(datasourceID string, config RateLimitConfig) *RateLimiter {
	if config.Capacity <= 0 {
		config.Capacity = 100 // 默认容量
	}
	if config.RefillRate <= 0 {
		config.RefillRate = 10 // 默认每秒 10 个令牌
	}

	return &RateLimiter{
		datasourceID: datasourceID,
		limiter:      rate.NewLimiter(rate.Limit(config.RefillRate), config.Capacity),
		capacity:     config.Capacity,
		refillRate:   config.RefillRate,
	}
}

// Wait 等待获取令牌（阻塞式）
func (r *RateLimiter) Wait(ctx context.Context) error {
	return r.limiter.Wait(ctx)
}

// Allow 尝试获取令牌（非阻塞）
func (r *RateLimiter) Allow() bool {
	return r.limiter.Allow()
}

// Reserve 预留令牌
func (r *RateLimiter) Reserve() *rate.Reservation {
	return r.limiter.Reserve()
}

// Stats 获取限流器统计信息
func (r *RateLimiter) Stats() map[string]interface{} {
	return map[string]interface{}{
		"datasource_id": r.datasourceID,
		"capacity":      r.capacity,
		"refill_rate":   r.refillRate,
		"tokens":        r.limiter.Tokens(),
	}
}

// HTTPPoolConfig HTTP 连接池配置
type HTTPPoolConfig struct {
	MaxConns       int           // 最大连接数
	MaxIdle        int           // 最大空闲连接数
	IdleTimeout    time.Duration // 空闲连接超时
	ConnectTimeout time.Duration // 连接超时
	RequestTimeout time.Duration // 请求超时
}

// HTTPPool HTTP 连接池
type HTTPPool struct {
	datasourceID  string
	config        HTTPPoolConfig
	activeConns   int
	mu            sync.Mutex
	connSemaphore chan struct{} // 用于限制并发连接数
}

// NewHTTPPool 创建 HTTP 连接池
func NewHTTPPool(datasourceID string, config HTTPPoolConfig) *HTTPPool {
	// 设置默认值
	if config.MaxConns <= 0 {
		config.MaxConns = 100
	}
	if config.MaxIdle <= 0 {
		config.MaxIdle = 10
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = 90 * time.Second
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 10 * time.Second
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 30 * time.Second
	}

	return &HTTPPool{
		datasourceID:  datasourceID,
		config:        config,
		connSemaphore: make(chan struct{}, config.MaxConns),
	}
}

// AcquireConn 获取连接许可（阻塞式）
func (p *HTTPPool) AcquireConn(ctx context.Context) error {
	select {
	case p.connSemaphore <- struct{}{}:
		p.mu.Lock()
		p.activeConns++
		p.mu.Unlock()
		return nil
	case <-ctx.Done():
		return fmt.Errorf("acquire connection timeout for datasource '%s': %w", p.datasourceID, ctx.Err())
	}
}

// ReleaseConn 释放连接许可
func (p *HTTPPool) ReleaseConn() {
	select {
	case <-p.connSemaphore:
		p.mu.Lock()
		p.activeConns--
		p.mu.Unlock()
	default:
		log.Printf("[HTTPPool] WARNING: release connection called but semaphore empty for datasource '%s'", p.datasourceID)
	}
}

// Stats 获取连接池统计信息
func (p *HTTPPool) Stats() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return map[string]interface{}{
		"datasource_id": p.datasourceID,
		"max_conns":     p.config.MaxConns,
		"active_conns":  p.activeConns,
		"available":     p.config.MaxConns - p.activeConns,
	}
}

// GetConfig 获取配置
func (p *HTTPPool) GetConfig() HTTPPoolConfig {
	return p.config
}

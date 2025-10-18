package fault

import (
	"math/rand"
	"mock-service/internal/config"
	"net/http"
	"sync"
	"time"
)

// FaultType 故障类型
type FaultType int

const (
	// HTTP故障类型
	HTTPFailure FaultType = iota
	HTTPRateLimit
	HTTPServerError

	// WebSocket故障类型
	WebSocketDisconnection
	WebSocketDataLoss
	WebSocketHeartbeatAnomaly

	// 链重组故障类型
	ChainReorg
)

// FaultInjector 故障注入器
type FaultInjector struct {
	mu     sync.RWMutex
	config *config.Config
	stats  map[FaultType]int64
	rng    *rand.Rand
}

// NewFaultInjector 创建新的故障注入器
func NewFaultInjector(cfg *config.Config) *FaultInjector {
	return &FaultInjector{
		config: cfg,
		stats:  make(map[FaultType]int64),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ShouldInjectHTTPFault 判断是否应该注入HTTP故障
func (f *FaultInjector) ShouldInjectHTTPFault() (bool, int) {
	if !f.config.Fault.HTTP.Enabled {
		return false, 0
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// 检查是否触发429限流错误
	if f.rng.Float64() < f.config.Fault.HTTP.RateLimitProbability {
		f.stats[HTTPRateLimit]++
		return true, http.StatusTooManyRequests
	}

	// 检查是否触发5xx服务器错误
	if f.rng.Float64() < f.config.Fault.HTTP.ServerErrorProbability {
		f.stats[HTTPServerError]++
		// 随机返回500或503错误
		if f.rng.Float64() < 0.5 {
			return true, http.StatusInternalServerError
		}
		return true, http.StatusServiceUnavailable
	}

	// 检查是否触发一般HTTP失败
	if f.rng.Float64() < f.config.Fault.HTTP.FailureProbability {
		f.stats[HTTPFailure]++
		// 随机返回400或404错误
		if f.rng.Float64() < 0.5 {
			return true, http.StatusBadRequest
		}
		return true, http.StatusNotFound
	}

	return false, 0
}

// ShouldInjectWebSocketDisconnection 判断是否应该注入WebSocket断开连接故障
func (f *FaultInjector) ShouldInjectWebSocketDisconnection() bool {
	if !f.config.Fault.WebSocket.Enabled {
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.rng.Float64() < f.config.Fault.WebSocket.DisconnectionProbability {
		f.stats[WebSocketDisconnection]++
		return true
	}

	return false
}

// ShouldInjectWebSocketDataLoss 判断是否应该注入WebSocket数据丢失故障
func (f *FaultInjector) ShouldInjectWebSocketDataLoss() bool {
	if !f.config.Fault.WebSocket.Enabled {
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.rng.Float64() < f.config.Fault.WebSocket.DataLossProbability {
		f.stats[WebSocketDataLoss]++
		return true
	}

	return false
}

// ShouldInjectWebSocketHeartbeatAnomaly 判断是否应该注入WebSocket心跳异常故障
func (f *FaultInjector) ShouldInjectWebSocketHeartbeatAnomaly() bool {
	if !f.config.Fault.WebSocket.Enabled {
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.rng.Float64() < f.config.Fault.WebSocket.HeartbeatAnomalyProbability {
		f.stats[WebSocketHeartbeatAnomaly]++
		return true
	}

	return false
}

// ShouldInjectChainReorg 判断是否应该注入链重组故障
// 返回值: (是否注入, 回退区块数)
func (f *FaultInjector) ShouldInjectChainReorg() (bool, int) {
	if !f.config.Fault.ChainReorg.Enabled {
		return false, 0
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.rng.Float64() < f.config.Fault.ChainReorg.Probability {
		f.stats[ChainReorg]++

		// 在配置的范围内随机选择回退区块数
		minDepth := f.config.Fault.ChainReorg.ReorgDepthMin
		maxDepth := f.config.Fault.ChainReorg.ReorgDepthMax

		if minDepth <= 0 {
			minDepth = 1
		}
		if maxDepth < minDepth {
			maxDepth = minDepth
		}

		reorgDepth := minDepth
		if maxDepth > minDepth {
			reorgDepth = minDepth + f.rng.Intn(maxDepth-minDepth+1)
		}

		return true, reorgDepth
	}

	return false, 0
}

// GetStats 获取故障注入统计信息
func (f *FaultInjector) GetStats() map[string]int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make(map[string]int64)
	result["http_failure"] = f.stats[HTTPFailure]
	result["http_rate_limit"] = f.stats[HTTPRateLimit]
	result["http_server_error"] = f.stats[HTTPServerError]
	result["websocket_disconnection"] = f.stats[WebSocketDisconnection]
	result["websocket_data_loss"] = f.stats[WebSocketDataLoss]
	result["websocket_heartbeat_anomaly"] = f.stats[WebSocketHeartbeatAnomaly]
	result["chain_reorg"] = f.stats[ChainReorg]

	return result
}

// ResetStats 重置故障注入统计信息
func (f *FaultInjector) ResetStats() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stats = make(map[FaultType]int64)
}

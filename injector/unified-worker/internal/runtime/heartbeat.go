package runtime

import (
	"context"
	"time"
	
	"unified-worker/pkg/types"
)

// HeartbeatManager 心跳管理器
type HeartbeatManager struct {
	config      types.HeartbeatConfig
	ticker      *time.Ticker
	lastPong    time.Time
	stopChan    chan struct{}
}

// NewHeartbeatManager 创建心跳管理器
func NewHeartbeatManager(config types.HeartbeatConfig) *HeartbeatManager {
	return &HeartbeatManager{
		config:   config,
		lastPong: time.Now(),
		stopChan: make(chan struct{}),
	}
}

// Start 启动心跳
func (h *HeartbeatManager) Start(ctx context.Context, pingFunc func() error, onTimeout func()) {
	if !h.config.Enabled {
		return
	}
	
	h.ticker = time.NewTicker(h.config.Interval)
	
	go func() {
		for {
			select {
			case <-h.ticker.C:
				// 发送ping
				if err := pingFunc(); err != nil {
					// ping失败，调用超时处理
					if onTimeout != nil {
						onTimeout()
					}
					return
				}
				
				// 检查pong超时
				if time.Since(h.lastPong) > h.config.Timeout {
					if onTimeout != nil {
						onTimeout()
					}
					return
				}
				
			case <-h.stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

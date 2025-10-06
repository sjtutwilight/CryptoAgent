package runtime

import "time"

// OnPong 接收到pong时调用
func (h *HeartbeatManager) OnPong() {
	h.lastPong = time.Now()
}

// Stop 停止心跳
func (h *HeartbeatManager) Stop() {
	if h.ticker != nil {
		h.ticker.Stop()
	}
	close(h.stopChan)
}

// IsTimeout 检查是否超时
func (h *HeartbeatManager) IsTimeout() bool {
	if !h.config.Enabled {
		return false
	}
	return time.Since(h.lastPong) > h.config.Timeout
}

// GetLastPongTime 获取最后一次pong时间
func (h *HeartbeatManager) GetLastPongTime() time.Time {
	return h.lastPong
}

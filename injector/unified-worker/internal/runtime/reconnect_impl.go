package runtime

import "fmt"

// IncRetry 增加重试计数
func (r *ReconnectManager) IncRetry() {
	r.retryCount++
}

// Reset 重置重试计数
func (r *ReconnectManager) Reset() {
	r.retryCount = 0
}

// GetRetryCount 获取当前重试次数
func (r *ReconnectManager) GetRetryCount() int {
	return r.retryCount
}

// GetMaxRetries 获取最大重试次数
func (r *ReconnectManager) GetMaxRetries() int {
	return r.config.MaxRetries
}

// String 返回重连管理器状态字符串
func (r *ReconnectManager) String() string {
	if r.config.MaxRetries < 0 {
		return fmt.Sprintf("ReconnectManager(retry=%d/unlimited)", r.retryCount)
	}
	return fmt.Sprintf("ReconnectManager(retry=%d/%d)", r.retryCount, r.config.MaxRetries)
}

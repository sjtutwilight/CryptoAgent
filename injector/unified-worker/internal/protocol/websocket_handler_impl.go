package protocol

import (
	"context"
	"fmt"

	"github.com/gorilla/websocket"
)

// Send 发送消息（用于订阅等操作）
func (w *WebSocketHandler) Send(ctx context.Context, message []byte) ([]byte, error) {
	w.mu.RLock()
	conn := w.conn
	w.mu.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("WebSocket未连接")
	}

	// 发送消息
	if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
		return nil, fmt.Errorf("发送WebSocket消息失败: %w", err)
	}

	// WebSocket是异步的，不等待响应
	return nil, nil
}

// Receive 接收消息
func (w *WebSocketHandler) Receive(ctx context.Context) (<-chan []byte, <-chan error) {
	return w.dataChan, w.errChan
}

// HealthCheck 健康检查
func (w *WebSocketHandler) HealthCheck(ctx context.Context) error {
	w.mu.RLock()
	connected := w.connected
	w.mu.RUnlock()

	if !connected {
		return fmt.Errorf("WebSocket未连接")
	}

	// 检查心跳超时
	if w.heartbeatMgr.IsTimeout() {
		return fmt.Errorf("心跳超时")
	}

	return nil
}

// Close 关闭WebSocket连接
func (w *WebSocketHandler) Close() error {
	close(w.stopChan)

	w.heartbeatMgr.Stop()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}

	w.connected = false

	return nil
}

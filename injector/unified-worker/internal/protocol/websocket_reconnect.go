package protocol

import (
	"context"
	"fmt"
	"log"
)

// handleDisconnect 处理断线
func (w *WebSocketHandler) handleDisconnect(ctx context.Context) {
	w.mu.Lock()
	w.connected = false
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
	w.mu.Unlock()

	log.Printf("WebSocket断开连接，准备重连")

	// 尝试重连
	go w.reconnectLoop(ctx)
}

// reconnectLoop 重连循环
func (w *WebSocketHandler) reconnectLoop(ctx context.Context) {
	for w.reconnectMgr.ShouldRetry() {
		w.reconnectMgr.IncRetry()

		log.Printf("开始第%d次重连尝试", w.reconnectMgr.GetRetryCount())

		// 等待退避时间
		if err := w.reconnectMgr.Wait(ctx); err != nil {
			log.Printf("重连等待被取消: %v", err)
			return
		}

		// 尝试连接
		if err := w.connect(ctx); err != nil {
			log.Printf("重连失败: %v", err)
			continue
		}

		log.Printf("重连成功")

		// 重新订阅
		if err := w.resubscribe(ctx); err != nil {
			log.Printf("重新订阅失败: %v", err)
			w.handleDisconnect(ctx)
			continue
		}

		return
	}

	// 重连失败
	log.Printf("达到最大重连次数，停止重连")
	w.errChan <- fmt.Errorf("WebSocket重连失败")
}

// resubscribe 重新订阅
func (w *WebSocketHandler) resubscribe(ctx context.Context) error {
	// 重新发送订阅请求
	for _, subID := range w.subscriptionIDs {
		log.Printf("重新订阅: %s", subID)
		// 这里需要根据具体业务逻辑重新发送订阅请求
		// 暂时留空，由上层Task层处理
	}
	return nil
}

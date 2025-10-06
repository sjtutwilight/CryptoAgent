package protocol

import (
	"context"
	"fmt"
	"log"
	"time"
	
	"github.com/gorilla/websocket"
)

// connect 建立WebSocket连接
func (w *WebSocketHandler) connect(ctx context.Context) error {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = w.runtimeConfig.Connection.Timeout
	
	conn, _, err := dialer.DialContext(ctx, w.url, nil)
	if err != nil {
		return fmt.Errorf("WebSocket连接失败: %w", err)
	}
	
	w.mu.Lock()
	w.conn = conn
	w.connected = true
	w.mu.Unlock()
	
	log.Printf("WebSocket连接成功: %s", w.url)
	
	// 启动消息接收
	go w.receiveLoop(ctx)
	
	// 启动心跳
	w.startHeartbeat(ctx)
	
	// 重置重连计数
	w.reconnectMgr.Reset()
	
	return nil
}

// receiveLoop 接收消息循环
func (w *WebSocketHandler) receiveLoop(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("WebSocket接收循环panic: %v", r)
		}
	}()
	
	for {
		select {
		case <-w.stopChan:
			return
		case <-ctx.Done():
			return
		default:
			w.mu.RLock()
			conn := w.conn
			w.mu.RUnlock()
			
			if conn == nil {
				return
			}
			
			// 设置读超时
			conn.SetReadDeadline(time.Now().Add(w.runtimeConfig.Connection.ReadTimeout))
			
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("WebSocket读取错误: %v", err)
				w.handleDisconnect(ctx)
				return
			}
			
			// 发送数据到channel
			select {
			case w.dataChan <- message:
			default:
				log.Printf("数据channel已满，丢弃消息")
			}
		}
	}
}

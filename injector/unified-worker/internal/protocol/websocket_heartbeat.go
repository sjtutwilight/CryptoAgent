package protocol

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/gorilla/websocket"
)

// startHeartbeat 启动心跳
func (w *WebSocketHandler) startHeartbeat(ctx context.Context) {
	if !w.runtimeConfig.Heartbeat.Enabled {
		return
	}

	pingFunc := func() error {
		w.mu.RLock()
		conn := w.conn
		w.mu.RUnlock()

		if conn == nil {
			return fmt.Errorf("连接不存在")
		}

		// 发送ping
		if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
			log.Printf("发送ping失败: %v", err)
			return err
		}

		return nil
	}

	onTimeout := func() {
		log.Printf("心跳超时，触发重连")
		w.handleDisconnect(ctx)
	}

	// 设置pong处理器
	w.mu.RLock()
	conn := w.conn
	w.mu.RUnlock()

    if conn != nil {
        conn.SetPongHandler(func(appData string) error {
            w.heartbeatMgr.OnPong()
            log.Printf("收到pong响应")
            return nil
        })

        // 处理服务端发来的ping：回复pong并刷新心跳时间
        conn.SetPingHandler(func(appData string) error {
            w.heartbeatMgr.OnPong()
            deadline := time.Now().Add(w.runtimeConfig.Connection.WriteTimeout)
            if err := conn.WriteControl(websocket.PongMessage, []byte(appData), deadline); err != nil {
                log.Printf("回复pong失败: %v", err)
                return err
            }
            return nil
        })
    }

	w.heartbeatMgr.Start(ctx, pingFunc, onTimeout)
}

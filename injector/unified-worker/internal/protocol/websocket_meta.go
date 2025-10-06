package protocol

import "unified-worker/pkg/types"

// Metadata 返回WebSocket协议元数据
func (w *WebSocketHandler) Metadata() types.ProtocolMetadata {
	return types.ProtocolMetadata{
		SupportsBidirectional: true,  // WebSocket是双向的
		RequiresHeartbeat:     true,  // WebSocket需要心跳
		RequiresReconnect:     true,  // WebSocket需要重连
		RequiresConnectionPool: false, // WebSocket不需要连接池
	}
}

// AddSubscriptionID 添加订阅ID（用于重连后重新订阅）
func (w *WebSocketHandler) AddSubscriptionID(subID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.subscriptionIDs = append(w.subscriptionIDs, subID)
}

// ClearSubscriptionIDs 清空订阅ID
func (w *WebSocketHandler) ClearSubscriptionIDs() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.subscriptionIDs = nil
}

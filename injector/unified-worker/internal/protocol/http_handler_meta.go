package protocol

import "unified-worker/pkg/types"

// Close 关闭HTTP处理器
func (h *HTTPHandler) Close() error {
	if h.pool != nil {
		return h.pool.Close()
	}
	return nil
}

// Metadata 返回HTTP协议元数据
func (h *HTTPHandler) Metadata() types.ProtocolMetadata {
	return types.ProtocolMetadata{
		SupportsBidirectional: false, // HTTP是单向的
		RequiresHeartbeat:     false, // HTTP不需要心跳
		RequiresReconnect:     false, // HTTP不需要重连
		RequiresConnectionPool: true, // HTTP需要连接池
	}
}

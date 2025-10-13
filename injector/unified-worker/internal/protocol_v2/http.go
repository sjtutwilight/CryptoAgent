package protocol_v2

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
	
	"unified-worker/pkg/types"
)

// HTTPProtocol HTTP协议（内置连接池）
type HTTPProtocol struct {
	baseURL string
	client  *http.Client
}

// NewHTTPProtocol 创建HTTP协议
func NewHTTPProtocol() Protocol {
	return &HTTPProtocol{}
}

// Type 返回协议类型
func (h *HTTPProtocol) Type() types.ProtocolType {
	return types.ProtocolHTTP
}

// Initialize 初始化协议
func (h *HTTPProtocol) Initialize(ctx context.Context, config map[string]interface{}) error {
	// 解析base_url
	baseURL, ok := config["base_url"].(string)
	if !ok {
		baseURL, ok = config["url"].(string)
		if !ok {
			return fmt.Errorf("缺少base_url配置")
		}
	}
	h.baseURL = baseURL
	
	// 创建HTTP客户端（内置连接池）
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}
	
	// 解析连接池配置（可选）
	if poolCfg, ok := config["connection_pool"].(map[string]interface{}); ok {
		if maxIdle, ok := poolCfg["max_idle_conns"].(int); ok {
			transport.MaxIdleConns = maxIdle
		}
		if maxPerHost, ok := poolCfg["max_conns_per_host"].(int); ok {
			transport.MaxIdleConnsPerHost = maxPerHost
		}
	}
	
	h.client = &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
	
	return nil
}

// Send 发送HTTP请求
func (h *HTTPProtocol) Send(ctx context.Context, message []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", h.baseURL, nil)
	if err != nil {
		return nil, err
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	return io.ReadAll(resp.Body)
}

// Receive HTTP不支持
func (h *HTTPProtocol) Receive(ctx context.Context) (<-chan []byte, <-chan error) {
	errChan := make(chan error, 1)
	errChan <- fmt.Errorf("HTTP不支持Receive")
	return nil, errChan
}

// Close 关闭连接
func (h *HTTPProtocol) Close() error {
	if h.client != nil {
		h.client.CloseIdleConnections()
	}
	return nil
}

// Metadata 返回元数据
func (h *HTTPProtocol) Metadata() types.ProtocolMetadata {
	return types.ProtocolMetadata{
		SupportsBidirectional:  false,
		RequiresHeartbeat:      false,
		RequiresReconnect:      false,
		RequiresConnectionPool: false, // 内置连接池
		RequiresRateLimit:      true,  // 需要业务层限流
		HasBuiltInReconnect:    false,
		HasBuiltInHeartbeat:    false,
		HasBuiltInRateLimit:    false,
	}
}

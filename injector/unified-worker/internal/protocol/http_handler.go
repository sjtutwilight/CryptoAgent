package protocol

import (
	"context"
	"fmt"
	"net/http"

	"unified-worker/internal/runtime"
	"unified-worker/pkg/types"
)

// HTTPHandler HTTP协议处理器
type HTTPHandler struct {
	baseURL string
	client  *http.Client
	pool    *runtime.HTTPConnectionPool
	headers map[string]string
}

// NewHTTPHandler 创建HTTP处理器
func NewHTTPHandler(runtimeCfg types.RuntimeConfig) *HTTPHandler {
	pool := runtime.NewHTTPConnectionPool(runtimeCfg.ConnectionPool, runtimeCfg.Connection)

	return &HTTPHandler{
		client: pool.GetClient(),
		pool:   pool,
	}
}

// Type 返回协议类型
func (h *HTTPHandler) Type() types.ProtocolType {
	return types.ProtocolHTTP
}

// Initialize 初始化HTTP处理器
func (h *HTTPHandler) Initialize(ctx context.Context, config map[string]interface{}) error {
	// 解析配置（兼容base_url和url两种配置）
	baseURL, ok := config["base_url"].(string)
	if !ok {
		// 尝试使用url字段
		baseURL, ok = config["url"].(string)
		if !ok {
			return fmt.Errorf("缺少base_url或url配置")
		}
	}
	h.baseURL = baseURL

	// 解析headers
	if headers, ok := config["headers"].(map[string]interface{}); ok {
		h.headers = make(map[string]string)
		for k, v := range headers {
			if strVal, ok := v.(string); ok {
				h.headers[k] = strVal
			}
		}
	}

	return nil
}

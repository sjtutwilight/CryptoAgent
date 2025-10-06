package runtime

import (
	"net/http"
	"time"
	
	"unified-worker/pkg/types"
)

// HTTPConnectionPool HTTP连接池
type HTTPConnectionPool struct {
	client *http.Client
	config types.ConnectionPoolConfig
}

// NewHTTPConnectionPool 创建HTTP连接池
func NewHTTPConnectionPool(config types.ConnectionPoolConfig, connConfig types.ConnectionConfig) *HTTPConnectionPool {
	transport := &http.Transport{
		MaxIdleConns:        config.MaxIdleConns,
		MaxIdleConnsPerHost: config.MaxConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   !config.Enabled,
	}
	
	client := &http.Client{
		Transport: transport,
		Timeout:   connConfig.Timeout,
	}
	
	return &HTTPConnectionPool{
		client: client,
		config: config,
	}
}

// GetClient 获取HTTP客户端
func (p *HTTPConnectionPool) GetClient() *http.Client {
	return p.client
}

// Close 关闭连接池
func (p *HTTPConnectionPool) Close() error {
	// HTTP客户端会自动管理连接，无需显式关闭
	return nil
}

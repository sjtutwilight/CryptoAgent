package handler

import (
	"context"
	"fmt"
	"log"
)

// RefillerHandler 补数据处理器
// 通过WebSocket或其他方式获取缺失数据
type RefillerHandler struct {
	*BaseHandler
	method  string                                                      // 补数据方法: "websocket", "http"
	fetcher func(ctx context.Context, start, end int64) ([]byte, error) // 补数据函数
}

// RefillerConfig 补数据配置
type RefillerConfig struct {
	Method string `json:"method"` // "websocket", "http"
	MaxGap int    `json:"max_gap"`
}

// NewRefillerHandler 创建补数据处理器
func NewRefillerHandler(config RefillerConfig) *RefillerHandler {
	return &RefillerHandler{
		BaseHandler: NewBaseHandler(fmt.Sprintf("RefillerHandler[%s]", config.Method)),
		method:      config.Method,
	}
}

// SetFetcher 设置补数据获取函数
func (h *RefillerHandler) SetFetcher(fetcher func(ctx context.Context, start, end int64) ([]byte, error)) {
	h.fetcher = fetcher
}

// Refill 执行补数据
func (h *RefillerHandler) Refill(ctx context.Context, start, end int64) ([]byte, error) {
	if h.fetcher == nil {
		return nil, fmt.Errorf("补数据函数未设置")
	}

	log.Printf("[%s] 开始补数据: 范围[%d, %d]", h.Name(), start, end)

	data, err := h.fetcher(ctx, start, end)
	if err != nil {
		return nil, fmt.Errorf("补数据失败: %w", err)
	}

	log.Printf("[%s] 补数据完成: %d bytes", h.Name(), len(data))
	return data, nil
}

// Handle 补数据处理器不参与正常处理链
func (h *RefillerHandler) Handle(ctx context.Context, data []byte) ([]byte, error) {
	// Refiller由MissingDetector主动调用，不在责任链中
	return h.CallNext(ctx, data)
}

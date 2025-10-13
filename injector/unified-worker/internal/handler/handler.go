package handler

import (
	"context"
)

// Handler 处理器接口
// 责任链模式，每个Handler处理特定职责
type Handler interface {
	// Handle 处理数据
	// 返回处理后的数据和错误
	Handle(ctx context.Context, data []byte) ([]byte, error)

	// Name 返回处理器名称
	Name() string

	// SetNext 设置下一个处理器
	SetNext(handler Handler)

	// GetNext 获取下一个处理器
	GetNext() Handler
}

// BaseHandler 基础处理器（提供责任链能力）
type BaseHandler struct {
	name string
	next Handler
}

// NewBaseHandler 创建基础处理器
func NewBaseHandler(name string) *BaseHandler {
	return &BaseHandler{
		name: name,
	}
}

// Name 返回处理器名称
func (h *BaseHandler) Name() string {
	return h.name
}

// SetNext 设置下一个处理器
func (h *BaseHandler) SetNext(handler Handler) {
	h.next = handler
}

// GetNext 获取下一个处理器
func (h *BaseHandler) GetNext() Handler {
	return h.next
}

// CallNext 调用下一个处理器
func (h *BaseHandler) CallNext(ctx context.Context, data []byte) ([]byte, error) {
	if h.next == nil {
		return data, nil // 链尾，返回数据
	}
	return h.next.Handle(ctx, data)
}

package chain

import "context"

// Handler 责任链处理器接口
type Handler interface {
	// Handle 处理请求
	Handle(ctx context.Context, req *Request) error
	
	// SetNext 设置下一个处理器
	SetNext(handler Handler)
	
	// GetName 获取处理器名称
	GetName() string
}

// Request 责任链请求（携带上下文数据）
type Request struct {
	// 角色配置
	RoleConfig interface{}
	
	// 动态数据（各个Handler可以添加）
	Data map[string]interface{}
	
	// 是否跳过后续处理器
	Skip bool
}

// BaseHandler 基础处理器
type BaseHandler struct {
	name string
	next Handler
}

// NewBaseHandler 创建基础处理器
func NewBaseHandler(name string) *BaseHandler {
	return &BaseHandler{name: name}
}

// SetNext 设置下一个处理器
func (h *BaseHandler) SetNext(handler Handler) {
	h.next = handler
}

// GetName 获取处理器名称
func (h *BaseHandler) GetName() string {
	return h.name
}

// CallNext 调用下一个处理器
func (h *BaseHandler) CallNext(ctx context.Context, req *Request) error {
	if req.Skip {
		return nil
	}
	
	if h.next != nil {
		return h.next.Handle(ctx, req)
	}
	
	return nil
}

// Chain 责任链
type Chain struct {
	head Handler
}

// NewChain 创建责任链
func NewChain(head Handler) *Chain {
	return &Chain{head: head}
}

// Execute 执行责任链
func (c *Chain) Execute(ctx context.Context, req *Request) error {
	if c.head != nil {
		return c.head.Handle(ctx, req)
	}
	return nil
}

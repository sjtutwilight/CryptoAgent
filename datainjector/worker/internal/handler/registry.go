package handler

import (
	"fmt"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler/parser"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type Factory func(cfg map[string]any) (Handler, error)

var registry = map[string]Factory{}

func Register(name string, f Factory) {
	registry[name] = f
}

func New(name string, cfg map[string]any) (Handler, error) {
	if f, ok := registry[name]; ok {
		return f(cfg)
	}

	// 尝试从 parser 模块创建
	if h, err := parser.Create(name, cfg); err == nil {
		return &parserAdapter{parser: h}, nil
	}

	return nil, fmt.Errorf("handler %q not registered", name)
}

// parserAdapter 将 parser.Handler 适配为 handler.Handler
type parserAdapter struct {
	parser parser.Handler
}

func (a *parserAdapter) Handle(msg *types.Message) ([]*types.Message, error) {
	return a.parser.Handle(msg)
}

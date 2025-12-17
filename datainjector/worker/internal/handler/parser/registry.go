package parser

import (
	"fmt"
	"sync"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

// Handler 定义 Parser 接口
type Handler interface {
	Handle(msg *types.Message) ([]*types.Message, error)
}

// Factory 是 Parser 的工厂函数类型
type Factory func(cfg map[string]any) (Handler, error)

var (
	registry = make(map[string]Factory)
	mu       sync.RWMutex
)

// Register 注册一个 parser 类型
func Register(name string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("parser: duplicate registration of %q", name))
	}
	registry[name] = factory
}

// Create 根据类型和配置创建 Parser 实例
func Create(parserType string, cfg map[string]any) (Handler, error) {
	mu.RLock()
	factory, ok := registry[parserType]
	mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("parser: unknown type %q", parserType)
	}
	return factory(cfg)
}

// List 返回所有已注册的 parser 类型
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

func init() {
	// 注册所有内置 parser
	Register("binance", func(cfg map[string]any) (Handler, error) {
		return NewBinanceParser(cfg)
	})
	Register("hyperliquid", func(cfg map[string]any) (Handler, error) {
		return NewHyperliquidParser(cfg)
	})

	Register("balance", func(cfg map[string]any) (Handler, error) {
		return NewBalanceParser(cfg)
	})

	Register("dex", func(cfg map[string]any) (Handler, error) {
		return NewDexParser(cfg)
	})
}







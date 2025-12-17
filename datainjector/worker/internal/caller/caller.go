package caller

import (
	"context"
	"fmt"
	"sync"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

// Caller 一次性调用接口：根据 args 拉取 0~N 条消息
type Caller interface {
	CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error)
}

// 工厂注册表
type Factory func(class string, params map[string]any) (Caller, error)

var (
	mu       sync.RWMutex
	registry = map[string]Factory{}
)

func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	registry[name] = f
}

func New(name, class string, params map[string]any) (Caller, error) {
	mu.RLock()
	f, ok := registry[name]
	mu.RUnlock()
	if !ok {
		// 调试：列出所有已注册的 caller
		mu.RLock()
		var registered []string
		for k := range registry {
			registered = append(registered, k)
		}
		mu.RUnlock()
		return nil, fmt.Errorf("caller %q not found (registered: %v)", name, registered)
	}
	return f(class, params)
}

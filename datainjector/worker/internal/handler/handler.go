package handler

import "github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"

// Handler 责任链节点接口
type Handler interface {
	Handle(msg *types.Message) ([]*types.Message, error)
}

// NoopHandler 直接透传
type NoopHandler struct{}

func init() {
	Register("noop", func(cfg map[string]any) (Handler, error) {
		return &NoopHandler{}, nil
	})
}

func (n *NoopHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil {
		return nil, nil
	}
	return []*types.Message{msg}, nil
}

func getString(m map[string]any, key, def string) string {
	if v, ok := m[key]; ok {
		if sv, ok := v.(string); ok {
			return sv
		}
	}
	return def
}

func getInt(m map[string]any, key string, def int) int {
	if v, ok := m[key]; ok {
		switch vv := v.(type) {
		case int:
			return vv
		case int64:
			return int(vv)
		case float64:
			return int(vv)
		}
	}
	return def
}

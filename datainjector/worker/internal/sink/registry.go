package sink

import "fmt"

type Factory func(cfg map[string]any) (Sink, error)

var registry = map[string]Factory{}

func Register(name string, f Factory) {
	registry[name] = f
}

func New(name string, cfg map[string]any) (Sink, error) {
	if f, ok := registry[name]; ok {
		return f(cfg)
	}
	return nil, fmt.Errorf("sink %q not registered", name)
}

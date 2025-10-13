package handler

import "fmt"

type Factory func(cfg map[string]any) (Handler, error)

var registry = map[string]Factory{}

func Register(name string, f Factory) {
	registry[name] = f
}

func New(name string, cfg map[string]any) (Handler, error) {
	if f, ok := registry[name]; ok {
		return f(cfg)
	}
	return nil, fmt.Errorf("handler %q not registered", name)
}

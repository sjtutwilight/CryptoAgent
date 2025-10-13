package parser

import (
	"fmt"
)

// SimpleParser 简化的Parser接口（用于Handler）
type SimpleParser interface {
	Parse(rawData []byte) (interface{}, error)
	Name() string
}

// ParserFactory Parser工厂
type ParserFactory struct {
	parsers map[string]SimpleParser
}

// NewParserFactory 创建Parser工厂
func NewParserFactory() *ParserFactory {
	factory := &ParserFactory{
		parsers: make(map[string]SimpleParser),
	}

	// 注册内置parsers
	factory.Register("BalanceParser", NewBalanceParserSimple())
	factory.Register("BlockParser", NewBlockParserSimple())
	factory.Register("GenericParser", NewGenericParserSimple())

	return factory
}

// Register 注册Parser
func (f *ParserFactory) Register(name string, parser SimpleParser) {
	f.parsers[name] = parser
}

// Create 创建Parser实例
func (f *ParserFactory) Create(name string) (SimpleParser, error) {
	parser, exists := f.parsers[name]
	if !exists {
		return nil, fmt.Errorf("未知Parser: %s", name)
	}
	return parser, nil
}

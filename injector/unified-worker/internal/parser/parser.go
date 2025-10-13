package parser

import (
	"context"
	"encoding/json"
	"fmt"
)

// DataParser 数据解析器接口（责任链模式）
type DataParser interface {
	// CanHandle 判断是否能处理该类型数据
	CanHandle(dataSourceID string, taskType string) bool

	// Parse 解析数据
	Parse(ctx context.Context, rawData []byte, config *ParserConfig) (*ParsedData, error)

	// GetSequence 提取序列号
	GetSequence(parsedData *ParsedData) (interface{}, error)

	// SetNext 设置下一个解析器（责任链）
	SetNext(parser DataParser)
}

// ParserConfig 解析器配置
type ParserConfig struct {
	DataSourceID  string                 // 数据源ID
	TaskType      string                 // 任务类型
	SequenceField string                 // 序列号字段
	CustomConfig  map[string]interface{} // 自定义配置
}

// ParsedData 解析后的数据
type ParsedData struct {
	OriginalData  json.RawMessage        // 原始数据
	ExtractedData map[string]interface{} // 提取的字段
	Metadata      map[string]interface{} // 元数据
}

// BaseParser 基础解析器（实现责任链）
type BaseParser struct {
	next DataParser
}

// SetNext 设置下一个解析器
func (bp *BaseParser) SetNext(parser DataParser) {
	bp.next = parser
}

// TryNext 尝试下一个解析器
func (bp *BaseParser) TryNext(ctx context.Context, rawData []byte, config *ParserConfig) (*ParsedData, error) {
	if bp.next != nil {
		return bp.next.Parse(ctx, rawData, config)
	}
	return nil, fmt.Errorf("无法找到合适的解析器处理: dataSourceID=%s, taskType=%s",
		config.DataSourceID, config.TaskType)
}

// ParserChain 解析器责任链
type ParserChain struct {
	head DataParser
}

// NewParserChain 创建解析器链
func NewParserChain() *ParserChain {
	// 构建责任链：DexParser -> BlockParser -> BalanceParser -> GenericParser
	// DexParser优先级最高，因为它需要完整解析交易和事件
	dexParser := NewDexParser()
	blockParser := NewDexParser() // 使用DexParser
	balanceParser := NewBalanceParser()
	genericParser := NewGenericParser()

	dexParser.SetNext(blockParser)
	blockParser.SetNext(balanceParser)
	balanceParser.SetNext(genericParser)

	return &ParserChain{
		head: dexParser,
	}
}

// Parse 解析数据（从链头开始）
func (pc *ParserChain) Parse(ctx context.Context, rawData []byte, config *ParserConfig) (*ParsedData, error) {
	if pc.head != nil {
		return pc.head.Parse(ctx, rawData, config)
	}
	return nil, fmt.Errorf("解析器链为空")
}

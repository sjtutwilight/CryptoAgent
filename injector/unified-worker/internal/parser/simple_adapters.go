package parser

import (
	"context"
	"encoding/json"
)

// BalanceParserSimple 简化的余额Parser
type BalanceParserSimple struct {
	parser *BalanceParser
}

func NewBalanceParserSimple() *BalanceParserSimple {
	return &BalanceParserSimple{
		parser: &BalanceParser{},
	}
}

func (p *BalanceParserSimple) Parse(rawData []byte) (interface{}, error) {
	config := &ParserConfig{}
	return p.parser.Parse(context.Background(), rawData, config)
}

func (p *BalanceParserSimple) Name() string {
	return "BalanceParser"
}

// BlockParserSimple 简化的区块Parser
type BlockParserSimple struct {
	parser *DexParser // 使用DexParser替代
}

func NewBlockParserSimple() *BlockParserSimple {
	return &BlockParserSimple{
		parser: NewDexParser(),
	}
}

func (p *BlockParserSimple) Parse(rawData []byte) (interface{}, error) {
	config := &ParserConfig{}
	return p.parser.Parse(context.Background(), rawData, config)
}

func (p *BlockParserSimple) Name() string {
	return "BlockParser"
}

// GenericParserSimple 简化的通用Parser
type GenericParserSimple struct {
	parser *GenericParser
}

func NewGenericParserSimple() *GenericParserSimple {
	return &GenericParserSimple{
		parser: &GenericParser{},
	}
}

func (p *GenericParserSimple) Parse(rawData []byte) (interface{}, error) {
	// 通用Parser直接透传JSON
	var result interface{}
	if err := json.Unmarshal(rawData, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (p *GenericParserSimple) Name() string {
	return "GenericParser"
}

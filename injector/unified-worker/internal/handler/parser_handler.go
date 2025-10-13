package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"unified-worker/internal/parser"
)

// ParserHandler Parser处理器
// 负责将原始数据解析为标准格式
type ParserHandler struct {
	*BaseHandler
	parser parser.SimpleParser
}

// NewParserHandler 创建Parser处理器
func NewParserHandler(parserName string, parserInstance parser.SimpleParser) *ParserHandler {
	return &ParserHandler{
		BaseHandler: NewBaseHandler(fmt.Sprintf("ParserHandler[%s]", parserName)),
		parser:      parserInstance,
	}
}

// Handle 处理数据
func (h *ParserHandler) Handle(ctx context.Context, data []byte) ([]byte, error) {
	log.Printf("[%s] 开始解析数据, 原始大小: %d bytes", h.Name(), len(data))

	// 解析数据
	parsed, err := h.parser.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("解析失败: %w", err)
	}

	// 序列化为JSON
	output, err := json.Marshal(parsed)
	if err != nil {
		return nil, fmt.Errorf("序列化解析结果失败: %w", err)
	}

	log.Printf("[%s] 解析完成, 输出大小: %d bytes", h.Name(), len(output))

	// 传递给下一个处理器
	return h.CallNext(ctx, output)
}

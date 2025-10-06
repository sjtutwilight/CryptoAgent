package pipeline

import (
	"context"
	"log"
	
	"unified-worker/internal/parser"
)

// ParseStage 解析阶段
type ParseStage struct {
	BasePipeline
	parserChain  *parser.ParserChain
	parserConfig *parser.ParserConfig
}

// NewParseStage 创建解析阶段
func NewParseStage(parserConfig *parser.ParserConfig) *ParseStage {
	return &ParseStage{
		BasePipeline: *NewBasePipeline("ParseStage"),
		parserChain:  parser.NewParserChain(),
		parserConfig: parserConfig,
	}
}

// Process 处理数据
func (ps *ParseStage) Process(ctx context.Context, data *PipelineData) error {
	log.Printf("[%s] 开始解析数据", ps.GetName())
	
	// 使用解析器链解析数据
	parsedData, err := ps.parserChain.Parse(ctx, data.RawData, ps.parserConfig)
	if err != nil {
		log.Printf("[%s] 解析失败: %v", ps.GetName(), err)
		return err
	}
	
	// 更新管道数据
	data.ParsedData = parsedData.ExtractedData
	data.Metadata["parser_type"] = parsedData.Metadata["parser_type"]
	
	log.Printf("[%s] 解析完成: parser_type=%v", ps.GetName(), parsedData.Metadata["parser_type"])
	
	// 继续下一个处理器
	return ps.ProcessNext(ctx, data)
}

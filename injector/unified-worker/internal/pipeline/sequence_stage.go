package pipeline

import (
	"context"
	"log"

	"unified-worker/internal/parser"
)

// SequenceStage 序列号提取阶段
type SequenceStage struct {
	BasePipeline
	sequenceField string
}

// NewSequenceStage 创建序列号提取阶段
func NewSequenceStage(sequenceField string) *SequenceStage {
	return &SequenceStage{
		BasePipeline:  *NewBasePipeline("SequenceStage"),
		sequenceField: sequenceField,
	}
}

// Process 处理数据
func (ss *SequenceStage) Process(ctx context.Context, data *PipelineData) error {
	log.Printf("[%s] 提取序列号: field=%s", ss.GetName(), ss.sequenceField)

	// 从解析后的数据中提取序列号
	if seq, ok := data.ParsedData[ss.sequenceField]; ok {
		data.Sequence = seq
		log.Printf("[%s] 序列号提取成功: %v", ss.GetName(), seq)
	} else {
		// 尝试通用提取（支持嵌套路径）
		if seq, err := parser.ExtractFieldByPath(data.ParsedData, ss.sequenceField); err == nil {
			data.Sequence = seq
			log.Printf("[%s] 序列号提取成功（嵌套路径）: %v", ss.GetName(), seq)
		} else {
			log.Printf("[%s] 警告: 未找到序列号字段", ss.GetName())
			// 没有序列号也继续处理
		}
	}

	// 继续下一个处理器
	return ss.ProcessNext(ctx, data)
}

package pipeline

import (
	"context"
	"encoding/json"
)

// Pipeline 数据处理管道（责任链模式）
type Pipeline interface {
	// Process 处理数据
	Process(ctx context.Context, data *PipelineData) error
	
	// SetNext 设置下一个处理器
	SetNext(pipeline Pipeline)
	
	// GetName 获取处理器名称
	GetName() string
}

// PipelineData 管道数据（在各个处理器间传递）
type PipelineData struct {
	RawData       []byte                 // 原始数据
	ParsedData    map[string]interface{} // 解析后的数据
	Sequence      interface{}            // 序列号
	Metadata      map[string]interface{} // 元数据
	ShouldOutput  bool                   // 是否应该输出
	OutputTopic   string                 // 输出Topic
	OutputKey     string                 // 输出Key
	OutputPayload json.RawMessage        // 输出载荷
}

// BasePipeline 基础管道处理器
type BasePipeline struct {
	name string
	next Pipeline
}

// NewBasePipeline 创建基础管道
func NewBasePipeline(name string) *BasePipeline {
	return &BasePipeline{name: name}
}

// SetNext 设置下一个处理器
func (bp *BasePipeline) SetNext(pipeline Pipeline) {
	bp.next = pipeline
}

// GetName 获取处理器名称
func (bp *BasePipeline) GetName() string {
	return bp.name
}

// ProcessNext 处理下一个
func (bp *BasePipeline) ProcessNext(ctx context.Context, data *PipelineData) error {
	if bp.next != nil {
		return bp.next.Process(ctx, data)
	}
	return nil
}

// PipelineChain 管道链
type PipelineChain struct {
	head Pipeline
}

// NewPipelineChain 创建管道链
func NewPipelineChain(head Pipeline) *PipelineChain {
	return &PipelineChain{head: head}
}

// Execute 执行管道
func (pc *PipelineChain) Execute(ctx context.Context, rawData []byte) error {
	data := &PipelineData{
		RawData:      rawData,
		ParsedData:   make(map[string]interface{}),
		Metadata:     make(map[string]interface{}),
		ShouldOutput: true,
	}
	
	if pc.head != nil {
		return pc.head.Process(ctx, data)
	}
	
	return nil
}

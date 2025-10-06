package pipeline

import (
	"context"
	"log"
	
	"unified-worker/internal/buffer"
	"unified-worker/internal/parser"
)

// BufferStage 缓冲阶段（处理乱序）
type BufferStage struct {
	BasePipeline
	seqBuffer    *buffer.SequenceBuffer
	lastSequence interface{}
}

// NewBufferStage 创建缓冲阶段
func NewBufferStage() *BufferStage {
	return &BufferStage{
		BasePipeline: *NewBasePipeline("BufferStage"),
		seqBuffer:    buffer.NewSequenceBuffer(1000, buffer.DefaultCompareInt64),
	}
}

// Process 处理数据
func (bs *BufferStage) Process(ctx context.Context, data *PipelineData) error {
	// 如果没有序列号，直接跳过缓冲
	if data.Sequence == nil {
		log.Printf("[%s] 无序列号，跳过缓冲", bs.GetName())
		return bs.ProcessNext(ctx, data)
	}
	
	// 检查是否需要缓冲
	if bs.shouldBuffer(data.Sequence) {
		log.Printf("[%s] 数据乱序，加入缓冲: seq=%v", bs.GetName(), data.Sequence)
		if err := bs.seqBuffer.Add(data.Sequence, data.RawData); err != nil {
			log.Printf("[%s] 缓冲失败: %v，直接输出", bs.GetName(), err)
		} else {
			// 数据已缓冲，不输出
			data.ShouldOutput = false
			log.Printf("[%s] 缓冲成功, buffer_size=%d", bs.GetName(), bs.seqBuffer.Size())
		}
	}
	
	// 更新最后序列号
	bs.lastSequence = data.Sequence
	
	// 继续下一个处理器
	return bs.ProcessNext(ctx, data)
}

// shouldBuffer 判断是否需要缓冲
func (bs *BufferStage) shouldBuffer(sequence interface{}) bool {
	if bs.lastSequence == nil {
		return false
	}
	
	currentSeq, err := parser.ConvertToInt64(sequence)
	if err != nil {
		return false
	}
	
	lastSeq, err := parser.ConvertToInt64(bs.lastSequence)
	if err != nil {
		return false
	}
	
	// 如果序列号跳跃，需要缓冲
	return currentSeq > lastSeq+1
}

// FlushBuffer 刷新缓冲区（供外部调用）
func (bs *BufferStage) FlushBuffer() []*buffer.BufferedItem {
	var items []*buffer.BufferedItem
	for {
		item, ok := bs.seqBuffer.GetNext(bs.lastSequence)
		if !ok {
			break
		}
		items = append(items, item)
		bs.lastSequence = item.Sequence
	}
	return items
}

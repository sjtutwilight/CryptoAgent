package buffer

import (
	"fmt"
)

// Add 添加数据到缓冲区
func (sb *SequenceBuffer) Add(sequence interface{}, data []byte) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	
	// 检查是否已存在
	if _, exists := sb.pending[sequence]; exists {
		return fmt.Errorf("序列号已存在: %v", sequence)
	}
	
	// 检查缓冲区大小
	if sb.buffer.Len() >= sb.maxBufferSize {
		return fmt.Errorf("缓冲区已满，大小: %d", sb.maxBufferSize)
	}
	
	item := &BufferedItem{
		Sequence: sequence,
		Data:     data,
	}
	
	// 插入到缓冲区（按序列号排序）
	inserted := false
	for e := sb.buffer.Front(); e != nil; e = e.Next() {
		existing := e.Value.(*BufferedItem)
		if sb.compareFn(sequence, existing.Sequence) < 0 {
			element := sb.buffer.InsertBefore(item, e)
			sb.pending[sequence] = element
			inserted = true
			break
		}
	}
	
	if !inserted {
		element := sb.buffer.PushBack(item)
		sb.pending[sequence] = element
	}
	
	return nil
}

// GetNext 获取下一个可处理的数据（序列号连续）
func (sb *SequenceBuffer) GetNext(currentSequence interface{}) (*BufferedItem, bool) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	
	if sb.buffer.Len() == 0 {
		return nil, false
	}
	
	front := sb.buffer.Front()
	if front == nil {
		return nil, false
	}
	
	item := front.Value.(*BufferedItem)
	
	// 检查是否是期望的下一个序列号
	if sb.compareFn(item.Sequence, currentSequence) <= 0 {
		// 移除并返回
		sb.buffer.Remove(front)
		delete(sb.pending, item.Sequence)
		return item, true
	}
	
	return nil, false
}

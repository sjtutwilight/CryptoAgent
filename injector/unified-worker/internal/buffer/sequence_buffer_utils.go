package buffer

import "container/list"

// Size 返回缓冲区大小
func (sb *SequenceBuffer) Size() int {
	sb.mu.RLock()
	defer sb.mu.RUnlock()
	return sb.buffer.Len()
}

// Clear 清空缓冲区
func (sb *SequenceBuffer) Clear() {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.buffer.Init()
	sb.pending = make(map[interface{}]*list.Element)
}

// Has 检查是否包含指定序列号
func (sb *SequenceBuffer) Has(sequence interface{}) bool {
	sb.mu.RLock()
	defer sb.mu.RUnlock()
	_, exists := sb.pending[sequence]
	return exists
}

// GetPendingSequences 获取所有待处理的序列号
func (sb *SequenceBuffer) GetPendingSequences() []interface{} {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	sequences := make([]interface{}, 0, len(sb.pending))
	for seq := range sb.pending {
		sequences = append(sequences, seq)
	}
	return sequences
}

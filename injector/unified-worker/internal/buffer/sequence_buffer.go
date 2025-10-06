package buffer

import (
	"container/list"
	"sync"
)

// SequenceBuffer 序列号缓冲器（用于乱序数据）
type SequenceBuffer struct {
	buffer        *list.List                    // 缓冲区（链表）
	pending       map[interface{}]*list.Element // 待处理的数据（key=sequence）
	expected      int64                         // 期望的下一个序列号（仅用于数字序列号）
	maxBufferSize int                           // 最大缓冲区大小
	mu            sync.RWMutex
	compareFn     CompareFunc // 序列号比较函数
}

// CompareFunc 序列号比较函数
// 返回值：-1表示a<b, 0表示a==b, 1表示a>b
type CompareFunc func(a, b interface{}) int

// BufferedItem 缓冲项
type BufferedItem struct {
	Sequence interface{} // 序列号
	Data     []byte      // 数据
}

// NewSequenceBuffer 创建序列号缓冲器
func NewSequenceBuffer(maxBufferSize int, compareFn CompareFunc) *SequenceBuffer {
	return &SequenceBuffer{
		buffer:        list.New(),
		pending:       make(map[interface{}]*list.Element),
		expected:      0,
		maxBufferSize: maxBufferSize,
		compareFn:     compareFn,
	}
}

// DefaultCompareInt64 默认的int64比较函数
func DefaultCompareInt64(a, b interface{}) int {
	aVal, aOk := a.(int64)
	bVal, bOk := b.(int64)
	if !aOk || !bOk {
		return 0
	}
	if aVal < bVal {
		return -1
	} else if aVal > bVal {
		return 1
	}
	return 0
}

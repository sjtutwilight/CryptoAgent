package integrity

import (
	"sync"
)

// Gate 决定消息何时对下游可见。
// Gate 作为控制面，不应缓存消息，而是控制 buffer 的释放策略。
type Gate interface {
	// ShouldPass 判断当前事件是否应该立即下发
	ShouldPass(evt *Event) bool
	// OnDelivered 通知 Gate 消息已下发，用于更新内部状态
	OnDelivered(evt *Event)
	// OnSnapshotApplied 快照应用完成，返回是否应该释放所有缓冲消息
	OnSnapshotApplied(lastSeq uint64) bool
}

type noopGate struct{}

func (g *noopGate) ShouldPass(evt *Event) bool {
	return true
}

func (g *noopGate) OnDelivered(evt *Event) {}

func (g *noopGate) OnSnapshotApplied(uint64) bool {
	return false // noop gate 不需要特殊处理快照
}

// snapshotHoldGate 在快照应用前阻塞所有 diff 消息，快照后一次性释放。
// 不缓存消息本身，由 SequenceEngine 的 buffer 负责缓存。
type snapshotHoldGate struct {
	mu              sync.Mutex
	snapshotApplied bool // 快照是否已应用
}

func (g *snapshotHoldGate) ShouldPass(evt *Event) bool {
	if evt == nil || evt.Message == nil {
		return false
	}
	// 快照消息立即放行
	if evt.Message.Metadata != nil {
		if snapshot, _ := evt.Message.Metadata["snapshot"].(bool); snapshot {
			return true
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// 快照应用后，所有消息放行
	return g.snapshotApplied
}

func (g *snapshotHoldGate) OnDelivered(evt *Event) {
	// 检测到快照消息下发，标记为已应用
	if evt != nil && evt.Message != nil && evt.Message.Metadata != nil {
		if snapshot, _ := evt.Message.Metadata["snapshot"].(bool); snapshot {
			g.mu.Lock()
			g.snapshotApplied = true
			g.mu.Unlock()
		}
	}
}

func (g *snapshotHoldGate) OnSnapshotApplied(uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.snapshotApplied = true
	return true // 通知 engine 释放所有缓冲消息
}

// finalityGate 等待 N 个块的确认后才放行消息。
// 不缓存消息本身，只维护确认状态，由 buffer 负责缓存。
type finalityGate struct {
	mu           sync.Mutex
	finality     int      // 需要的确认块数
	seqWindow    []uint64 // 已确认的序列号窗口（FIFO）
	confirmedSeq uint64   // 已确认可下发的最大序列号
}

func newFinalityGate(blocks int) Gate {
	if blocks <= 0 {
		return &noopGate{}
	}
	return &finalityGate{
		finality:  blocks,
		seqWindow: make([]uint64, 0, blocks),
	}
}

func (g *finalityGate) ShouldPass(evt *Event) bool {
	if evt == nil {
		return false
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// 只有已确认的序列号才能通过
	return evt.Seq <= g.confirmedSeq
}

func (g *finalityGate) OnDelivered(evt *Event) {
	if evt == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// 将新序列号加入窗口
	g.seqWindow = append(g.seqWindow, evt.Seq)

	// 如果窗口已满，释放最老的序列号
	if len(g.seqWindow) >= g.finality {
		confirmed := g.seqWindow[0]
		g.seqWindow = g.seqWindow[1:]

		// 更新已确认序列号
		if confirmed > g.confirmedSeq {
			g.confirmedSeq = confirmed
		}
	}
}

func (g *finalityGate) OnSnapshotApplied(lastSeq uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	// snapshot 看作强一致性，直接确认到该序列号
	if lastSeq > g.confirmedSeq {
		g.confirmedSeq = lastSeq
	}
	// 清空窗口，从快照位置重新开始
	g.seqWindow = nil

	return true // 通知 engine 释放 buffer 中 <= lastSeq 的消息
}







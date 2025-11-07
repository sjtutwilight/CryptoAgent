package integrity

import (
	"math"
	"sort"
	"sync"
	"time"
)

// reorderBuffer 提供按序 drain 能力，同时支持 TTL 与容量约束。
type reorderBuffer struct {
	ttl        time.Duration // 单条缓存的生存时间
	maxBuckets int           // 缓存桶数量上限
	sweepEvery time.Duration // 清理周期

	mu        sync.Mutex
	buckets   map[uint64][]*Event  // seq → [event]
	firstSeen map[uint64]time.Time // 记录首次出现时间
	lastSweep time.Time            // 上次清理时间
}

func newReorderBuffer(cfg Config) *reorderBuffer {
	return &reorderBuffer{
		ttl:        cfg.Buffer.TTL,
		maxBuckets: cfg.Buffer.MaxBuckets,
		sweepEvery: cfg.Buffer.SweepEvery,
		buckets:    make(map[uint64][]*Event),
		firstSeen:  make(map[uint64]time.Time),
	}
}

func (b *reorderBuffer) add(evt *Event) {
	if evt == nil {
		return
	}
	// 每个 seq 使用 append 保持 FIFO
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buckets[evt.Seq] = append(b.buckets[evt.Seq], evt)
	if _, ok := b.firstSeen[evt.Seq]; !ok {
		b.firstSeen[evt.Seq] = evt.Arrival
	}
}

func (b *reorderBuffer) drain(expected uint64) ([]*Event, uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*Event
	for {
		events, ok := b.buckets[expected]
		if !ok {
			break
		}
		delete(b.buckets, expected)
		delete(b.firstSeen, expected)
		out = append(out, events...)
		expected++
	}
	return out, expected
}

func (b *reorderBuffer) cleanup(le uint64) {
	// 用于快照确认或跳跃时快速丢弃旧 bucket
	b.mu.Lock()
	defer b.mu.Unlock()
	for seq := range b.buckets {
		if seq <= le {
			delete(b.buckets, seq)
			delete(b.firstSeen, seq)
		}
	}
}

func (b *reorderBuffer) sweep(now time.Time) []uint64 {
	if now.IsZero() {
		now = time.Now()
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sweepEvery > 0 && now.Sub(b.lastSweep) < b.sweepEvery {
		return nil
	}
	b.lastSweep = now

	var removed []uint64

	if b.ttl > 0 {
		for seq, t := range b.firstSeen {
			if now.Sub(t) > b.ttl {
				delete(b.firstSeen, seq)
				delete(b.buckets, seq)
				removed = append(removed, seq)
			}
		}
	}

	if b.maxBuckets > 0 && len(b.buckets) > b.maxBuckets {
		excess := len(b.buckets) - b.maxBuckets
		seqs := make([]uint64, 0, len(b.buckets))
		for seq := range b.buckets {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for i := 0; i < excess && i < len(seqs); i++ {
			seq := seqs[i]
			delete(b.buckets, seq)
			delete(b.firstSeen, seq)
			removed = append(removed, seq)
		}
	}

	if len(removed) == 0 {
		return nil
	}
	sort.Slice(removed, func(i, j int) bool { return removed[i] < removed[j] })
	out := removed[:0]
	last := uint64(math.MaxUint64)
	for _, seq := range removed {
		if seq == last {
			continue
		}
		out = append(out, seq)
		last = seq
	}
	return out
}

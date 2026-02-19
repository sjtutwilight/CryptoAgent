package integrity

import (
	"sort"
	"sync"
	"time"
)

type gapWindow struct {
	Start     uint64
	End       uint64
	FirstSeen time.Time
	LastSeen  time.Time
}

type gapWindowStats struct {
	OpenCount int
	Missing   uint64
	OldestAge time.Duration
}

type gapWindows struct {
	mu         sync.Mutex
	windows    []gapWindow
	ttl        time.Duration
	maxWindows int
}

func newGapWindows(cfg Config) *gapWindows {
	return &gapWindows{
		ttl:        cfg.Buffer.TTL,
		maxWindows: cfg.Buffer.MaxBuckets,
	}
}

func (g *gapWindows) reset() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.windows = nil
}

func (g *gapWindows) add(start, end uint64, now time.Time) {
	if g == nil || end < start {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	g.windows = append(g.windows, gapWindow{
		Start:     start,
		End:       end,
		FirstSeen: now,
		LastSeen:  now,
	})
	sort.Slice(g.windows, func(i, j int) bool {
		return g.windows[i].Start < g.windows[j].Start
	})

	merged := make([]gapWindow, 0, len(g.windows))
	for _, curr := range g.windows {
		if len(merged) == 0 {
			merged = append(merged, curr)
			continue
		}
		last := &merged[len(merged)-1]
		if curr.Start > last.End+1 {
			merged = append(merged, curr)
			continue
		}
		if curr.End > last.End {
			last.End = curr.End
		}
		if curr.FirstSeen.Before(last.FirstSeen) {
			last.FirstSeen = curr.FirstSeen
		}
		if curr.LastSeen.After(last.LastSeen) {
			last.LastSeen = curr.LastSeen
		}
	}
	g.windows = merged
	g.trimLocked()
}

func (g *gapWindows) resolveTo(seq uint64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	next := make([]gapWindow, 0, len(g.windows))
	for _, w := range g.windows {
		if w.End <= seq {
			continue
		}
		if w.Start <= seq {
			w.Start = seq + 1
		}
		next = append(next, w)
	}
	g.windows = next
}

func (g *gapWindows) sweep(now time.Time) {
	if g == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.ttl > 0 {
		next := g.windows[:0]
		for _, w := range g.windows {
			if now.Sub(w.LastSeen) > g.ttl {
				continue
			}
			next = append(next, w)
		}
		g.windows = next
	}
	g.trimLocked()
}

func (g *gapWindows) stats(now time.Time) gapWindowStats {
	if g == nil {
		return gapWindowStats{}
	}
	if now.IsZero() {
		now = time.Now()
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	var out gapWindowStats
	out.OpenCount = len(g.windows)
	var oldest time.Time
	for _, w := range g.windows {
		out.Missing += w.End - w.Start + 1
		if oldest.IsZero() || w.FirstSeen.Before(oldest) {
			oldest = w.FirstSeen
		}
	}
	if !oldest.IsZero() {
		out.OldestAge = now.Sub(oldest)
		if out.OldestAge < 0 {
			out.OldestAge = 0
		}
	}
	return out
}

func (g *gapWindows) trimLocked() {
	if g.maxWindows <= 0 || len(g.windows) <= g.maxWindows {
		return
	}
	excess := len(g.windows) - g.maxWindows
	g.windows = g.windows[excess:]
}

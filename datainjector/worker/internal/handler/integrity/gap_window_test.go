package integrity

import (
	"testing"
	"time"
)

func TestGapWindowsMergeDuplicateRanges(t *testing.T) {
	cfg := Config{}
	cfg.Buffer.TTL = time.Second
	cfg.Buffer.MaxBuckets = 10
	cfg.Normalise()

	g := newGapWindows(cfg)
	now := time.Now()
	g.add(100, 110, now)
	g.add(105, 115, now.Add(time.Millisecond))
	g.add(100, 110, now.Add(2*time.Millisecond))

	stats := g.stats(now.Add(3 * time.Millisecond))
	if stats.OpenCount != 1 {
		t.Fatalf("expected merged single window, got %d", stats.OpenCount)
	}
	if stats.Missing != 16 { // [100,115]
		t.Fatalf("expected missing=16, got %d", stats.Missing)
	}
}

func TestGapWindowsTTLSweep(t *testing.T) {
	cfg := Config{}
	cfg.Buffer.TTL = 5 * time.Millisecond
	cfg.Buffer.MaxBuckets = 10
	cfg.Normalise()

	g := newGapWindows(cfg)
	now := time.Now()
	g.add(1, 3, now)
	g.add(10, 12, now.Add(2*time.Millisecond))
	g.sweep(now.Add(10 * time.Millisecond))

	stats := g.stats(now.Add(10 * time.Millisecond))
	if stats.OpenCount != 0 {
		t.Fatalf("expected all windows swept by ttl, got %d", stats.OpenCount)
	}
}

func TestGapWindowsMaxBucketsTrim(t *testing.T) {
	cfg := Config{}
	cfg.Buffer.TTL = 0
	cfg.Buffer.MaxBuckets = 2
	cfg.Normalise()

	g := newGapWindows(cfg)
	now := time.Now()
	g.add(1, 1, now)
	g.add(3, 3, now)
	g.add(5, 5, now)
	g.add(7, 7, now)

	stats := g.stats(now)
	if stats.OpenCount != 2 {
		t.Fatalf("expected 2 windows after trim, got %d", stats.OpenCount)
	}
	if stats.Missing != 2 {
		t.Fatalf("expected missing=2 after trim, got %d", stats.Missing)
	}
}

package integrity

import (
	"testing"
	"time"
)

func TestReorderBufferDrainAndCleanup(t *testing.T) {
	cfg := Config{}
	cfg.Buffer.TTL = time.Second
	cfg.Buffer.SweepEvery = time.Millisecond
	cfg.Buffer.MaxBuckets = 16
	cfg.Normalise()

	b := newReorderBuffer(cfg)
	now := time.Now()
	b.add(&Event{Seq: 11, Arrival: now})
	b.add(&Event{Seq: 10, Arrival: now})
	b.add(&Event{Seq: 10, Arrival: now.Add(time.Millisecond)})

	out, next := b.drain(10)
	if next != 12 {
		t.Fatalf("expected next=12, got %d", next)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 events drained, got %d", len(out))
	}

	b.add(&Event{Seq: 20, Arrival: now})
	b.add(&Event{Seq: 21, Arrival: now})
	b.cleanup(20)
	if size := b.size(); size != 1 {
		t.Fatalf("expected size=1 after cleanup, got %d", size)
	}
}

func TestReorderBufferSweepByTTL(t *testing.T) {
	cfg := Config{}
	cfg.Buffer.TTL = 5 * time.Millisecond
	cfg.Buffer.SweepEvery = 0
	cfg.Buffer.MaxBuckets = 0
	cfg.Normalise()

	b := newReorderBuffer(cfg)
	now := time.Now()
	b.add(&Event{Seq: 1, Arrival: now})
	b.add(&Event{Seq: 2, Arrival: now.Add(2 * time.Millisecond)})

	removed := b.sweep(now.Add(10 * time.Millisecond))
	if len(removed) != 2 || removed[0] != 1 || removed[1] != 2 {
		t.Fatalf("unexpected removed by ttl: %#v", removed)
	}
	if size := b.size(); size != 0 {
		t.Fatalf("expected empty buffer after ttl sweep, got size=%d", size)
	}
}

func TestReorderBufferSweepByMaxBuckets(t *testing.T) {
	cfg := Config{}
	cfg.Buffer.TTL = 0
	cfg.Buffer.SweepEvery = 0
	cfg.Buffer.MaxBuckets = 2
	cfg.Normalise()

	b := newReorderBuffer(cfg)
	now := time.Now()
	b.add(&Event{Seq: 5, Arrival: now})
	b.add(&Event{Seq: 4, Arrival: now})
	b.add(&Event{Seq: 9, Arrival: now})
	b.add(&Event{Seq: 7, Arrival: now})

	removed := b.sweep(now)
	if len(removed) != 2 || removed[0] != 4 || removed[1] != 5 {
		t.Fatalf("unexpected removed by max buckets: %#v", removed)
	}
	if size := b.size(); size != 2 {
		t.Fatalf("expected size=2 after max-buckets sweep, got %d", size)
	}
}

package role

import (
	"testing"
	"time"
)

func TestBackfillFailureStateTransitionsToCooldown(t *testing.T) {
	r := &Role{
		backfillStates: make(map[string]backfillState),
	}
	key := "range|stream-a|1|10"
	now := time.Now()

	state, wait := r.backfillMarkFailure(key, 2, 5*time.Second, now)
	if state != "degraded" {
		t.Fatalf("expected degraded state on first failure, got %s", state)
	}
	if wait != 0 {
		t.Fatalf("expected no cooldown on first failure, got %v", wait)
	}

	state, wait = r.backfillMarkFailure(key, 2, 5*time.Second, now.Add(time.Second))
	if state != "cooldown" {
		t.Fatalf("expected cooldown state when threshold reached, got %s", state)
	}
	if wait <= 0 {
		t.Fatalf("expected positive cooldown wait when threshold reached, got %v", wait)
	}

	if remain, ok := r.backfillCooldownRemaining(key, now.Add(2*time.Second)); !ok || remain <= 0 {
		t.Fatalf("expected cooldown remaining > 0, ok=%v remain=%v", ok, remain)
	}

	r.backfillMarkSuccess(key)
	if remain, ok := r.backfillCooldownRemaining(key, now.Add(3*time.Second)); ok || remain != 0 {
		t.Fatalf("expected cooldown cleared after success, ok=%v remain=%v", ok, remain)
	}
}

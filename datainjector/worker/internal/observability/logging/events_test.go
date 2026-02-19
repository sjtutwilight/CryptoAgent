package logging

import "testing"

func TestIntegrityEventConstantsPresent(t *testing.T) {
	events := []string{
		EventIntegrityGapDetected,
		EventIntegrityAdvance,
		EventIntegrityBackfillTrigger,
		EventIntegrityBackfillSkipped,
		EventIntegrityBackfillRetry,
		EventIntegrityBackfillSuccess,
		EventIntegrityBackfillAttempt,
		EventIntegrityBackfillEnqueue,
		EventIntegrityBackfillExhaust,
		EventIntegrityBackfillDedup,
		EventIntegrityBackfillResult,
		EventIntegrityBackfillSession,
		EventIntegritySnapshotAnchor,
		EventIntegrityTimeoutAdvance,
		EventIntegritySessionState,
	}
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		if event == "" {
			t.Fatalf("integrity event constant must not be empty")
		}
		if _, ok := seen[event]; ok {
			t.Fatalf("duplicate integrity event constant: %s", event)
		}
		seen[event] = struct{}{}
	}
}

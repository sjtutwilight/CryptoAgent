package integrity

import (
	"testing"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func TestBuildRangeEvaluatorBinanceDepthUsesRangeOrPrevFinal(t *testing.T) {
	eval := buildRangeEvaluator("binance_depth", "first_update_id")

	if !eval.Covers(101, &Event{
		Seq:        105,
		RangeStart: 100,
		HasRange:   true,
	}) {
		t.Fatalf("expected range-based coverage to pass")
	}

	if !eval.Covers(201, &Event{
		Seq:      250,
		HasRange: false,
		Message: &types.Message{
			Metadata: map[string]any{
				"prev_final_update_id": int64(200),
			},
		},
	}) {
		t.Fatalf("expected pu-based fallback coverage to pass")
	}

	if eval.Covers(301, &Event{
		Seq:      350,
		HasRange: false,
		Message: &types.Message{
			Metadata: map[string]any{
				"prev_final_update_id": int64(100),
			},
		},
	}) {
		t.Fatalf("expected mismatched pu to fail coverage")
	}
}

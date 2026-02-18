package handler

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler/parser/binance"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func TestOrderbookTopicRouterRoutesDiffContract(t *testing.T) {
	raw, err := newOrderbookTopicRouter(map[string]any{
		"symbol":   "AAVEUSDT",
		"exchange": "binance",
	})
	if err != nil {
		t.Fatalf("newOrderbookTopicRouter failed: %v", err)
	}
	h := raw.(*orderbookTopicRouter)

	outs, err := h.Handle(&types.Message{
		Metadata: map[string]any{
			"role_id":  "role-perp-diff",
			"exchange": "binance",
			"binance_depth": &binance.DepthMessage{
				Symbol:            "AAVEUSDT",
				FirstUpdateID:     101,
				FinalUpdateID:     103,
				PrevFinalUpdateID: 100,
				EventTime:         1700000000000,
				Bids:              [][]string{{"100.1", "1.2"}},
				Asks:              [][]string{{"100.2", "1.4"}},
			},
		},
		Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("handle diff failed: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("outs=%d, want 1", len(outs))
	}

	out := outs[0]
	if got := out.Metadata["ob_topic"]; got != obTopicDiff {
		t.Fatalf("ob_topic=%v, want %s", got, obTopicDiff)
	}
	if got := out.Metadata["snapshot"]; got != false {
		t.Fatalf("snapshot=%v, want false", got)
	}
	if got := out.Metadata["first_update_id"]; got != int64(101) {
		t.Fatalf("first_update_id=%v, want 101", got)
	}
	if got := out.Metadata["final_update_id"]; got != int64(103) {
		t.Fatalf("final_update_id=%v, want 103", got)
	}
	if got := out.Metadata["prev_final_update_id"]; got != int64(100) {
		t.Fatalf("prev_final_update_id=%v, want 100", got)
	}

	var payload map[string]any
	if err := json.Unmarshal(out.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	assertHasKey(t, payload, "symbol")
	assertHasKey(t, payload, "exchange")
	assertHasKey(t, payload, "first_update_id")
	assertHasKey(t, payload, "final_update_id")
	assertHasKey(t, payload, "prev_final_update_id")
	assertHasKey(t, payload, "exchange_ts")
	assertHasKey(t, payload, "ingest_ts")
}

func TestOrderbookTopicRouterRoutesBackfillSnapshotContract(t *testing.T) {
	raw, err := newOrderbookTopicRouter(map[string]any{
		"symbol":   "AAVEUSDT",
		"exchange": "binance",
	})
	if err != nil {
		t.Fatalf("newOrderbookTopicRouter failed: %v", err)
	}
	h := raw.(*orderbookTopicRouter)

	before := testutil.ToFloat64(metrics.OrderbookSnapshotEmitted.WithLabelValues("role-perp-diff", "backfill", "gap"))
	outs, err := h.Handle(&types.Message{
		Metadata: map[string]any{
			"role_id":         "role-perp-diff",
			"exchange":        "binance",
			"symbol":          "AAVEUSDT",
			"is_backfill":     true,
			"snapshot":        true,
			"snapshot_source": "backfill",
			"snapshot_reason": "gap",
		},
		Payload: mustJSON(t, binance.DepthSnapshotResponse{
			LastUpdateID: 999,
			EventTime:    1700000000123,
			Bids:         [][]string{{"100.1", "9.9"}},
			Asks:         [][]string{{"100.2", "8.8"}},
			Symbol:       "AAVEUSDT",
		}),
	})
	if err != nil {
		t.Fatalf("handle snapshot failed: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("outs=%d, want 1", len(outs))
	}

	out := outs[0]
	if got := out.Metadata["ob_topic"]; got != obTopicSnapshot {
		t.Fatalf("ob_topic=%v, want %s", got, obTopicSnapshot)
	}
	if got := out.Metadata["snapshot"]; got != true {
		t.Fatalf("snapshot=%v, want true", got)
	}
	if got := out.Metadata["snapshot_source"]; got != "backfill" {
		t.Fatalf("snapshot_source=%v, want backfill", got)
	}
	if got := out.Metadata["snapshot_reason"]; got != "gap" {
		t.Fatalf("snapshot_reason=%v, want gap", got)
	}

	after := testutil.ToFloat64(metrics.OrderbookSnapshotEmitted.WithLabelValues("role-perp-diff", "backfill", "gap"))
	if after != before+1 {
		t.Fatalf("snapshot metric not incremented, before=%v after=%v", before, after)
	}

	var payload map[string]any
	if err := json.Unmarshal(out.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	assertHasKey(t, payload, "symbol")
	assertHasKey(t, payload, "exchange")
	assertHasKey(t, payload, "lastUpdateId")
	assertHasKey(t, payload, "snapshot")
	assertHasKey(t, payload, "snapshot_source")
	assertHasKey(t, payload, "snapshot_reason")
	assertHasKey(t, payload, "exchange_ts")
	assertHasKey(t, payload, "ingest_ts")
}

func TestOrderbookTopicRouterPeriodicSnapshotDefaults(t *testing.T) {
	raw, err := newOrderbookTopicRouter(map[string]any{
		"symbol":   "AAVEUSDT",
		"exchange": "binance",
	})
	if err != nil {
		t.Fatalf("newOrderbookTopicRouter failed: %v", err)
	}
	h := raw.(*orderbookTopicRouter)

	outs, err := h.Handle(&types.Message{
		Metadata: map[string]any{
			"role_id":  "role-perp-snapshot",
			"exchange": "binance",
			"symbol":   "AAVEUSDT",
			"snapshot": true,
		},
		Payload: mustJSON(t, binance.DepthSnapshotResponse{
			LastUpdateID: 1200,
			EventTime:    1700000000222,
			Bids:         [][]string{{"100.1", "3.3"}},
			Asks:         [][]string{{"100.2", "2.2"}},
			Symbol:       "AAVEUSDT",
		}),
	})
	if err != nil {
		t.Fatalf("handle snapshot failed: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("outs=%d, want 1", len(outs))
	}
	if got := outs[0].Metadata["snapshot_source"]; got != "periodic" {
		t.Fatalf("snapshot_source=%v, want periodic", got)
	}
	if got := outs[0].Metadata["snapshot_reason"]; got != "periodic" {
		t.Fatalf("snapshot_reason=%v, want periodic", got)
	}
}

func assertHasKey(t *testing.T, m map[string]any, k string) {
	t.Helper()
	if _, ok := m[k]; !ok {
		t.Fatalf("payload missing key %s", k)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return b
}

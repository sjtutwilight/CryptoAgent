package integrity

import "testing"

func TestParseConfigAppliesDefaultsAndProfileOverrides(t *testing.T) {
	cfg, err := ParseConfig(map[string]any{
		"profile":        "binance_depth",
		"sequence_field": "final_update_id",
	})
	if err != nil {
		t.Fatalf("parse config failed: %v", err)
	}
	if !cfg.Backfill.SnapshotBased {
		t.Fatalf("expected snapshot-based enabled for binance_depth")
	}
	if cfg.Backfill.OrderbookMode != "snapshot_gate" {
		t.Fatalf("expected default orderbook mode snapshot_gate, got %s", cfg.Backfill.OrderbookMode)
	}
	if cfg.Gate.Mode != "snapshot_hold" {
		t.Fatalf("expected gate mode snapshot_hold, got %s", cfg.Gate.Mode)
	}
	if cfg.Feature.HardTimeoutPriority || cfg.Feature.SidechannelAnchor || cfg.Feature.GapWindowMetrics {
		t.Fatalf("expected feature flags default to false for compatibility")
	}
}

func TestParseConfigSidechannelOverridesGate(t *testing.T) {
	cfg, err := ParseConfig(map[string]any{
		"profile":                       "binance_depth",
		"sequence_field":                "final_update_id",
		"orderbook_mode":                "snapshot_sidechannel",
		"gate_mode":                     "snapshot_hold",
		"hard_timeout_ms":               5000,
		"hard_timeout_priority_enabled": true,
		"sidechannel_anchor_enabled":    true,
		"gap_window_metrics_enabled":    true,
	})
	if err != nil {
		t.Fatalf("parse config failed: %v", err)
	}
	if !cfg.SnapshotSideChannelEnabled() {
		t.Fatalf("expected sidechannel mode enabled")
	}
	if cfg.Gate.Mode != "none" {
		t.Fatalf("expected gate forced to none in sidechannel mode, got %s", cfg.Gate.Mode)
	}
	if cfg.Sequence.HardTimeout.Milliseconds() != 5000 {
		t.Fatalf("expected hard timeout kept from config")
	}
	if !cfg.Feature.HardTimeoutPriority || !cfg.Feature.SidechannelAnchor || !cfg.Feature.GapWindowMetrics {
		t.Fatalf("expected feature flags enabled from config")
	}
}

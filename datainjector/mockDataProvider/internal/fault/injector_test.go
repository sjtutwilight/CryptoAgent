package fault

import (
	"mock-service/internal/config"
	"testing"
)

func TestShouldInjectWebSocketDataLossEveryN(t *testing.T) {
	cfg := &config.Config{
		Fault: config.FaultConfig{
			WebSocket: config.WebSocketFaultConfig{
				Enabled:             true,
				DataLossProbability: 0,
				DataLossEveryN:      3,
			},
		},
	}

	inj := NewFaultInjector(cfg)
	got := make([]bool, 0, 6)
	for i := 0; i < 6; i++ {
		got = append(got, inj.ShouldInjectWebSocketDataLoss())
	}

	want := []bool{false, false, true, false, false, true}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("call %d: expected %v, got %v", i+1, want[i], got[i])
		}
	}
}

func TestShouldInjectWebSocketDisconnectionEveryN(t *testing.T) {
	cfg := &config.Config{
		Fault: config.FaultConfig{
			WebSocket: config.WebSocketFaultConfig{
				Enabled:                  true,
				DisconnectionProbability: 0,
				DisconnectionEveryN:      2,
			},
		},
	}

	inj := NewFaultInjector(cfg)
	got := make([]bool, 0, 5)
	for i := 0; i < 5; i++ {
		got = append(got, inj.ShouldInjectWebSocketDisconnection())
	}

	want := []bool{false, true, false, true, false}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("call %d: expected %v, got %v", i+1, want[i], got[i])
		}
	}
}

func TestResetStatsAlsoResetsDeterministicCounters(t *testing.T) {
	cfg := &config.Config{
		Fault: config.FaultConfig{
			WebSocket: config.WebSocketFaultConfig{
				Enabled:             true,
				DataLossProbability: 0,
				DataLossEveryN:      2,
			},
		},
	}

	inj := NewFaultInjector(cfg)

	if inj.ShouldInjectWebSocketDataLoss() {
		t.Fatal("first call should not inject")
	}
	if !inj.ShouldInjectWebSocketDataLoss() {
		t.Fatal("second call should inject")
	}

	inj.ResetStats()

	if inj.ShouldInjectWebSocketDataLoss() {
		t.Fatal("after reset, first call should not inject")
	}
	if !inj.ShouldInjectWebSocketDataLoss() {
		t.Fatal("after reset, second call should inject")
	}
}

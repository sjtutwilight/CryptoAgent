package controller

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mock-service/internal/config"
	"mock-service/internal/fault"
	"mock-service/internal/generator"
	"mock-service/internal/model"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func TestBinanceSubscribeStreamIntegrityWithoutFault(t *testing.T) {
	wsURL, cleanup := setupBinanceWSTestServer(t, config.WebSocketFaultConfig{
		Enabled:                     true,
		DisconnectionProbability:    0,
		DataLossProbability:         0,
		HeartbeatAnomalyProbability: 0,
	})
	defer cleanup()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket failed: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": []string{"aaveusdt@depth@100ms", "aaveusdt@aggTrade"},
		"id":     1,
	}); err != nil {
		t.Fatalf("send subscribe failed: %v", err)
	}

	depthEvents := make([]model.BinanceDepthDiff, 0, 16)
	aggTrades := make([]model.BinanceAggTrade, 0, 16)

	deadline := time.Now().Add(5 * time.Second)
	for len(depthEvents) < 10 || len(aggTrades) < 10 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting events: depth=%d aggtrade=%d", len(depthEvents), len(aggTrades))
		}

		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read websocket message failed: %v", err)
		}

		var raw map[string]interface{}
		if err := json.Unmarshal(payload, &raw); err == nil {
			if _, ok := raw["result"]; ok {
				continue
			}
		}

		eventType, _ := raw["e"].(string)
		if eventType == "" {
			t.Fatalf("missing event type in payload=%s", string(payload))
		}

		switch eventType {
		case "depthUpdate":
			var evt model.BinanceDepthDiff
			if err := json.Unmarshal(payload, &evt); err != nil {
				t.Fatalf("parse depth event failed: %v", err)
			}
			depthEvents = append(depthEvents, evt)
		case "aggTrade":
			var evt model.BinanceAggTrade
			if err := json.Unmarshal(payload, &evt); err != nil {
				t.Fatalf("parse aggTrade event failed: %v", err)
			}
			aggTrades = append(aggTrades, evt)
		default:
			t.Fatalf("unexpected event type: %s", eventType)
		}
	}

	for i := 1; i < len(depthEvents); i++ {
		prev := depthEvents[i-1]
		curr := depthEvents[i]
		if curr.PrevFinalUpdateID != prev.FinalUpdateID {
			t.Fatalf("depth continuity broken at %d: prev.u=%d curr.pu=%d", i, prev.FinalUpdateID, curr.PrevFinalUpdateID)
		}
		if curr.FirstUpdateID != prev.FinalUpdateID+1 {
			t.Fatalf("depth first_update mismatch at %d: expected %d got %d", i, prev.FinalUpdateID+1, curr.FirstUpdateID)
		}
	}

	for i := 1; i < len(aggTrades); i++ {
		prev := aggTrades[i-1]
		curr := aggTrades[i]
		if curr.AggTradeID != prev.AggTradeID+1 {
			t.Fatalf("aggTrade continuity broken at %d: expected agg_id=%d got %d", i, prev.AggTradeID+1, curr.AggTradeID)
		}
		if curr.FirstTradeID != prev.LastTradeID+1 {
			t.Fatalf("trade id range broken at %d: expected first_trade_id=%d got %d", i, prev.LastTradeID+1, curr.FirstTradeID)
		}
	}
}

func TestBinanceSubscribeStreamDetectsGapWithDeterministicDataLoss(t *testing.T) {
	wsURL, cleanup := setupBinanceWSTestServer(t, config.WebSocketFaultConfig{
		Enabled:                     true,
		DisconnectionProbability:    0,
		DataLossProbability:         0,
		HeartbeatAnomalyProbability: 0,
		DataLossEveryN:              2,
	})
	defer cleanup()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket failed: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": []string{"aaveusdt@depth@100ms"},
		"id":     2,
	}); err != nil {
		t.Fatalf("send subscribe failed: %v", err)
	}

	depthEvents := make([]model.BinanceDepthDiff, 0, 16)
	deadline := time.Now().Add(5 * time.Second)
	for len(depthEvents) < 8 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting depth events: got=%d", len(depthEvents))
		}

		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read websocket message failed: %v", err)
		}

		var ack map[string]interface{}
		if err := json.Unmarshal(payload, &ack); err == nil {
			if _, ok := ack["result"]; ok {
				continue
			}
		}

		var evt model.BinanceDepthDiff
		if err := json.Unmarshal(payload, &evt); err != nil {
			t.Fatalf("parse depth event failed: %v", err)
		}
		if evt.EventType != "depthUpdate" {
			t.Fatalf("unexpected event type: %s", evt.EventType)
		}
		depthEvents = append(depthEvents, evt)
	}

	hasGap := false
	for i := 1; i < len(depthEvents); i++ {
		if depthEvents[i].PrevFinalUpdateID != depthEvents[i-1].FinalUpdateID {
			hasGap = true
			break
		}
	}

	if !hasGap {
		t.Fatalf("expected at least one sequence gap under deterministic data loss")
	}
}

func setupBinanceWSTestServer(t *testing.T, wsFault config.WebSocketFaultConfig) (string, func()) {
	t.Helper()
	gin.SetMode(gin.ReleaseMode)

	cfg := &config.Config{
		Fault: config.FaultConfig{
			WebSocket: wsFault,
		},
		Data: config.DataConfig{
			Binance: config.BinanceConfig{
				Enabled:       true,
				IntervalMs:    20,
				SnapshotDepth: 100,
				Symbols: []config.BinanceSymbolConfig{
					{
						Symbol:       "AAVEUSDT",
						BasePrice:    100,
						PriceTick:    0.1,
						QuantityTick: 0.1,
						Levels:       30,
					},
				},
			},
		},
	}

	sim := generator.NewBinanceOrderBookSimulator(&cfg.Data.Binance)
	if sim == nil {
		t.Fatal("simulator should not be nil")
	}
	sim.Start()

	ctrl := NewBinanceOrderBookController(cfg, sim, fault.NewFaultInjector(cfg))
	if ctrl == nil {
		t.Fatal("controller should not be nil")
	}

	r := gin.New()
	ctrl.RegisterRoutes(r)

	srv := httptest.NewServer(r)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/binance"

	cleanup := func() {
		srv.Close()
		sim.Stop()
	}
	return wsURL, cleanup
}

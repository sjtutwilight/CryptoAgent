package handler

import "testing"

func TestParseAggTradePayloadCombinedStream(t *testing.T) {
	payload := []byte(`{"stream":"aaveusdt@aggTrade","data":{"e":"aggTrade","E":1700000000000,"a":123,"p":"100.1","q":"2.3","f":11,"l":12,"T":1700000000001,"m":true}}`)

	trades, err := parseAggTradePayload(payload, "")
	if err != nil {
		t.Fatalf("parse combined stream payload failed: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(trades))
	}
	if trades[0].EventType != "aggTrade" {
		t.Fatalf("expected event type aggTrade, got %s", trades[0].EventType)
	}
	if trades[0].Symbol != "AAVEUSDT" {
		t.Fatalf("expected symbol AAVEUSDT from stream fallback, got %s", trades[0].Symbol)
	}
}

func TestParseAggTradePayloadAppliesFallbackSymbol(t *testing.T) {
	payload := []byte(`{"e":"aggTrade","E":1700000000000,"a":123,"p":"100.1","q":"2.3","f":11,"l":12,"T":1700000000001,"m":true}`)

	trades, err := parseAggTradePayload(payload, "aaveusdt")
	if err != nil {
		t.Fatalf("parse payload failed: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(trades))
	}
	if trades[0].Symbol != "AAVEUSDT" {
		t.Fatalf("expected symbol AAVEUSDT from fallback, got %s", trades[0].Symbol)
	}
}

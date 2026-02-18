package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler/parser/binance"
	obslogging "github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

const (
	defaultOrderbookExchange = "binance"
	obTopicDiff              = "diff"
	obTopicSnapshot          = "snapshot"
)

type orderbookTopicRouter struct {
	symbol         string
	exchange       string
	snapshotSource string
	snapshotReason string
}

type orderbookDiffEvent struct {
	Symbol            string     `json:"symbol"`
	Exchange          string     `json:"exchange"`
	Snapshot          bool       `json:"snapshot"`
	FirstUpdateID     int64      `json:"first_update_id"`
	FinalUpdateID     int64      `json:"final_update_id"`
	PrevFinalUpdateID int64      `json:"prev_final_update_id"`
	Bids              [][]string `json:"bids,omitempty"`
	Asks              [][]string `json:"asks,omitempty"`
	ExchangeTS        int64      `json:"exchange_ts"`
	IngestTS          int64      `json:"ingest_ts"`
}

type orderbookSnapshotEvent struct {
	Symbol         string     `json:"symbol"`
	Exchange       string     `json:"exchange"`
	Snapshot       bool       `json:"snapshot"`
	LastUpdateID   int64      `json:"lastUpdateId"`
	SnapshotSource string     `json:"snapshot_source"`
	SnapshotReason string     `json:"snapshot_reason"`
	Bids           [][]string `json:"bids,omitempty"`
	Asks           [][]string `json:"asks,omitempty"`
	ExchangeTS     int64      `json:"exchange_ts"`
	IngestTS       int64      `json:"ingest_ts"`
}

func init() {
	Register("orderbook_topic_router", newOrderbookTopicRouter)
}

func newOrderbookTopicRouter(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	return &orderbookTopicRouter{
		symbol:         strings.ToUpper(getString(cfg, "symbol", "")),
		exchange:       strings.ToLower(getString(cfg, "exchange", defaultOrderbookExchange)),
		snapshotSource: strings.TrimSpace(getString(cfg, "snapshot_source", "")),
		snapshotReason: strings.TrimSpace(getString(cfg, "snapshot_reason", "")),
	}, nil
}

func (h *orderbookTopicRouter) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}
	if msg.Metadata == nil {
		msg.Metadata = map[string]any{}
	}

	if out, ok, err := h.handleDiff(msg); ok {
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		return []*types.Message{out}, nil
	}
	out, err := h.handleSnapshot(msg)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return []*types.Message{out}, nil
}

func (h *orderbookTopicRouter) handleDiff(msg *types.Message) (*types.Message, bool, error) {
	depth, ok := msg.Metadata["binance_depth"].(*binance.DepthMessage)
	if !ok || depth == nil {
		return nil, false, nil
	}
	if isSnapshotMeta(msg.Metadata) {
		return nil, false, nil
	}

	symbol := h.resolveSymbol(depth.Symbol, msg.Metadata)
	if symbol == "" {
		return nil, true, fmt.Errorf("orderbook_topic_router: diff symbol missing")
	}
	if h.symbol != "" && symbol != h.symbol {
		return nil, true, nil
	}
	exchange := h.resolveExchange(msg.Metadata)
	now := time.Now().UTC().UnixMilli()
	event := orderbookDiffEvent{
		Symbol:            symbol,
		Exchange:          exchange,
		Snapshot:          false,
		FirstUpdateID:     depth.FirstUpdateID,
		FinalUpdateID:     depth.FinalUpdateID,
		PrevFinalUpdateID: depth.PrevFinalUpdateID,
		Bids:              depth.Bids,
		Asks:              depth.Asks,
		ExchangeTS:        firstNonZeroInt64(depth.TransactionTime, depth.EventTime, toInt64(msg.Metadata["event_time"]), toInt64(msg.Metadata["exchange_ts"]), now),
		IngestTS:          now,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, true, fmt.Errorf("orderbook_topic_router: marshal diff payload failed: %w", err)
	}
	meta := copyMessageMetadata(msg.Metadata)
	meta["symbol"] = symbol
	meta["exchange"] = exchange
	meta["snapshot"] = false
	meta["first_update_id"] = event.FirstUpdateID
	meta["final_update_id"] = event.FinalUpdateID
	meta["prev_final_update_id"] = event.PrevFinalUpdateID
	meta["exchange_ts"] = event.ExchangeTS
	meta["ingest_ts"] = event.IngestTS
	meta["ob_topic"] = obTopicDiff

	return &types.Message{Metadata: meta, Payload: payload}, true, nil
}

func (h *orderbookTopicRouter) handleSnapshot(msg *types.Message) (*types.Message, error) {
	var snapshot binance.DepthSnapshotResponse
	if err := json.Unmarshal(msg.Payload, &snapshot); err != nil {
		return nil, fmt.Errorf("orderbook_topic_router: decode snapshot failed: %w", err)
	}
	if snapshot.Code != 0 {
		return nil, fmt.Errorf("orderbook_topic_router: snapshot api error code=%d msg=%s", snapshot.Code, snapshot.Msg)
	}

	symbol := h.resolveSymbol(snapshot.Symbol, msg.Metadata)
	if symbol == "" {
		return nil, fmt.Errorf("orderbook_topic_router: snapshot symbol missing")
	}
	if h.symbol != "" && symbol != h.symbol {
		return nil, nil
	}
	exchange := h.resolveExchange(msg.Metadata)
	lastUpdateID := snapshot.LastUpdateID
	if lastUpdateID == 0 {
		lastUpdateID = toInt64(msg.Metadata["final_update_id"])
	}
	if lastUpdateID == 0 {
		return nil, fmt.Errorf("orderbook_topic_router: snapshot lastUpdateId missing")
	}
	now := time.Now().UTC().UnixMilli()
	source, reason := h.resolveSnapshotMeta(msg.Metadata)
	event := orderbookSnapshotEvent{
		Symbol:         symbol,
		Exchange:       exchange,
		Snapshot:       true,
		LastUpdateID:   lastUpdateID,
		SnapshotSource: source,
		SnapshotReason: reason,
		Bids:           snapshot.Bids,
		Asks:           snapshot.Asks,
		ExchangeTS:     firstNonZeroInt64(snapshot.Transaction, snapshot.EventTime, toInt64(msg.Metadata["event_time"]), toInt64(msg.Metadata["exchange_ts"]), now),
		IngestTS:       now,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("orderbook_topic_router: marshal snapshot payload failed: %w", err)
	}
	meta := copyMessageMetadata(msg.Metadata)
	meta["symbol"] = symbol
	meta["exchange"] = exchange
	meta["snapshot"] = true
	meta["snapshot_source"] = source
	meta["snapshot_reason"] = reason
	meta["lastUpdateId"] = lastUpdateID
	meta["final_update_id"] = lastUpdateID
	meta["exchange_ts"] = event.ExchangeTS
	meta["ingest_ts"] = event.IngestTS
	meta["ob_topic"] = obTopicSnapshot

	roleID := util.ToString(meta["role_id"])
	metrics.RecordOrderbookSnapshotEmitted(roleID, source, reason)
	obslogging.Info(context.Background(), obslogging.EventOrderbookSnapshotEmit, "orderbook snapshot emitted", obslogging.Fields{
		"role_id":         roleID,
		"symbol":          symbol,
		"exchange":        exchange,
		"snapshot_source": source,
		"snapshot_reason": reason,
		"last_update_id":  lastUpdateID,
	})

	return &types.Message{Metadata: meta, Payload: payload}, nil
}

func (h *orderbookTopicRouter) resolveSymbol(payloadSymbol string, meta map[string]any) string {
	symbol := strings.ToUpper(strings.TrimSpace(payloadSymbol))
	if symbol == "" {
		symbol = strings.ToUpper(strings.TrimSpace(util.ToString(meta["symbol"])))
	}
	if symbol == "" {
		symbol = strings.ToUpper(strings.TrimSpace(util.ToString(meta["binance_symbol"])))
	}
	if symbol == "" {
		symbol = h.symbol
	}
	return symbol
}

func (h *orderbookTopicRouter) resolveExchange(meta map[string]any) string {
	exchange := strings.ToLower(strings.TrimSpace(util.ToString(meta["exchange"])))
	if exchange == "" {
		exchange = h.exchange
	}
	if exchange == "" {
		exchange = defaultOrderbookExchange
	}
	return exchange
}

func (h *orderbookTopicRouter) resolveSnapshotMeta(meta map[string]any) (string, string) {
	source := strings.TrimSpace(util.ToString(meta["snapshot_source"]))
	reason := strings.TrimSpace(util.ToString(meta["snapshot_reason"]))
	if source == "" {
		source = h.snapshotSource
	}
	if reason == "" {
		reason = h.snapshotReason
	}
	if source == "" {
		if isBackfillSnapshot(meta) {
			source = "backfill"
		} else {
			source = "periodic"
		}
	}
	if reason == "" {
		if source == "periodic" {
			reason = "periodic"
		} else {
			reason = "gap"
		}
	}
	return source, reason
}

func copyMessageMetadata(meta map[string]any) map[string]any {
	if meta == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(meta)+8)
	for k, v := range meta {
		out[k] = v
	}
	return out
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

func toInt64(v any) int64 {
	switch vv := v.(type) {
	case int64:
		return vv
	case int:
		return int64(vv)
	case float64:
		return int64(vv)
	case float32:
		return int64(vv)
	case json.Number:
		n, _ := vv.Int64()
		return n
	case string:
		return util.ToInt64(vv)
	default:
		return 0
	}
}

func isSnapshotMeta(meta map[string]any) bool {
	if meta == nil {
		return false
	}
	if snapshot, ok := meta["snapshot"].(bool); ok && snapshot {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(util.ToString(meta["backfill_type"])), types.BackfillTypeSnapshot) {
		return true
	}
	return false
}

func isBackfillSnapshot(meta map[string]any) bool {
	if meta == nil {
		return false
	}
	if isBackfill, ok := meta["is_backfill"].(bool); ok && isBackfill {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(util.ToString(meta["backfill_type"])), types.BackfillTypeSnapshot) {
		return true
	}
	return false
}

package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler/parser/binance"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/resource/orderbook"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

const (
	defaultSnapshotURL = "https://fapi.binance.com/fapi/v1/depth"
	defaultExchange    = "binance"
)

type orderbookDiffHandler struct {
	symbol    string
	exchange  string
	engine    *orderbook.Engine
	listeners []SnapshotListener
}

type orderbookValidator struct{}

type orderbookEvent struct {
	Symbol     string       `json:"symbol"`
	Exchange   string       `json:"exchange"`
	Snapshot   bool         `json:"snapshot"`
	Depth      depthPayload `json:"depth"`
	Seq        int64        `json:"seq"`
	ExchangeTS int64        `json:"exchange_ts"`
	IngestTS   int64        `json:"ingest_ts"`
}

type depthPayload struct {
	Bids [][]string `json:"bids"`
	Asks [][]string `json:"asks"`
}

func init() {
	Register("orderbook_diff", newOrderbookDiffHandler)
	Register("orderbook_validator", newOrderbookValidator)
}

func newOrderbookDiffHandler(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	symbol := strings.ToUpper(getString(cfg, "symbol", ""))
	if symbol == "" {
		return nil, fmt.Errorf("orderbook_diff: symbol required")
	}
	maxDepth := getInt(cfg, "max_depth", 100)
	h := &orderbookDiffHandler{
		symbol:   symbol,
		exchange: getString(cfg, "exchange", defaultExchange),
		engine:   orderbook.NewEngine(symbol, maxDepth),
	}
	return h, nil
}

func newOrderbookValidator(cfg map[string]any) (Handler, error) {
	return &orderbookValidator{}, nil
}

func (h *orderbookDiffHandler) SetSnapshotListener(listener SnapshotListener) {
	if listener == nil {
		return
	}
	h.listeners = append(h.listeners, listener)
}

func (h *orderbookDiffHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}

	// 检查是否为 backfill 的 snapshot 消息
	if isBackfill, _ := msg.Metadata["is_backfill"].(bool); isBackfill {
		if isSnapshot, _ := msg.Metadata["snapshot"].(bool); isSnapshot {
			log.Printf("[orderbook_diff] 收到快照消息: symbol=%s", h.symbol)
			out, lastUpdateID, err := h.applySnapshotFromMessage(msg)
			if err != nil {
				return nil, err
			}

			if len(h.listeners) > 0 {
				for _, listener := range h.listeners {
					if listener == nil {
						continue
					}
					ready := listener.OnSnapshotApplied(uint64(lastUpdateID))
					for _, readyMsg := range ready {
						diffOut, err := h.handleDiffMessage(readyMsg)
						if err != nil {
							return nil, err
						}
						out = append(out, diffOut...)
					}
				}
			}
			return out, nil
		}
	}

	return h.handleDiffMessage(msg)
}

// applySnapshotFromMessage 处理从 backfill 返回的快照消息
func (h *orderbookDiffHandler) applySnapshotFromMessage(msg *types.Message) ([]*types.Message, int64, error) {
	var snapshot binance.DepthSnapshotResponse
	if err := json.Unmarshal(msg.Payload, &snapshot); err != nil {
		return nil, 0, fmt.Errorf("orderbook_diff: decode snapshot failed: %w", err)
	}
	if snapshot.Code != 0 {
		return nil, 0, fmt.Errorf("orderbook_diff: snapshot api error code=%d msg=%s", snapshot.Code, snapshot.Msg)
	}
	if snapshot.LastUpdateID == 0 {
		return nil, 0, fmt.Errorf("orderbook_diff: snapshot missing lastUpdateId")
	}

	log.Printf("[orderbook_diff] 应用快照: symbol=%s lastUpdateID=%d bids=%d asks=%d",
		h.symbol, snapshot.LastUpdateID, len(snapshot.Bids), len(snapshot.Asks))

	book, err := h.engine.ApplySnapshot(orderbook.Snapshot{
		LastUpdateID: snapshot.LastUpdateID,
		Bids:         toLevels(snapshot.Bids),
		Asks:         toLevels(snapshot.Asks),
	})
	if err != nil {
		return nil, 0, err
	}

	log.Printf("[orderbook_diff] 快照应用成功: symbol=%s lastUpdateID=%d", h.symbol, h.engine.LastUpdateID())

	exchangeTS := snapshot.Transaction
	if exchangeTS == 0 {
		exchangeTS = snapshot.EventTime
	}
	if exchangeTS == 0 {
		exchangeTS = time.Now().UTC().UnixMilli()
	}

	out, err := buildOrderbookMessage(book, h.symbol, h.exchange, exchangeTS)
	if err != nil {
		return nil, 0, err
	}
	return []*types.Message{out}, snapshot.LastUpdateID, nil
}

func (h *orderbookDiffHandler) handleDiffMessage(msg *types.Message) ([]*types.Message, error) {
	// 依赖上游 parser handler 已将数据解析并放入 metadata
	// 确保配置中 binance_parser 在 orderbook_diff 之前执行
	if msg.Metadata == nil {
		return nil, fmt.Errorf("orderbook_diff: metadata 缺失，请确保上游 parser handler 已配置")
	}

	depth, ok := msg.Metadata["binance_depth"].(*binance.DepthMessage)
	if !ok || depth == nil {
		return nil, fmt.Errorf("orderbook_diff: binance_depth 未找到或类型错误，请确保上游 binance_parser 已配置")
	}

	if depth.Symbol != "" && strings.ToUpper(depth.Symbol) != h.symbol {
		return nil, nil
	}

	book, applied, err := h.engine.ApplyDiff(orderbook.Diff{
		FirstUpdateID: depth.FirstUpdateID,
		FinalUpdateID: depth.FinalUpdateID,
		PrevFinalID:   depth.PrevFinalUpdateID,
		Bids:          toLevels(depth.Bids),
		Asks:          toLevels(depth.Asks),
	})
	if err != nil {
		switch err {
		case orderbook.ErrNoSnapshot:
			return nil, nil
		case orderbook.ErrStaleUpdate:
			return nil, nil
		case orderbook.ErrSequenceGap:
			log.Printf("[orderbook_diff] sequence gap for %s: U=%d u=%d pu=%d last=%d",
				h.symbol, depth.FirstUpdateID, depth.FinalUpdateID, depth.PrevFinalUpdateID, h.engine.LastUpdateID())
			return nil, nil
		default:
			return nil, err
		}
	}
	if !applied {
		log.Printf("[orderbook_diff] diff 未应用: symbol=%s U=%d u=%d last=%d",
			h.symbol, depth.FirstUpdateID, depth.FinalUpdateID, h.engine.LastUpdateID())
		return nil, nil
	}

	log.Printf("[orderbook_diff] diff 应用成功: symbol=%s U=%d u=%d last=%d",
		h.symbol, depth.FirstUpdateID, depth.FinalUpdateID, h.engine.LastUpdateID())

	exchangeTS := depth.TransactionTime
	if exchangeTS == 0 {
		exchangeTS = depth.EventTime
	}
	if exchangeTS == 0 {
		exchangeTS = time.Now().UTC().UnixMilli()
	}

	out, err := buildOrderbookMessage(book, h.symbol, h.exchange, exchangeTS)
	if err != nil {
		return nil, err
	}
	return []*types.Message{out}, nil
}

func (v *orderbookValidator) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}

	var payload orderbookEvent
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, fmt.Errorf("orderbook_validator: invalid payload: %w", err)
	}
	if payload.Symbol == "" || payload.ExchangeTS == 0 {
		return nil, fmt.Errorf("orderbook_validator: missing symbol or exchange_ts")
	}
	return []*types.Message{msg}, nil
}

func buildOrderbookMessage(book orderbook.Book, symbol, exchange string, exchangeTS int64) (*types.Message, error) {
	if exchange == "" {
		exchange = defaultExchange
	}
	now := time.Now().UTC().UnixMilli()
	event := orderbookEvent{
		Symbol:     symbol,
		Exchange:   exchange,
		Snapshot:   book.Snapshot,
		Depth:      depthPayload{Bids: book.Bids, Asks: book.Asks},
		Seq:        book.Seq,
		ExchangeTS: exchangeTS,
		IngestTS:   now,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("orderbook: marshal payload failed: %w", err)
	}
	meta := map[string]any{
		"symbol":      symbol,
		"exchange":    exchange,
		"seq":         book.Seq,
		"snapshot":    book.Snapshot,
		"exchange_ts": exchangeTS,
	}
	return &types.Message{
		Metadata: meta,
		Payload:  payload,
	}, nil
}

func toLevels(v [][]string) []orderbook.Level {
	if len(v) == 0 {
		return nil
	}
	out := make([]orderbook.Level, 0, len(v))
	for _, item := range v {
		if len(item) < 2 {
			continue
		}
		out = append(out, orderbook.Level{
			Price: item[0],
			Size:  item[1],
		})
	}
	return out
}

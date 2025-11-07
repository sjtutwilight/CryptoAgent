package handler

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler/parser/hyperliquid"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

type hyperliquidOrderbookHandler struct {
	exchange string
	symbol   string
	maxDepth int
}

type hyperliquidTradeHandler struct {
	exchange string
	symbol   string
}

type hyperliquidAssetCtxHandler struct {
	exchange        string
	symbol          string
	kind            string
	outputs         map[string]string
	fundingInterval string
	coins           map[string]struct{}
}

func init() {
	Register("hyperliquid_orderbook", newHyperliquidOrderbookHandler)
	Register("hyperliquid_trade", newHyperliquidTradeHandler)
	Register("hyperliquid_asset_ctx", newHyperliquidAssetCtxHandler)
}

func newHyperliquidOrderbookHandler(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	return &hyperliquidOrderbookHandler{
		exchange: util.GetString(cfg, "exchange", "hyperliquid"),
		symbol:   strings.ToUpper(util.GetString(cfg, "symbol", "")),
		maxDepth: util.GetInt(cfg, "max_depth", 200),
	}, nil
}

func newHyperliquidTradeHandler(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	return &hyperliquidTradeHandler{
		exchange: util.GetString(cfg, "exchange", "hyperliquid"),
		symbol:   strings.ToUpper(util.GetString(cfg, "symbol", "")),
	}, nil
}

func newHyperliquidAssetCtxHandler(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	outputs := map[string]string{}
	if outCfg, ok := cfg["outputs"].(map[string]any); ok {
		for k, v := range outCfg {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				outputs[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(s)
			}
		}
	}
	coins := map[string]struct{}{}
	if arr, ok := cfg["coins"].([]any); ok {
		for _, item := range arr {
			if s := strings.TrimSpace(util.ToString(item)); s != "" {
				coins[strings.ToUpper(s)] = struct{}{}
			}
		}
	} else if arr, ok := cfg["coins"].([]string); ok {
		for _, item := range arr {
			if s := strings.TrimSpace(item); s != "" {
				coins[strings.ToUpper(s)] = struct{}{}
			}
		}
	}
	kind := strings.ToLower(util.GetString(cfg, "kind", "mark_index"))
	if len(outputs) == 0 {
		switch kind {
		case "mark_index", "funding_rate", "open_interest":
		default:
			return nil, fmt.Errorf("hyperliquid_asset_ctx: unsupported kind %s", kind)
		}
	}
	return &hyperliquidAssetCtxHandler{
		exchange:        util.GetString(cfg, "exchange", "hyperliquid"),
		symbol:          strings.ToUpper(util.GetString(cfg, "symbol", "")),
		kind:            kind,
		outputs:         outputs,
		fundingInterval: util.GetString(cfg, "funding_interval", "1h"),
		coins:           coins,
	}, nil
}

func (h *hyperliquidOrderbookHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil {
		return nil, nil
	}
	var snapshot *hyperliquid.OrderbookSnapshot
	if msg.Metadata != nil {
		if v, ok := msg.Metadata["hyperliquid_orderbook"].(*hyperliquid.OrderbookSnapshot); ok {
			snapshot = v
		}
	}
	if snapshot == nil {
		decoded, err := hyperliquid.DecodeOrderbookMessage(msg.Payload)
		if err != nil {
			return nil, err
		}
		snapshot = decoded
	}
	if snapshot == nil {
		return nil, nil
	}
	symbol := strings.ToUpper(snapshot.Coin)
	if h.symbol != "" && symbol != h.symbol {
		return nil, nil
	}
	bids := trimDepth(snapshot.Bids, h.maxDepth)
	asks := trimDepth(snapshot.Asks, h.maxDepth)
	exchangeTS := util.FirstNonZero(snapshot.Time, time.Now().UTC().UnixMilli())
	seq := snapshot.Sequence
	if seq == 0 {
		seq = exchangeTS
	}
	event := orderbookEvent{
		Symbol:   symbol,
		Exchange: util.FirstNonEmpty(h.exchange, "hyperliquid"),
		Snapshot: true,
		Depth: depthPayload{
			Bids: bids,
			Asks: asks,
		},
		Seq:        seq,
		ExchangeTS: exchangeTS,
		IngestTS:   time.Now().UTC().UnixMilli(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      symbol,
		"exchange":    event.Exchange,
		"snapshot":    true,
		"seq":         event.Seq,
		"exchange_ts": event.ExchangeTS,
	}
	return []*types.Message{{Metadata: meta, Payload: payload}}, nil
}

func (h *hyperliquidTradeHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil {
		return nil, nil
	}
	var trade *hyperliquid.TradeMessage
	if msg.Metadata != nil {
		if v, ok := msg.Metadata["hyperliquid_trade"].(*hyperliquid.TradeMessage); ok {
			trade = v
		}
	}
	if trade == nil {
		decoded, err := hyperliquid.DecodeTradeMessage(msg.Payload)
		if err != nil {
			return nil, err
		}
		if len(decoded) > 0 {
			trade = decoded[0]
		}
	}
	if trade == nil {
		return nil, nil
	}
	symbol := strings.ToUpper(trade.Coin)
	if h.symbol != "" && symbol != h.symbol {
		return nil, nil
	}
	side := normaliseTradeSide(trade.Side)
	if side == "unknown" {
		return nil, nil
	}
	buyerMaker := side == "sell"
	event := tradeEvent{
		Symbol:     symbol,
		Exchange:   util.FirstNonEmpty(h.exchange, "hyperliquid"),
		Price:      trade.Px,
		Size:       trade.Sz,
		Side:       side,
		BuyerMaker: buyerMaker,
		ExchangeTS: util.FirstNonZero(trade.Time, trade.Seq),
		IngestTS:   time.Now().UTC().UnixMilli(),
	}
	if tradeID, err := util.ParseUintString(trade.TID); err == nil {
		if tradeID > math.MaxInt64 {
			event.TradeID = int64(tradeID % math.MaxInt64)
		} else {
			event.TradeID = int64(tradeID)
		}
	} else {
		event.TradeID = trade.Seq
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      symbol,
		"exchange":    event.Exchange,
		"trade_id":    event.TradeID,
		"exchange_ts": event.ExchangeTS,
		"side":        side,
	}
	return []*types.Message{{Metadata: meta, Payload: payload}}, nil
}

func (h *hyperliquidAssetCtxHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil {
		return nil, nil
	}
	ctxs, err := h.collectContexts(msg)
	if err != nil {
		return nil, err
	}
	if len(ctxs) == 0 {
		return nil, nil
	}
	out := make([]*types.Message, 0, len(ctxs))
	for _, ctx := range ctxs {
		if ctx == nil {
			continue
		}
		symbol := strings.ToUpper(ctx.Coin)
		if symbol == "" {
			continue
		}
		if len(h.coins) > 0 {
			if _, ok := h.coins[symbol]; !ok {
				continue
			}
		}
		if h.symbol != "" && symbol != h.symbol {
			continue
		}
		if len(h.outputs) > 0 {
			for kind, topic := range h.outputs {
				evtMsg, err := h.buildEvent(kind, symbol, ctx)
				if err != nil {
					return nil, err
				}
				if evtMsg == nil {
					continue
				}
				if evtMsg.Metadata == nil {
					evtMsg.Metadata = map[string]any{}
				}
				if topic != "" {
					evtMsg.Metadata["sink_topic"] = topic
				}
				out = append(out, evtMsg)
			}
			continue
		}
		evtMsg, err := h.buildEvent(h.kind, symbol, ctx)
		if err != nil {
			return nil, err
		}
		if evtMsg != nil {
			out = append(out, evtMsg)
		}
	}
	return out, nil
}

func (h *hyperliquidAssetCtxHandler) collectContexts(msg *types.Message) ([]*hyperliquid.AssetContext, error) {
	var ctxs []*hyperliquid.AssetContext
	if msg.Metadata != nil {
		if ctx, ok := msg.Metadata["hyperliquid_asset_ctx"].(*hyperliquid.AssetContext); ok {
			ctxs = append(ctxs, ctx)
		}
		if list, ok := msg.Metadata["hyperliquid_asset_ctxs"].([]*hyperliquid.AssetContext); ok {
			ctxs = append(ctxs, list...)
		}
	}
	if len(ctxs) == 0 && len(msg.Payload) > 0 {
		parsed, err := hyperliquid.DecodeMetaAndAssetCtxs(msg.Payload)
		if err != nil {
			return nil, err
		}
		ctxs = append(ctxs, parsed...)
	}
	if len(h.coins) > 0 && len(ctxs) > 0 {
		filtered := make([]*hyperliquid.AssetContext, 0, len(h.coins))
		for _, ctx := range ctxs {
			if ctx == nil {
				continue
			}
			if _, ok := h.coins[strings.ToUpper(ctx.Coin)]; ok {
				filtered = append(filtered, ctx)
			}
		}
		ctxs = filtered
	}
	return ctxs, nil
}

func (h *hyperliquidAssetCtxHandler) buildEvent(kind, symbol string, ctx *hyperliquid.AssetContext) (*types.Message, error) {
	switch kind {
	case "mark_index":
		return h.buildMarkIndexEvent(symbol, ctx)
	case "funding_rate":
		return h.buildFundingEvent(symbol, ctx)
	case "open_interest":
		return h.buildOpenInterestEvent(symbol, ctx)
	default:
		return nil, fmt.Errorf("hyperliquid_asset_ctx: unsupported kind %s", kind)
	}
}

func (h *hyperliquidAssetCtxHandler) buildMarkIndexEvent(symbol string, ctx *hyperliquid.AssetContext) (*types.Message, error) {
	event := markIndexEvent{
		Symbol:          symbol,
		Exchange:        util.FirstNonEmpty(h.exchange, "hyperliquid"),
		MarkPrice:       ctx.MarkPx,
		IndexPrice:      util.FirstNonEmpty(ctx.OraclePx, ctx.MarkPx),
		FairBasis:       diffDecimal(ctx.MarkPx, ctx.OraclePx),
		LastFundingRate: ctx.Funding,
		NextFundingTime: 0,
		ExchangeTS:      util.FirstNonZero(ctx.Timestamp, time.Now().UTC().UnixMilli()),
		IngestTS:        time.Now().UTC().UnixMilli(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      symbol,
		"exchange":    event.Exchange,
		"exchange_ts": event.ExchangeTS,
	}
	return &types.Message{Metadata: meta, Payload: payload}, nil
}

func (h *hyperliquidAssetCtxHandler) buildFundingEvent(symbol string, ctx *hyperliquid.AssetContext) (*types.Message, error) {
	event := fundingEvent{
		Symbol:          symbol,
		Exchange:        util.FirstNonEmpty(h.exchange, "hyperliquid"),
		FundingRate:     ctx.Funding,
		FundingTime:     util.FirstNonZero(ctx.Timestamp, time.Now().UTC().UnixMilli()),
		FundingInterval: h.fundingInterval,
		ExchangeTS:      util.FirstNonZero(ctx.Timestamp, time.Now().UTC().UnixMilli()),
		IngestTS:        time.Now().UTC().UnixMilli(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      symbol,
		"exchange":    event.Exchange,
		"exchange_ts": event.ExchangeTS,
	}
	return &types.Message{Metadata: meta, Payload: payload}, nil
}

func (h *hyperliquidAssetCtxHandler) buildOpenInterestEvent(symbol string, ctx *hyperliquid.AssetContext) (*types.Message, error) {
	event := oiEvent{
		Symbol:     symbol,
		Exchange:   util.FirstNonEmpty(h.exchange, "hyperliquid"),
		OI:         ctx.OpenInterest,
		OIUSD:      ctx.OpenInterest,
		ExchangeTS: util.FirstNonZero(ctx.Timestamp, time.Now().UTC().UnixMilli()),
		IngestTS:   time.Now().UTC().UnixMilli(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      symbol,
		"exchange":    event.Exchange,
		"exchange_ts": event.ExchangeTS,
	}
	return &types.Message{Metadata: meta, Payload: payload}, nil
}

func trimDepth(levels [][]string, maxDepth int) [][]string {
	if maxDepth <= 0 || len(levels) <= maxDepth {
		return levels
	}
	out := make([][]string, 0, maxDepth)
	for i := 0; i < maxDepth && i < len(levels); i++ {
		level := levels[i]
		copied := make([]string, len(level))
		copy(copied, level)
		out = append(out, copied)
	}
	return out
}

func normaliseTradeSide(side string) string {
	switch strings.ToLower(strings.TrimSpace(side)) {
	case "b", "buy", "long":
		return "buy"
	case "s", "sell", "short":
		return "sell"
	default:
		return "unknown"
	}
}

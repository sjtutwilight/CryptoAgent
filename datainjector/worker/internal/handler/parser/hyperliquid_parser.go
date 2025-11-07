package parser

import (
	"fmt"
	"strings"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler/parser/hyperliquid"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

// HyperliquidParser normalises Hyperliquid websocket payloads into metadata objects.
type HyperliquidParser struct {
	kind     string
	exchange string
}

// NewHyperliquidParser creates a parser with the configured kind.
func NewHyperliquidParser(cfg map[string]any) (*HyperliquidParser, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	kind := strings.ToLower(util.GetString(cfg, "kind", ""))
	if kind == "" {
		return nil, fmt.Errorf("hyperliquid_parser: kind required (orderbook/trade/asset_ctx)")
	}
	return &HyperliquidParser{
		kind:     kind,
		exchange: util.GetString(cfg, "exchange", "hyperliquid"),
	}, nil
}

// Handle parses the payload and enriches metadata.
func (p *HyperliquidParser) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}
	if msg.Metadata == nil {
		msg.Metadata = map[string]any{}
	}

	switch p.kind {
	case "orderbook":
		book, err := hyperliquid.DecodeOrderbookMessage(msg.Payload)
		if err != nil {
			return nil, err
		}
		if book == nil {
			return nil, nil
		}
		symbol := strings.ToUpper(book.Coin)
		msg.Metadata["hyperliquid_orderbook"] = book
		msg.Metadata["hyperliquid_symbol"] = symbol
		msg.Metadata["exchange"] = p.exchange
		msg.Metadata["symbol"] = symbol
		msg.Metadata["snapshot"] = true
		if book.Time != 0 {
			msg.Metadata["exchange_ts"] = book.Time
		}
		msg.Metadata["hyperliquid_seq"] = book.Sequence
	case "trade":
		trades, err := hyperliquid.DecodeTradeMessage(msg.Payload)
		if err != nil {
			return nil, err
		}
		if len(trades) == 0 {
			return nil, nil
		}
		out := make([]*types.Message, 0, len(trades))
		for _, trade := range trades {
			if trade == nil {
				continue
			}
			symbol := strings.ToUpper(trade.Coin)
			meta := util.CopyMap(msg.Metadata)
			if meta == nil {
				meta = map[string]any{}
			}
			meta["hyperliquid_trade"] = trade
			meta["hyperliquid_symbol"] = symbol
			meta["exchange"] = p.exchange
			meta["symbol"] = symbol
			if trade.Time != 0 {
				meta["exchange_ts"] = trade.Time
			}
			meta["hyperliquid_seq"] = trade.Seq
			out = append(out, &types.Message{
				Metadata: meta,
				Payload:  msg.Payload,
			})
		}
		if len(out) == 0 {
			return nil, nil
		}
		return out, nil
	case "asset_ctx":
		ctx, err := hyperliquid.DecodeAssetCtxMessage(msg.Payload)
		if err != nil {
			return nil, err
		}
		if ctx == nil {
			return nil, nil
		}
		symbol := strings.ToUpper(ctx.Coin)
		msg.Metadata["hyperliquid_asset_ctx"] = ctx
		msg.Metadata["hyperliquid_symbol"] = symbol
		msg.Metadata["exchange"] = p.exchange
		msg.Metadata["symbol"] = symbol
		if ctx.Timestamp != 0 {
			msg.Metadata["exchange_ts"] = ctx.Timestamp
		}
	default:
		return nil, fmt.Errorf("hyperliquid_parser: unsupported kind=%s", p.kind)
	}

	return []*types.Message{msg}, nil
}

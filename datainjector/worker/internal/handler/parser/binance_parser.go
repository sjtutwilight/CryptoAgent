package parser

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler/parser/binance"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

// BinanceParser 解析 Binance 原始消息，支持多种数据类型
// 支持的 kind: depth, trade, mark_index, liquidation, kline
type BinanceParser struct {
	kind           string // 数据类型：depth/trade/mark_index/liquidation/kline
	dropUnfinished bool   // 仅用于 kline：是否丢弃未完成的 K 线
	includeRaw     bool   // 仅用于 kline：是否包含原始 payload
	exchange       string // 仅用于 kline：交易所名称
}

// NewBinanceParser 创建 Binance 解析器
func NewBinanceParser(cfg map[string]any) (*BinanceParser, error) {
	kind := strings.ToLower(util.GetString(cfg, "kind", ""))
	if kind == "" {
		return nil, fmt.Errorf("binance_parser: kind required (depth/trade/mark_index/liquidation/kline)")
	}

	p := &BinanceParser{
		kind:           kind,
		dropUnfinished: util.GetBool(cfg, "drop_unfinished", true),
		includeRaw:     util.GetBool(cfg, "include_raw_payload", false),
		exchange:       util.GetString(cfg, "exchange", "binance"),
	}

	return p, nil
}

// Handle 根据 kind 类型分发到不同的解析逻辑
func (p *BinanceParser) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}
	if msg.Metadata == nil {
		msg.Metadata = map[string]any{}
	}

	switch p.kind {
	case "depth":
		return p.handleDepth(msg)
	case "trade":
		return p.handleTrade(msg)
	case "mark_index":
		return p.handleMarkIndex(msg)
	case "liquidation":
		return p.handleLiquidation(msg)
	case "kline":
		return p.handleKline(msg)
	default:
		return nil, fmt.Errorf("binance_parser: unsupported kind=%s", p.kind)
	}
}

// handleDepth 处理深度数据
func (p *BinanceParser) handleDepth(msg *types.Message) ([]*types.Message, error) {
	// 快照消息需要提取序列号，让数据正确性模块判断快照位置
	if snapshot, ok := msg.Metadata["snapshot"].(bool); ok && snapshot {
		var snapshotData map[string]any
		if err := json.Unmarshal(msg.Payload, &snapshotData); err == nil {
			if lastUpdateID, ok := snapshotData["lastUpdateId"].(float64); ok {
				msg.Metadata["final_update_id"] = int64(lastUpdateID)
			}
		}
		return []*types.Message{msg}, nil
	}

	depth, stream, err := binance.DecodeDepthMessage(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("binance_parser depth decode failed: %w", err)
	}
	if depth != nil {
		msg.Metadata["binance_depth"] = depth
		msg.Metadata["binance_symbol"] = strings.ToUpper(depth.Symbol)
		msg.Metadata["final_update_id"] = depth.FinalUpdateID // 提取序列号供 integrity handler 使用
		msg.Metadata["first_update_id"] = depth.FirstUpdateID
		msg.Metadata["prev_final_update_id"] = depth.PrevFinalUpdateID
		if stream != "" {
			msg.Metadata["binance_stream"] = stream
		}
	}
	return []*types.Message{msg}, nil
}

// handleTrade 处理交易数据
func (p *BinanceParser) handleTrade(msg *types.Message) ([]*types.Message, error) {
	trade, stream, err := binance.DecodeTradeMessage(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("binance_parser trade decode failed: %w", err)
	}
	if trade != nil {
		msg.Metadata["binance_trade"] = trade
		msg.Metadata["binance_symbol"] = strings.ToUpper(trade.Symbol)
		if stream != "" {
			msg.Metadata["binance_stream"] = stream
		}
	}
	return []*types.Message{msg}, nil
}

// handleMarkIndex 处理标记价格和资金费率数据
func (p *BinanceParser) handleMarkIndex(msg *types.Message) ([]*types.Message, error) {
	mark, stream, err := binance.DecodeMarkIndexMessage(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("binance_parser mark_index decode failed: %w", err)
	}
	if mark != nil {
		msg.Metadata["binance_mark_index"] = mark
		msg.Metadata["binance_symbol"] = strings.ToUpper(mark.Symbol)
		if stream != "" {
			msg.Metadata["binance_stream"] = stream
		}
	}
	return []*types.Message{msg}, nil
}

// handleLiquidation 处理强平数据
func (p *BinanceParser) handleLiquidation(msg *types.Message) ([]*types.Message, error) {
	liq, stream, err := binance.DecodeLiquidationMessage(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("binance_parser liquidation decode failed: %w", err)
	}
	if liq != nil {
		msg.Metadata["binance_liquidation"] = liq
		msg.Metadata["binance_symbol"] = strings.ToUpper(liq.Symbol)
		if stream != "" {
			msg.Metadata["binance_stream"] = stream
		}
	}
	return []*types.Message{msg}, nil
}

// handleKline 处理 K 线数据并标准化
func (p *BinanceParser) handleKline(msg *types.Message) ([]*types.Message, error) {
	var root map[string]any
	if err := json.Unmarshal(msg.Payload, &root); err != nil {
		return nil, fmt.Errorf("binance_parser kline: invalid json payload: %w", err)
	}

	payload := util.ExtractMap(root, "data")
	if payload == nil {
		payload = root
	}

	klineSource := util.ExtractMap(payload, "k")
	if klineSource == nil {
		klineSource = util.ExtractMap(payload, "kline")
	}
	if klineSource == nil {
		// 非 kline 数据或订阅确认消息，直接忽略
		return nil, nil
	}

	// 如果配置为丢弃未完成的 K 线
	if p.dropUnfinished {
		if finished, ok := util.ToBool(klineSource["x"]); ok && !finished {
			return nil, nil
		}
	}

	// 提取并标准化字段
	exchange := util.FirstNonEmpty(util.ToString(payload["exchange"]), util.ToString(payload["datasource_id"]), p.exchange)
	symbol := util.FirstNonEmpty(
		util.ToString(payload["symbol"]),
		util.ToString(payload["s"]),
		util.ToString(klineSource["s"]),
	)
	if symbol == "" {
		symbol = "UNKNOWN"
	}

	interval := util.FirstNonEmpty(
		util.ToString(payload["interval"]),
		util.ToString(klineSource["i"]),
	)
	if interval == "" {
		interval = "UNKNOWN"
	}

	eventTime := util.FirstNonZero(
		util.ToInt64(payload["eventTime"]),
		util.ToInt64(payload["E"]),
		util.ToInt64(root["eventTime"]),
		util.ToInt64(root["E"]),
	)

	ingestTime := util.FirstNonZero(
		util.ToInt64(payload["ingestTime"]),
		util.ToInt64(root["ingestTime"]),
	)
	if ingestTime == 0 {
		// 使用当前时间的毫秒时间戳
		ingestTime = util.ToInt64(msg.Metadata["ingest_time"])
	}

	startTime := util.ToInt64(klineSource["t"])
	closeTime := util.ToInt64(klineSource["T"])
	openPrice := util.ToString(klineSource["o"])
	closePrice := util.ToString(klineSource["c"])
	highPrice := util.ToString(klineSource["h"])
	lowPrice := util.ToString(klineSource["l"])
	baseVolume := util.ToString(klineSource["v"])
	quoteVolume := util.FirstNonEmpty(util.ToString(klineSource["q"]), util.ToString(klineSource["Q"]))
	tradeCount := util.ToInt(klineSource["n"])
	closed, _ := util.ToBool(klineSource["x"])

	if baseVolume == "" {
		baseVolume = "0"
	}
	if quoteVolume == "" {
		quoteVolume = "0"
	}

	normalized := map[string]any{
		"exchange":   exchange,
		"symbol":     symbol,
		"interval":   interval,
		"eventTime":  eventTime,
		"ingestTime": ingestTime,
		"kline": map[string]any{
			"startTime":   startTime,
			"closeTime":   closeTime,
			"openPrice":   openPrice,
			"closePrice":  closePrice,
			"highPrice":   highPrice,
			"lowPrice":    lowPrice,
			"baseVolume":  baseVolume,
			"quoteVolume": quoteVolume,
			"tradeCount":  tradeCount,
			"closed":      closed,
		},
	}

	if p.includeRaw {
		normalized["raw"] = root
	}

	payloadBytes, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("binance_parser kline: marshal normalized payload failed: %w", err)
	}

	meta := util.CopyMap(msg.Metadata)
	meta["exchange"] = exchange
	meta["symbol"] = symbol
	meta["interval"] = interval
	if eventTime != 0 {
		meta["event_time"] = eventTime
	}
	if startTime != 0 {
		meta["start_time"] = startTime
	}
	if closeTime != 0 {
		meta["close_time"] = closeTime
	}

	msg.Metadata = meta
	msg.Payload = payloadBytes
	return []*types.Message{msg}, nil
}

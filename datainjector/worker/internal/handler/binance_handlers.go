package handler

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler/parser/binance"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

const defaultExchange = "binance"

type tradeEvent struct {
	Symbol      string `json:"symbol"`
	Exchange    string `json:"exchange"`
	Price       string `json:"price"`
	Size        string `json:"size"`
	Side        string `json:"side"`
	BuyerMaker  bool   `json:"buyer_maker"`
	ExchangeTS  int64  `json:"exchange_ts"`
	IngestTS    int64  `json:"ingest_ts"`
	TradeID     int64  `json:"trade_id"`
	BuyerOrder  int64  `json:"buyer_order_id"`
	SellerOrder int64  `json:"seller_order_id"`
}

type markIndexEvent struct {
	Symbol          string `json:"symbol"`
	Exchange        string `json:"exchange"`
	MarkPrice       string `json:"mark_price"`
	IndexPrice      string `json:"index_price"`
	FairBasis       string `json:"fair_basis"`
	LastFundingRate string `json:"last_funding_rate"`
	NextFundingTime int64  `json:"next_funding_time"`
	ExchangeTS      int64  `json:"exchange_ts"`
	IngestTS        int64  `json:"ingest_ts"`
}

type fundingEvent struct {
	Symbol          string `json:"symbol"`
	Exchange        string `json:"exchange"`
	FundingRate     string `json:"funding_rate"`
	FundingTime     int64  `json:"funding_time"`
	FundingInterval string `json:"funding_interval"`
	ExchangeTS      int64  `json:"exchange_ts"`
	IngestTS        int64  `json:"ingest_ts"`
}

type oiEvent struct {
	Symbol     string `json:"symbol"`
	Exchange   string `json:"exchange"`
	OI         string `json:"oi"`
	OIUSD      string `json:"oi_usd"`
	ExchangeTS int64  `json:"exchange_ts"`
	IngestTS   int64  `json:"ingest_ts"`
}

type liquidationEvent struct {
	Symbol     string `json:"symbol"`
	Exchange   string `json:"exchange"`
	Side       string `json:"side"`
	Qty        string `json:"qty"`
	Price      string `json:"price"`
	OrderID    int64  `json:"order_id"`
	ExchangeTS int64  `json:"exchange_ts"`
	IngestTS   int64  `json:"ingest_ts"`
}

type tradeNormalizer struct {
	symbol   string
	exchange string
}

type markIndexHandler struct {
	symbol   string
	exchange string
}

type fundingNormalizer struct {
	symbol          string
	exchange        string
	defaultInterval string
}

type oiNormalizer struct {
	symbol   string
	exchange string
}

type liquidationNormalizer struct {
	symbol   string
	exchange string
}

type aggTradeHandler struct {
	symbol   string
	exchange string
}

type aggTradeRESTItem struct {
	AggTradeID   int64  `json:"a"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	FirstTradeID int64  `json:"f"`
	LastTradeID  int64  `json:"l"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
	Symbol       string `json:"s"`
}

func init() {
	Register("trade_normalizer", newTradeNormalizer)
	Register("mark_index_parser", newMarkIndexHandler)
	Register("funding_normalizer", newFundingNormalizer)
	Register("oi_normalizer", newOINormalizer)
	Register("liquidation_normalizer", newLiquidationNormalizer)
	Register("binance_aggtrade", newAggTradeHandler)
}

func newTradeNormalizer(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	return &tradeNormalizer{
		symbol:   strings.ToUpper(util.GetString(cfg, "symbol", "")),
		exchange: util.GetString(cfg, "exchange", defaultExchange),
	}, nil
}

func newMarkIndexHandler(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	return &markIndexHandler{
		symbol:   strings.ToUpper(util.GetString(cfg, "symbol", "")),
		exchange: util.GetString(cfg, "exchange", defaultExchange),
	}, nil
}

func newFundingNormalizer(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	return &fundingNormalizer{
		symbol:          strings.ToUpper(util.GetString(cfg, "symbol", "")),
		exchange:        util.GetString(cfg, "exchange", defaultExchange),
		defaultInterval: util.GetString(cfg, "interval", "8h"),
	}, nil
}

func newOINormalizer(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	return &oiNormalizer{
		symbol:   strings.ToUpper(util.GetString(cfg, "symbol", "")),
		exchange: util.GetString(cfg, "exchange", defaultExchange),
	}, nil
}

func newLiquidationNormalizer(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	return &liquidationNormalizer{
		symbol:   strings.ToUpper(util.GetString(cfg, "symbol", "")),
		exchange: util.GetString(cfg, "exchange", defaultExchange),
	}, nil
}

func newAggTradeHandler(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	return &aggTradeHandler{
		symbol:   strings.ToUpper(util.GetString(cfg, "symbol", "")),
		exchange: util.GetString(cfg, "exchange", defaultExchange),
	}, nil
}

func (h *tradeNormalizer) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}
	// 依赖上游 binance_parser 已将数据解析并放入 metadata
	if msg.Metadata == nil {
		return nil, fmt.Errorf("trade_normalizer: metadata 缺失")
	}

	trade, ok := msg.Metadata["binance_trade"].(*binance.TradeMessage)
	if !ok || trade == nil {
		return nil, fmt.Errorf("trade_normalizer: binance_trade 未找到，请确保上游 binance_parser 已配置")
	}

	if h.symbol != "" && strings.ToUpper(trade.Symbol) != h.symbol {
		return nil, nil
	}
	side := "buy"
	if trade.IsBuyerMaker {
		side = "sell"
	}
	event := tradeEvent{
		Symbol:      strings.ToUpper(trade.Symbol),
		Exchange:    util.FirstNonEmpty(h.exchange, defaultExchange),
		Price:       trade.Price,
		Size:        trade.Quantity,
		Side:        side,
		BuyerMaker:  trade.IsBuyerMaker,
		ExchangeTS:  util.FirstNonZero(trade.TradeTime, trade.EventTime),
		IngestTS:    time.Now().UTC().UnixMilli(),
		TradeID:     trade.TradeID,
		BuyerOrder:  trade.BuyerOrder,
		SellerOrder: trade.SellerOrder,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      event.Symbol,
		"exchange":    event.Exchange,
		"trade_id":    event.TradeID,
		"exchange_ts": event.ExchangeTS,
	}
	return []*types.Message{{Metadata: meta, Payload: payload}}, nil
}

func (h *markIndexHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}
	// 依赖上游 binance_parser 已将数据解析并放入 metadata
	if msg.Metadata == nil {
		return nil, fmt.Errorf("mark_index_parser: metadata 缺失")
	}

	data, ok := msg.Metadata["binance_mark_index"].(*binance.MarkIndexMessage)
	if !ok || data == nil {
		return nil, fmt.Errorf("mark_index_parser: binance_mark_index 未找到，请确保上游 binance_parser 已配置")
	}

	if h.symbol != "" && strings.ToUpper(data.Symbol) != h.symbol {
		return nil, nil
	}
	fair := diffDecimal(data.MarkPrice, data.IndexPrice)
	event := markIndexEvent{
		Symbol:          strings.ToUpper(data.Symbol),
		Exchange:        util.FirstNonEmpty(h.exchange, defaultExchange),
		MarkPrice:       data.MarkPrice,
		IndexPrice:      data.IndexPrice,
		FairBasis:       fair,
		LastFundingRate: data.RateLimit,
		NextFundingTime: data.NextFundingTime,
		ExchangeTS:      util.FirstNonZero(data.EventTime, data.NextFundingTime),
		IngestTS:        time.Now().UTC().UnixMilli(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      event.Symbol,
		"exchange":    event.Exchange,
		"exchange_ts": event.ExchangeTS,
	}
	return []*types.Message{{Metadata: meta, Payload: payload}}, nil
}

func (h *fundingNormalizer) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}

	var records []struct {
		Symbol      string `json:"symbol"`
		FundingTime int64  `json:"fundingTime"`
		FundingRate string `json:"fundingRate"`
	}
	if err := json.Unmarshal(msg.Payload, &records); err != nil {
		var single struct {
			Symbol      string `json:"symbol"`
			FundingTime int64  `json:"fundingTime"`
			FundingRate string `json:"fundingRate"`
		}
		if err2 := json.Unmarshal(msg.Payload, &single); err2 != nil {
			return nil, fmt.Errorf("funding_normalizer: decode payload failed: %w", err)
		}
		if single.Symbol != "" {
			records = append(records, single)
		}
	}
	if len(records) == 0 {
		return nil, nil
	}
	record := records[len(records)-1]
	if h.symbol != "" && strings.ToUpper(record.Symbol) != h.symbol {
		return nil, nil
	}
	event := fundingEvent{
		Symbol:          strings.ToUpper(record.Symbol),
		Exchange:        util.FirstNonEmpty(h.exchange, defaultExchange),
		FundingRate:     record.FundingRate,
		FundingTime:     record.FundingTime,
		FundingInterval: h.defaultInterval,
		ExchangeTS:      record.FundingTime,
		IngestTS:        time.Now().UTC().UnixMilli(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      event.Symbol,
		"exchange":    event.Exchange,
		"exchange_ts": event.ExchangeTS,
	}
	return []*types.Message{{Metadata: meta, Payload: payload}}, nil
}

func (h *oiNormalizer) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}

	var hist []struct {
		Symbol             string `json:"symbol"`
		SumOpenInterest    string `json:"sumOpenInterest"`
		SumOpenInterestVal string `json:"sumOpenInterestValue"`
		Timestamp          int64  `json:"timestamp"`
	}
	if err := json.Unmarshal(msg.Payload, &hist); err == nil && len(hist) > 0 {
		last := hist[len(hist)-1]
		if h.symbol != "" && strings.ToUpper(last.Symbol) != h.symbol {
			return nil, nil
		}
		event := oiEvent{
			Symbol:     strings.ToUpper(last.Symbol),
			Exchange:   util.FirstNonEmpty(h.exchange, defaultExchange),
			OI:         last.SumOpenInterest,
			OIUSD:      util.FirstNonEmpty(last.SumOpenInterestVal, last.SumOpenInterest),
			ExchangeTS: last.Timestamp,
			IngestTS:   time.Now().UTC().UnixMilli(),
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		meta := map[string]any{
			"symbol":      event.Symbol,
			"exchange":    event.Exchange,
			"exchange_ts": event.ExchangeTS,
		}
		return []*types.Message{{Metadata: meta, Payload: payload}}, nil
	}

	var current struct {
		Symbol       string `json:"symbol"`
		OpenInterest string `json:"openInterest"`
		Time         int64  `json:"time"`
	}
	if err := json.Unmarshal(msg.Payload, &current); err != nil {
		return nil, fmt.Errorf("oi_normalizer: decode payload failed: %w", err)
	}
	if h.symbol != "" && strings.ToUpper(current.Symbol) != h.symbol {
		return nil, nil
	}
	event := oiEvent{
		Symbol:     strings.ToUpper(current.Symbol),
		Exchange:   util.FirstNonEmpty(h.exchange, defaultExchange),
		OI:         current.OpenInterest,
		OIUSD:      current.OpenInterest,
		ExchangeTS: current.Time,
		IngestTS:   time.Now().UTC().UnixMilli(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      event.Symbol,
		"exchange":    event.Exchange,
		"exchange_ts": event.ExchangeTS,
	}
	return []*types.Message{{Metadata: meta, Payload: payload}}, nil
}

func (h *liquidationNormalizer) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}
	// 依赖上游 binance_parser 已将数据解析并放入 metadata
	if msg.Metadata == nil {
		return nil, fmt.Errorf("liquidation_normalizer: metadata 缺失")
	}

	data, ok := msg.Metadata["binance_liquidation"].(*binance.LiquidationMessage)
	if !ok || data == nil {
		return nil, fmt.Errorf("liquidation_normalizer: binance_liquidation 未找到，请确保上游 binance_parser 已配置")
	}

	if h.symbol != "" && strings.ToUpper(data.Symbol) != h.symbol {
		return nil, nil
	}
	order := data.Order
	event := liquidationEvent{
		Symbol:     strings.ToUpper(data.Symbol),
		Exchange:   util.FirstNonEmpty(h.exchange, defaultExchange),
		Side:       strings.ToLower(order.Side),
		Qty:        order.OriginalQty,
		Price:      order.Price,
		OrderID:    order.OrderID,
		ExchangeTS: util.FirstNonZero(data.EventTime, data.EventTime),
		IngestTS:   time.Now().UTC().UnixMilli(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{
		"symbol":      event.Symbol,
		"exchange":    event.Exchange,
		"exchange_ts": event.ExchangeTS,
		"order_id":    event.OrderID,
	}
	return []*types.Message{{Metadata: meta, Payload: payload}}, nil
}

func (h *aggTradeHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}

	aggTrades, err := parseAggTradePayload(msg.Payload, h.symbol)
	if err != nil {
		return nil, fmt.Errorf("binance_aggtrade: 解析消息失败: %w", err)
	}
	out := make([]*types.Message, 0, len(aggTrades))
	for _, aggTrade := range aggTrades {
		if aggTrade.EventType != "aggTrade" {
			return nil, fmt.Errorf("binance_aggtrade: 非法事件类型: %s", aggTrade.EventType)
		}

		symbol := strings.ToUpper(util.FirstNonEmpty(aggTrade.Symbol, h.symbol))
		if h.symbol != "" && symbol != h.symbol {
			continue
		}

		// 计算交易方向: buyer_maker=true表示卖单(taker是买方，maker是卖方)
		side := "buy"
		if aggTrade.IsBuyerMaker {
			side = "sell"
		}

		// 构造标准化的交易事件
		event := tradeEvent{
			Symbol:      symbol,
			Exchange:    util.FirstNonEmpty(h.exchange, defaultExchange),
			Price:       aggTrade.Price,
			Size:        aggTrade.Quantity,
			Side:        side,
			BuyerMaker:  aggTrade.IsBuyerMaker,
			ExchangeTS:  util.FirstNonZero(aggTrade.TradeTime, aggTrade.EventTime),
			IngestTS:    time.Now().UTC().UnixMilli(),
			TradeID:     aggTrade.AggTradeID, // 使用聚合交易ID
			BuyerOrder:  0,                   // aggTrade不包含订单ID信息
			SellerOrder: 0,
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}

		meta := map[string]any{
			"symbol":         event.Symbol,
			"exchange":       event.Exchange,
			"agg_trade_id":   aggTrade.AggTradeID,
			"first_trade_id": aggTrade.FirstTradeID,
			"last_trade_id":  aggTrade.LastTradeID,
			"exchange_ts":    event.ExchangeTS,
		}
		inheritBackfillMeta(meta, msg.Metadata)
		out = append(out, &types.Message{Metadata: meta, Payload: payload})
	}
	return out, nil
}

func parseAggTradePayload(payload []byte, fallbackSymbol string) ([]binance.AggTradeMessage, error) {
	// Combined stream payload: {"stream":"btcusdt@aggTrade","data":{...}}
	var wsEnvelope struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &wsEnvelope); err == nil && len(wsEnvelope.Data) > 0 {
		symbol := strings.ToUpper(strings.TrimSpace(fallbackSymbol))
		if stream := strings.TrimSpace(wsEnvelope.Stream); stream != "" {
			if idx := strings.Index(stream, "@"); idx > 0 {
				stream = stream[:idx]
			}
			stream = strings.ToUpper(strings.TrimSpace(stream))
			if stream != "" {
				symbol = stream
			}
		}
		return parseAggTradePayload(wsEnvelope.Data, symbol)
	}

	var wsTrade binance.AggTradeMessage
	if err := json.Unmarshal(payload, &wsTrade); err == nil && wsTrade.EventType != "" {
		if wsTrade.Symbol == "" && fallbackSymbol != "" {
			wsTrade.Symbol = strings.ToUpper(strings.TrimSpace(fallbackSymbol))
		}
		return []binance.AggTradeMessage{wsTrade}, nil
	}

	var wsTrades []binance.AggTradeMessage
	if err := json.Unmarshal(payload, &wsTrades); err == nil && len(wsTrades) > 0 {
		hasWSEnvelope := false
		for _, item := range wsTrades {
			if item.EventType != "" || item.Symbol != "" {
				hasWSEnvelope = true
				break
			}
		}
		if hasWSEnvelope {
			return wsTrades, nil
		}
	}

	var restTrades []aggTradeRESTItem
	if err := json.Unmarshal(payload, &restTrades); err == nil && len(restTrades) > 0 {
		out := make([]binance.AggTradeMessage, 0, len(restTrades))
		for _, item := range restTrades {
			out = append(out, convertRestAggTrade(item, fallbackSymbol))
		}
		return out, nil
	}

	var restTrade aggTradeRESTItem
	if err := json.Unmarshal(payload, &restTrade); err == nil && restTrade.AggTradeID > 0 {
		return []binance.AggTradeMessage{convertRestAggTrade(restTrade, fallbackSymbol)}, nil
	}
	return nil, fmt.Errorf("不支持的 payload 结构")
}

func convertRestAggTrade(item aggTradeRESTItem, fallbackSymbol string) binance.AggTradeMessage {
	return binance.AggTradeMessage{
		EventType:    "aggTrade",
		EventTime:    item.TradeTime,
		Symbol:       strings.ToUpper(util.FirstNonEmpty(item.Symbol, fallbackSymbol)),
		AggTradeID:   item.AggTradeID,
		Price:        item.Price,
		Quantity:     item.Quantity,
		FirstTradeID: item.FirstTradeID,
		LastTradeID:  item.LastTradeID,
		TradeTime:    item.TradeTime,
		IsBuyerMaker: item.IsBuyerMaker,
	}
}

func inheritBackfillMeta(dst map[string]any, src map[string]any) {
	if dst == nil || src == nil {
		return
	}
	keys := []string{
		"is_backfill",
		"backfill_type",
		"backfill_attempt",
		"backfill_step",
		"backfill_steps",
	}
	for _, key := range keys {
		if v, ok := src[key]; ok {
			dst[key] = v
		}
	}
}

func diffDecimal(a, b string) string {
	if a == "" || b == "" {
		return ""
	}
	r1, ok1 := new(big.Rat).SetString(a)
	r2, ok2 := new(big.Rat).SetString(b)
	if !ok1 || !ok2 {
		return ""
	}
	return new(big.Rat).Sub(r1, r2).FloatString(10)
}

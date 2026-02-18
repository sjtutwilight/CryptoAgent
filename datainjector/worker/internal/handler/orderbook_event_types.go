package handler

// depthPayload 是订单簿深度层级的通用结构。
type depthPayload struct {
	Bids [][]string `json:"bids"`
	Asks [][]string `json:"asks"`
}

// orderbookEvent 是历史兼容的订单簿事件结构（仍用于部分非 Binance 链路）。
type orderbookEvent struct {
	Symbol     string       `json:"symbol"`
	Exchange   string       `json:"exchange"`
	Snapshot   bool         `json:"snapshot"`
	Depth      depthPayload `json:"depth"`
	Seq        int64        `json:"seq"`
	ExchangeTS int64        `json:"exchange_ts"`
	IngestTS   int64        `json:"ingest_ts"`
}

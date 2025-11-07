package model

// BinanceDepthSnapshot 模拟Binance深度快照
type BinanceDepthSnapshot struct {
	LastUpdateID    int64      `json:"lastUpdateId"`
	EventTime       int64      `json:"E"`
	TransactionTime int64      `json:"T"`
	Bids            [][]string `json:"bids"`
	Asks            [][]string `json:"asks"`
}

// BinanceDepthDiff 模拟Binance深度增量
type BinanceDepthDiff struct {
	EventType         string     `json:"e"`
	EventTime         int64      `json:"E"`
	TransactionTime   int64      `json:"T"`
	Symbol            string     `json:"s"`
	FirstUpdateID     int64      `json:"U"`
	FinalUpdateID     int64      `json:"u"`
	PrevFinalUpdateID int64      `json:"pu"`
	Bids              [][]string `json:"b"`
	Asks              [][]string `json:"a"`
}

// BinanceAggTrade 模拟Binance聚合交易
type BinanceAggTrade struct {
	EventType    string `json:"e"` // Event type: "aggTrade"
	EventTime    int64  `json:"E"` // Event time
	Symbol       string `json:"s"` // Symbol
	AggTradeID   int64  `json:"a"` // Aggregate trade ID
	Price        string `json:"p"` // Price
	Quantity     string `json:"q"` // Quantity
	FirstTradeID int64  `json:"f"` // First trade ID
	LastTradeID  int64  `json:"l"` // Last trade ID
	TradeTime    int64  `json:"T"` // Trade time
	IsBuyerMaker bool   `json:"m"` // Is the buyer the market maker?
}

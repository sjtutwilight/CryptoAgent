package binance

// DepthMessage represents a websocket depth update payload.
type DepthMessage struct {
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

// CombinedDepthMessage wraps depth updates when using the combined stream endpoint.
type CombinedDepthMessage struct {
	Stream string       `json:"stream"`
	Data   DepthMessage `json:"data"`
}

// DepthSnapshotResponse describes the REST snapshot body.
type DepthSnapshotResponse struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
	EventTime    int64      `json:"E"`
	Transaction  int64      `json:"T"`
	Symbol       string     `json:"s"`
	Code         int        `json:"code"`
	Msg          string     `json:"msg"`
}

// TradeMessage represents a trade websocket payload.
type TradeMessage struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	TradeID      int64  `json:"t"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	BuyerOrder   int64  `json:"b"`
	SellerOrder  int64  `json:"a"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

// CombinedTradeMessage wraps a trade stream event.
type CombinedTradeMessage struct {
	Stream string       `json:"stream"`
	Data   TradeMessage `json:"data"`
}

// MarkIndexMessage contains mark/index price push payload.
type MarkIndexMessage struct {
	EventType       string `json:"e"`
	EventTime       int64  `json:"E"`
	Symbol          string `json:"s"`
	MarkPrice       string `json:"p"`
	IndexPrice      string `json:"i"`
	EstimatedPrice  string `json:"P"`
	RateLimit       string `json:"r"` // funding rate
	NextFundingTime int64  `json:"T"`
}

type CombinedMarkIndexMessage struct {
	Stream string           `json:"stream"`
	Data   MarkIndexMessage `json:"data"`
}

// LiquidationMessage represents liquidation websocket payload.
type LiquidationMessage struct {
	EventType string           `json:"e"`
	EventTime int64            `json:"E"`
	Symbol    string           `json:"s"`
	Order     LiquidationOrder `json:"o"`
}

type LiquidationOrder struct {
	Symbol          string `json:"s"`
	Side            string `json:"S"`
	OrderType       string `json:"o"`
	TimeInForce     string `json:"f"`
	OriginalQty     string `json:"q"`
	Price           string `json:"p"`
	AveragePrice    string `json:"ap"`
	OrderStatus     string `json:"X"`
	LastFilledQty   string `json:"l"`
	AccumulatedQty  string `json:"z"`
	OrderID         int64  `json:"i"`
	ClientOrderID   string `json:"c"`
	LastFilledPrice string `json:"L"`
}

type CombinedLiquidationMessage struct {
	Stream string             `json:"stream"`
	Data   LiquidationMessage `json:"data"`
}

// AggTradeMessage represents aggregate trade websocket payload.
type AggTradeMessage struct {
	EventType    string `json:"e"`      // Event type: "aggTrade"
	EventTime    int64  `json:"E"`      // Event time
	Symbol       string `json:"s"`      // Symbol
	AggTradeID   int64  `json:"a"`      // Aggregate trade ID
	Price        string `json:"p"`      // Price
	Quantity     string `json:"q"`      // Quantity
	FirstTradeID int64  `json:"f"`      // First trade ID
	LastTradeID  int64  `json:"l"`      // Last trade ID
	TradeTime    int64  `json:"T"`      // Trade time
	IsBuyerMaker bool   `json:"m"`      // Is the buyer the market maker?
}

type CombinedAggTradeMessage struct {
	Stream string          `json:"stream"`
	Data   AggTradeMessage `json:"data"`
}

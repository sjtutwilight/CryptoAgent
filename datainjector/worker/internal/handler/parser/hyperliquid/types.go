package hyperliquid

// OrderbookSnapshot represents a snapshot style L2 book update from Hyperliquid.
type OrderbookSnapshot struct {
	Coin      string
	Time      int64
	Sequence  int64
	Bids      [][]string
	Asks      [][]string
	RawLevels interface{}
}

// TradeMessage represents a trade fill pushed via Hyperliquid websocket.
type TradeMessage struct {
	Coin    string
	Side    string
	Px      string
	Sz      string
	Time    int64
	TID     string
	Seq     int64
	RawData map[string]any
}

// AssetContext holds mark price, funding rate and open interest metrics.
type AssetContext struct {
	Coin         string
	MarkPx       string
	OraclePx     string
	OpenInterest string
	Funding      string
	DayVolume    string
	PrevDayPx    string
	MidPx        string
	Timestamp    int64
	RawCtx       map[string]any
}

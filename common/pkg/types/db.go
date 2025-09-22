package types

import "time"

// Transaction represents a blockchain transaction
type Transaction struct {
	ID              int64     `db:"id"`
	ChainID         string    `db:"chain_id"`
	TransactionHash string    `db:"transaction_hash"`
	BlockNumber     int64     `db:"block_number"`
	BlockTimestamp  time.Time `db:"block_timestamp"`
	FromAddress     string    `db:"from_address"`
	ToAddress       string    `db:"to_address"`
	MethodName      string    `db:"method_name"`
	Status          int       `db:"transaction_status"`
	GasUsed         int64     `db:"gas_used"`
	InputData       string    `db:"input_data"`
	CreateTime      time.Time `db:"create_time"`
}

// Event represents a blockchain event
type Event struct {
	ID              int64                  `db:"id"`
	TransactionID   int64                  `db:"transaction_id"`
	ChainID         string                 `db:"chain_id"`
	EventName       string                 `db:"event_name"`
	ContractAddress string                 `db:"contract_address"`
	LogIndex        int                    `db:"log_index"`
	EventData       string                 `db:"event_data"`
	CreateTime      time.Time              `db:"create_time"`
	BlockNumber     int64                  `db:"block_number"`
	DecodedArgs     map[string]interface{} `db:"-"`
}

// Pair represents a TWSwap pair
type Pair struct {
	ID                   int64     `db:"id"`
	ChainID              string    `db:"chain_id"`
	PairAddress          string    `db:"pair_address"`
	Token0ID             int64     `db:"token0_id"`
	Token1ID             int64     `db:"token1_id"`
	FeeTier              string    `db:"fee_tier"`
	CreatedAtTimestamp   time.Time `db:"created_at_timestamp"`
	CreatedAtBlockNumber int64     `db:"created_at_block_number"`
}

// Token represents a token
type Token struct {
	ID           int64     `db:"id"`
	ChainID      string    `db:"chain_id"`
	TokenAddress string    `db:"token_address"`
	TokenSymbol  string    `db:"token_symbol"`
	TokenName    string    `db:"token_name"`
	Decimals     int       `db:"decimals"`
	TrustScore   float64   `db:"trust_score"`
	CreateTime   time.Time `db:"create_time"`
	UpdateTime   time.Time `db:"update_time"`
}

// TokenMetric represents token metrics
type TokenMetric struct {
	ID             int64     `db:"id"`
	TokenID        int64     `db:"token_id"`
	TimeWindow     string    `db:"time_window"`
	EndTime        time.Time `db:"end_time"`
	VolumeUSD      float64   `db:"volume_usd"`
	TxCount        int       `db:"txcnt"`
	TokenPriceUSD  float64   `db:"token_price_usd"`
	BuyPressureUSD float64   `db:"buy_pressure_usd"`
	BuyersCount    int       `db:"buyers_count"`
	SellersCount   int       `db:"sellers_count"`
	BuyVolumeUSD   float64   `db:"buy_volume_usd"`
	SellVolumeUSD  float64   `db:"sell_volume_usd"`
	MakersCount    int       `db:"makers_count"`
	BuyCount       int       `db:"buy_count"`
	SellCount      int       `db:"sell_count"`
	UpdateTime     time.Time `db:"update_time"`
}

// TWSwapFactory represents a TWSwap factory contract
type TWSwapFactory struct {
	ID             int64     `db:"id"`
	ChainID        string    `db:"chain_id"`
	FactoryAddress string    `db:"factory_address"`
	TimeWindow     string    `db:"time_window"`
	EndTime        time.Time `db:"end_time"`
	PairCount      int       `db:"pair_count"`
	VolumeUSD      float64   `db:"volume_usd"`
	LiquidityUSD   float64   `db:"liquidity_usd"`
	TxCount        int       `db:"txcnt"`
	UpdateTime     time.Time `db:"update_time"`
}

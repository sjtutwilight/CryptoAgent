package models

import (
	"time"
)

// Token 表示代币信息
type TokenDB struct {
	ID            int        `db:"id"`
	ChainID       int        `db:"chain_id"`
	ChainName     string     `db:"chain_name"`
	TokenSymbol   string     `db:"token_symbol"`
	TokenCatagory string     `db:"token_catagory"`
	TokenDecimals int        `db:"token_decimals"`
	TokenAddress  string     `db:"token_address"`
	Issuer        string     `db:"issuer"`
	CreateTime    *time.Time `db:"create_time"`
	UpdateTime    *time.Time `db:"update_time"`
}

// TokenMetric 表示代币指标
type TokenMetricDB struct {
	TokenID       int        `db:"token_id"`
	TokenPrice    float64    `db:"token_price"`
	TokenAge      int        `db:"token_age"`
	LiquidityUSD  float64    `db:"liquidity_usd"`
	SecurityScore int        `db:"security_score"`
	FDV           float64    `db:"fdv"`
	MCAP          float64    `db:"mcap"`
	CreateTime    *time.Time `db:"create_time"`
	UpdateTime    *time.Time `db:"update_time"`
}

// TokenRecentMetric 表示代币最近指标
type TokenRecentMetricDB struct {
	TokenID        int        `db:"token_id"`
	TimeWindow     string     `db:"time_window"`
	EndTime        *time.Time `db:"end_time"`
	Tag            string     `db:"tag"`
	TxCnt          int        `db:"txcnt"`
	BuyCount       int        `db:"buy_count"`
	SellCount      int        `db:"sell_count"`
	VolumeUSD      float64    `db:"volume_usd"`
	BuyVolumeUSD   float64    `db:"buy_volume_usd"`
	SellVolumeUSD  float64    `db:"sell_volume_usd"`
	BuyPressureUSD float64    `db:"buy_pressure_usd"`
	TokenPriceUSD  float64    `db:"token_price_usd"`
}

// TokenRollingMetric 表示代币滚动指标
type TokenRollingMetricDB struct {
	TokenID       int        `db:"token_id"`
	TimeWindow    string     `db:"time_window"`
	EndTime       *time.Time `db:"end_time"`
	TokenPriceUSD float64    `db:"token_price_usd"`
	MCAP          float64    `db:"mcap"`
}

// TokenHolder 表示代币持有者
type TokenHolderDB struct {
	TokenID        int        `db:"token_id"`
	AccountID      int        `db:"account_id"`
	TokenAddress   string     `db:"token_address"`
	AccountAddress string     `db:"account_address"`
	Entity         string     `db:"entity"`
	ValueUSD       float64    `db:"value_usd"`
	Ownership      float64    `db:"ownership"`
	CreateTime     *time.Time `db:"create_time"`
	UpdateTime     *time.Time `db:"update_time"`
}

// TWSwapPair 表示交易对
type TWSwapPairDB struct {
	ID                   int64     `db:"id"`
	ChainID              string    `db:"chain_id"`
	PairAddress          string    `db:"pair_address"`
	PairName             string    `db:"pair_name"`
	Token0ID             int64     `db:"token0_id"`
	Token1ID             int64     `db:"token1_id"`
	FeeTier              string    `db:"fee_tier"`
	CreatedAtTimestamp   time.Time `db:"created_at_timestamp"`
	CreatedAtBlockNumber int64     `db:"created_at_block_number"`
}

// TWSwapPairMetric 表示交易对指标
type TWSwapPairMetricDB struct {
	ID              int64     `db:"id"`
	PairID          int64     `db:"pair_id"`
	TimeWindow      string    `db:"time_window"`
	EndTime         time.Time `db:"end_time"`
	Token0Reserve   float64   `db:"token0_reserve"`
	Token1Reserve   float64   `db:"token1_reserve"`
	ReserveUSD      float64   `db:"reserve_usd"`
	Token0VolumeUSD float64   `db:"token0_volume_usd"`
	Token1VolumeUSD float64   `db:"token1_volume_usd"`
	VolumeUSD       float64   `db:"volume_usd"`
	TxCnt           int       `db:"txcnt"`
}

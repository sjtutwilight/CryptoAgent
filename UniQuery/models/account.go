package models

import (
	"database/sql"
	"time"
)

// Account 表示账户信息
type AccountDB struct {
	ID             int        `db:"id"`
	ChainID        int        `db:"chain_id"`
	ChainName      string     `db:"chain_name"`
	Address        string     `db:"address"`
	Entity         string     `db:"entity"`
	SmartMoneyTag  bool       `db:"smart_money_tag"`
	CexTag         bool       `db:"cex_tag"`
	BigWhaleTag    bool       `db:"big_whale_tag"`
	FreshWalletTag bool       `db:"fresh_wallet_tag"`
	CreateTime     *time.Time `db:"create_time"`
	UpdateTime     *time.Time `db:"update_time"`
}

// AccountAsset 表示账户资产信息
type AccountAssetDB struct {
	AccountID  int        `db:"account_id"`
	AssetType  string     `db:"asset_type"`
	BizID      int        `db:"biz_id"`
	BizName    string     `db:"biz_name"`
	Value      float64    `db:"value"`
	ExtendInfo string     `db:"extend_info"`
	CreateTime *time.Time `db:"create_time"`
	UpdateTime *time.Time `db:"update_time"`
}

// AccountAssetView 表示账户资产视图
type AccountAssetViewDB struct {
	AccountID  int        `db:"account_id"`
	AssetType  string     `db:"asset_type"`
	BizID      int        `db:"biz_id"`
	BizName    string     `db:"biz_name"`
	Value      float64    `db:"value"`
	ValueUSD   float64    `db:"value_usd"`
	AssetPrice float64    `db:"asset_price"`
	CreateTime *time.Time `db:"create_time"`
	UpdateTime *time.Time `db:"update_time"`
}

// AccountTransferHistory 表示账户转账历史
type AccountTransferHistoryDB struct {
	AccountID     int        `db:"account_id"`
	IsBuy         int        `db:"isBuy"`
	EndTime       *time.Time `db:"end_time"`
	TxCnt         int        `db:"txcnt"`
	TotalValueUSD float64    `db:"total_value_usd"`
}

// TransferEvent 表示转账事件
type TransferEventDB struct {
	ID              int            `db:"id"`
	TokenID         int            `db:"token_id"`
	EventID         int            `db:"event_id"`
	FromAddress     string         `db:"from_address"`
	ToAddress       string         `db:"to_address"`
	BlockTimestamp  sql.NullTime   `db:"block_timestamp"`
	Timestamp       sql.NullTime   `db:"timestamp"`
	TokenSymbol     string         `db:"token_symbol"`
	Value           float64        `db:"value"`
	ValueUSD        float64        `db:"value_usd"`
	TransactionHash sql.NullString `db:"transaction_hash"`
}

// Transaction 表示交易信息
type TransactionDB struct {
	ID                int64        `db:"id"`
	ChainID           string       `db:"chain_id"`
	TransactionHash   string       `db:"transaction_hash"`
	BlockNumber       int64        `db:"block_number"`
	BlockTimestamp    sql.NullTime `db:"block_timestamp"`
	Timestamp         sql.NullTime `db:"timestamp"`
	FromAddress       string       `db:"from_address"`
	ToAddress         string       `db:"to_address"`
	MethodName        string       `db:"method_name"`
	TransactionStatus int          `db:"transaction_status"`
	GasUsed           int64        `db:"gas_used"`
	InputData         string       `db:"input_data"`
	CreateTime        *time.Time   `db:"create_time"`
}

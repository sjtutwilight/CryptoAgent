package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	redis "github.com/go-redis/redis/v8"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

// BalanceParser 解析账户余额数据并进行标准化
// 支持从 Redis 查询资产价格，计算 USD 价值
type BalanceParser struct {
	redisClient  *redis.Client
	priceTimeout time.Duration
}

type balanceRaw struct {
	ChainID     string       `json:"chain_id"`
	BlockNumber uint64       `json:"block_number"`
	BlockHash   string       `json:"block_hash"`
	ObservedAt  string       `json:"observed_at"`
	Account     balanceAcct  `json:"account"`
	Asset       balanceAsset `json:"asset"`
	BalanceWei  string       `json:"balance_wei"`
}

type balanceAcct struct {
	ID        int64  `json:"id"`
	Address   string `json:"address"`
	LabelMask int    `json:"label_mask"`
	Tag       string `json:"tag"`
}

type balanceAsset struct {
	Kind         string `json:"kind"`
	BizID        int64  `json:"biz_id"`
	Address      string `json:"address"`
	Symbol       string `json:"symbol"`
	Decimals     int    `json:"decimals"`
	DefaultPrice string `json:"default_price"`
	BizName      string `json:"biz_name"`
}

type balanceOutput struct {
	AccountID       int64  `json:"account_id"`
	ObservedTime    string `json:"observed_time"`
	BlockID         uint64 `json:"block_id"`
	AssetType       string `json:"asset_type"`
	BizID           int64  `json:"biz_id"`
	Amount          string `json:"amount"`
	PriceUSD        string `json:"price_usd"`
	ValueUSD        string `json:"value_usd"`
	LabelMask       int    `json:"label_mask"`
	AccountAddress  string `json:"account_address"`
	ContractAddress string `json:"contract_address"`
	BizName         string `json:"biz_name"`
}

// NewBalanceParser 创建余额解析器
func NewBalanceParser(cfg map[string]any) (*BalanceParser, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	addr := util.GetString(cfg, "redis_addr", "localhost:6379")
	db := util.GetInt(cfg, "redis_db", 0)
	password := util.GetString(cfg, "redis_password", "")
	timeout := time.Duration(util.GetInt(cfg, "redis_timeout_ms", 2000)) * time.Millisecond

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &BalanceParser{
		redisClient:  client,
		priceTimeout: timeout,
	}, nil
}

// Handle 处理余额消息，解析并标准化
func (p *BalanceParser) Handle(msg *types.Message) ([]*types.Message, error) {
	var raw balanceRaw
	if err := json.Unmarshal(msg.Payload, &raw); err != nil {
		return nil, fmt.Errorf("balance_parser: invalid payload: %w", err)
	}

	balanceWei, ok := new(big.Int).SetString(raw.BalanceWei, 10)
	if !ok {
		return nil, nil
	}
	if balanceWei.Sign() == 0 {
		return nil, nil
	}

	amountStr := formatDecimal(balanceWei, raw.Asset.Decimals)
	priceStr := p.resolvePrice(raw.Asset)
	valueStr := multiplyDecimal(amountStr, priceStr)

	observedTime := raw.ObservedAt
	if observedTime == "" {
		observedTime = time.Now().UTC().Format(time.RFC3339)
	}

	out := balanceOutput{
		AccountID:       raw.Account.ID,
		ObservedTime:    observedTime,
		BlockID:         raw.BlockNumber,
		AssetType:       raw.Asset.Kind,
		BizID:           raw.Asset.BizID,
		Amount:          amountStr,
		PriceUSD:        priceStr,
		ValueUSD:        valueStr,
		LabelMask:       raw.Account.LabelMask,
		AccountAddress:  raw.Account.Address,
		ContractAddress: raw.Asset.Address,
		BizName:         raw.Asset.BizName,
	}

	payload, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}

	msg.Metadata = map[string]any{
		"chain_id":     raw.ChainID,
		"account_id":   raw.Account.ID,
		"asset_type":   raw.Asset.Kind,
		"biz_id":       raw.Asset.BizID,
		"block_number": raw.BlockNumber,
	}
	msg.Payload = payload
	log.Printf("[BalanceParser] Handle msg=%v", msg)
	return []*types.Message{msg}, nil
}

// resolvePrice 从 Redis 查询资产价格，如果查询失败则使用默认价格
func (p *BalanceParser) resolvePrice(asset balanceAsset) string {
	if p.redisClient == nil {
		return util.FirstNonEmpty(asset.DefaultPrice, "1")
	}
	key := priceRedisKey(asset)
	ctx, cancel := context.WithTimeout(context.Background(), p.priceTimeout)
	defer cancel()
	price, err := p.redisClient.Get(ctx, key).Result()
	if err == nil && price != "" {
		return price
	}
	return util.FirstNonEmpty(asset.DefaultPrice, "1")
}

// Close 关闭 Redis 连接
func (p *BalanceParser) Close() error {
	if p.redisClient != nil {
		return p.redisClient.Close()
	}
	return nil
}

// formatDecimal 将 Wei 单位的大整数转换为带小数的字符串
func formatDecimal(value *big.Int, decimals int) string {
	if decimals <= 0 {
		return value.String()
	}
	denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	rat := new(big.Rat).SetFrac(value, denom)
	return rat.FloatString(decimals)
}

// multiplyDecimal 计算两个十进制字符串的乘积（用于计算 USD 价值）
func multiplyDecimal(a, b string) string {
	ra, okA := new(big.Rat).SetString(a)
	rb, okB := new(big.Rat).SetString(b)
	if !okA || !okB {
		return "0"
	}
	return new(big.Rat).Mul(ra, rb).FloatString(18)
}

// priceRedisKey 生成 Redis 价格查询键
func priceRedisKey(asset balanceAsset) string {
	addr := strings.ToLower(asset.Address)
	if asset.Kind == "lp" {
		return fmt.Sprintf("lp_price:%s", addr)
	}
	return fmt.Sprintf("token_price:%s", addr)
}







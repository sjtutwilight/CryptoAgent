package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	redis "github.com/go-redis/redis/v8"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

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

func init() {
	Register("balance_parser", func(cfg map[string]any) (Handler, error) {
		return newBalanceParser(cfg)
	})
}

func newBalanceParser(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	addr := getString(cfg, "redis_addr", "localhost:6379")
	db := getInt(cfg, "redis_db", 0)
	password := getString(cfg, "redis_password", "")
	timeout := time.Duration(getInt(cfg, "redis_timeout_ms", 2000)) * time.Millisecond

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
	return []*types.Message{msg}, nil
}

func (p *BalanceParser) resolvePrice(asset balanceAsset) string {
	if p.redisClient == nil {
		return firstNonEmpty(asset.DefaultPrice, "1")
	}
	key := priceRedisKey(asset)
	ctx, cancel := context.WithTimeout(context.Background(), p.priceTimeout)
	defer cancel()
	price, err := p.redisClient.Get(ctx, key).Result()
	if err == nil && price != "" {
		return price
	}
	return firstNonEmpty(asset.DefaultPrice, "1")
}

func formatDecimal(value *big.Int, decimals int) string {
	if decimals <= 0 {
		return value.String()
	}
	denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	rat := new(big.Rat).SetFrac(value, denom)
	return rat.FloatString(decimals)
}

func multiplyDecimal(a, b string) string {
	ra, okA := new(big.Rat).SetString(a)
	rb, okB := new(big.Rat).SetString(b)
	if !okA || !okB {
		return "0"
	}
	return new(big.Rat).Mul(ra, rb).FloatString(18)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func (p *BalanceParser) Close() error {
	if p.redisClient != nil {
		return p.redisClient.Close()
	}
	return nil
}

func priceRedisKey(asset balanceAsset) string {
	addr := strings.ToLower(asset.Address)
	if asset.Kind == "lp" {
		return fmt.Sprintf("lp_price:%s", addr)
	}
	return fmt.Sprintf("token_price:%s", addr)
}

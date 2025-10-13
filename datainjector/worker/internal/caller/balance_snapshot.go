package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

var balanceOfSig = []byte{0x70, 0xa0, 0x82, 0x31}

type balanceSnapshotCaller struct {
	ethClient *ethclient.Client
	rpcClient *rpc.Client
	chainID   string

	accounts []accountConfig
	tokens   []assetToken
	pairs    []assetPair

	mu        sync.Mutex
	lastBlock uint64
}

type accountConfig struct {
	ID        int64
	Address   common.Address
	LabelMask int
	Tag       string
}

type assetToken struct {
	ID           int64
	Address      common.Address
	Symbol       string
	Decimals     int
	DefaultPrice string
	BizName      string
}

type assetPair struct {
	ID           int64
	Address      common.Address
	Symbol       string
	DefaultPrice string
}

type balanceJob struct {
	account accountConfig
	asset   assetInfo
	result  *hexutil.Bytes
}

type assetInfo struct {
	Kind         string
	BizID        int64
	Address      common.Address
	Symbol       string
	Decimals     int
	DefaultPrice string
	BizName      string
}

type rawBalanceSnapshot struct {
	ChainID     string     `json:"chain_id"`
	BlockNumber uint64     `json:"block_number"`
	BlockHash   string     `json:"block_hash"`
	ObservedAt  string     `json:"observed_at"`
	Account     rawAccount `json:"account"`
	Asset       rawAsset   `json:"asset"`
	BalanceWei  string     `json:"balance_wei"`
}

type rawAccount struct {
	ID        int64  `json:"id"`
	Address   string `json:"address"`
	LabelMask int    `json:"label_mask"`
	Tag       string `json:"tag"`
}

type rawAsset struct {
	Kind         string `json:"kind"`
	BizID        int64  `json:"biz_id"`
	Address      string `json:"address"`
	Symbol       string `json:"symbol"`
	Decimals     int    `json:"decimals"`
	DefaultPrice string `json:"default_price"`
	BizName      string `json:"biz_name"`
}

func newBalanceSnapshotCaller(params map[string]any) (Caller, error) {
	endpoint := getString(params, "rpc_endpoint", "")
	if endpoint == "" {
		return nil, fmt.Errorf("balance_snapshot: rpc_endpoint required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ethCli, err := ethclient.DialContext(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("balance_snapshot: dial rpc: %w", err)
	}
	rpcCli, err := rpc.DialContext(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("balance_snapshot: dial rpc client: %w", err)
	}

	chainID := getString(params, "chain_id", "")
	if chainID == "" {
		onChainID, err := ethCli.ChainID(ctx)
		if err != nil {
			return nil, fmt.Errorf("balance_snapshot: fetch chain id: %w", err)
		}
		chainID = onChainID.String()
	}

	deploymentPath := getString(params, "deployment_path", "../deployment.json")
	accounts, tokens, pairs, err := loadDeploymentAssets(deploymentPath)
	if err != nil {
		return nil, err
	}

	return &balanceSnapshotCaller{
		ethClient: ethCli,
		rpcClient: rpcCli,
		chainID:   chainID,
		accounts:  accounts,
		tokens:    tokens,
		pairs:     pairs,
	}, nil
}

func (c *balanceSnapshotCaller) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {

	c.mu.Lock()
	last := c.lastBlock
	c.mu.Unlock()

	latest, err := c.ethClient.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("balance_snapshot: blockNumber: %w", err)
	}
	if latest == 0 || latest == last {
		return nil, nil
	}

	block, err := c.ethClient.BlockByNumber(ctx, big.NewInt(int64(latest)))
	if err != nil {
		return nil, fmt.Errorf("balance_snapshot: block %d: %w", latest, err)
	}

	observedAt := time.Unix(int64(block.Time()), 0).UTC().Format(time.RFC3339)

	jobs, batch := c.buildBatch(latest)
	if len(batch) == 0 {
		return nil, nil
	}

	if err := c.rpcClient.BatchCallContext(ctx, batch); err != nil {
		return nil, fmt.Errorf("balance_snapshot: batch call: %w", err)
	}
	messages := make([]*types.Message, 0, len(jobs))
	for idx, job := range jobs {
		if batch[idx].Error != nil {

			continue
		}
		if job.result == nil {
			continue
		}
		balance := new(big.Int).SetBytes(*job.result)
		if balance == nil || balance.Sign() == 0 {
			continue
		}

		raw := rawBalanceSnapshot{
			ChainID:     c.chainID,
			BlockNumber: latest,
			BlockHash:   block.Hash().Hex(),
			ObservedAt:  observedAt,
			Account: rawAccount{
				ID:        job.account.ID,
				Address:   job.account.Address.Hex(),
				LabelMask: job.account.LabelMask,
				Tag:       job.account.Tag,
			},
			Asset: rawAsset{
				Kind:         job.asset.Kind,
				BizID:        job.asset.BizID,
				Address:      job.asset.Address.Hex(),
				Symbol:       job.asset.Symbol,
				Decimals:     job.asset.Decimals,
				DefaultPrice: job.asset.DefaultPrice,
				BizName:      job.asset.BizName,
			},
			BalanceWei: balance.String(),
		}

		payload, err := json.Marshal(raw)
		if err != nil {
			continue
		}

		msg := &types.Message{
			Metadata: map[string]any{
				"chain_id":     c.chainID,
				"account_id":   job.account.ID,
				"asset_type":   job.asset.Kind,
				"biz_id":       job.asset.BizID,
				"block_number": latest,
			},
			Payload: payload,
		}
		messages = append(messages, msg)
	}
	c.setLastBlock(latest)
	return messages, nil
}

func (c *balanceSnapshotCaller) buildBatch(blockNumber uint64) ([]*balanceJob, []rpc.BatchElem) {
	total := len(c.accounts) * (len(c.tokens) + len(c.pairs))
	jobs := make([]*balanceJob, 0, total)
	batch := make([]rpc.BatchElem, 0, total)
	blockArg := hexutil.EncodeUint64(blockNumber)

	for _, account := range c.accounts {
		for _, token := range c.tokens {
			var result hexutil.Bytes
			jobs = append(jobs, &balanceJob{
				account: account,
				asset: assetInfo{
					Kind:         "erc20",
					BizID:        token.ID,
					Address:      token.Address,
					Symbol:       token.Symbol,
					Decimals:     token.Decimals,
					DefaultPrice: token.DefaultPrice,
					BizName:      token.BizName,
				},
				result: &result,
			})
			batch = append(batch, rpc.BatchElem{
				Method: "eth_call",
				Args: []interface{}{map[string]interface{}{
					"to":   token.Address.Hex(),
					"data": hexutil.Encode(balanceOfData(account.Address)),
				}, blockArg},
				Result: &result,
			})
		}

		for _, pair := range c.pairs {
			var result hexutil.Bytes
			jobs = append(jobs, &balanceJob{
				account: account,
				asset: assetInfo{
					Kind:         "lp",
					BizID:        pair.ID,
					Address:      pair.Address,
					Symbol:       pair.Symbol,
					Decimals:     18,
					DefaultPrice: pair.DefaultPrice,
					BizName:      pair.Symbol,
				},
				result: &result,
			})
			batch = append(batch, rpc.BatchElem{
				Method: "eth_call",
				Args: []interface{}{map[string]interface{}{
					"to":   pair.Address.Hex(),
					"data": hexutil.Encode(balanceOfData(account.Address)),
				}, blockArg},
				Result: &result,
			})
		}
	}

	return jobs, batch
}

func (c *balanceSnapshotCaller) setLastBlock(block uint64) {
	c.mu.Lock()
	if block > c.lastBlock {
		c.lastBlock = block
	}
	c.mu.Unlock()
}

func balanceOfData(account common.Address) []byte {
	data := make([]byte, 4+32)
	copy(data[:4], balanceOfSig)
	copy(data[4:], common.LeftPadBytes(account.Bytes(), 32))
	return data
}

// Deployment helpers -----------------------------------------------------

type deploymentFile struct {
	Accounts []deploymentAccount `json:"accounts"`
	Tokens   []deploymentToken   `json:"tokens"`
	Pairs    []deploymentPair    `json:"pairs"`
}

type deploymentAccount struct {
	Address string `json:"address"`
	ID      string `json:"id"`
	Tag     string `json:"tag"`
}

type deploymentToken struct {
	Address  string `json:"address"`
	Symbol   string `json:"symbol"`
	Decimals string `json:"decimals"`
	ID       string `json:"id"`
}

type deploymentPair struct {
	Address  string `json:"address"`
	Token0   string `json:"token0"`
	Token1   string `json:"token1"`
	PairName string `json:"pairName"`
	ID       string `json:"id"`
}

func loadDeploymentAssets(path string) ([]accountConfig, []assetToken, []assetPair, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("balance_snapshot: read deployment: %w", err)
	}

	var dep deploymentFile
	if err := json.Unmarshal(data, &dep); err != nil {
		return nil, nil, nil, fmt.Errorf("balance_snapshot: parse deployment: %w", err)
	}

	accounts := make([]accountConfig, 0, len(dep.Accounts))
	for _, acc := range dep.Accounts {
		id := parseInt64(acc.ID)
		accounts = append(accounts, accountConfig{
			ID:        id,
			Address:   common.HexToAddress(acc.Address),
			LabelMask: tagToMask(acc.Tag),
			Tag:       acc.Tag,
		})
	}

	tokens := make([]assetToken, 0, len(dep.Tokens))
	tokenMap := make(map[string]assetToken, len(dep.Tokens))
	for _, tok := range dep.Tokens {
		decimals := parseInt(tok.Decimals, 18)
		id := parseInt64(tok.ID)
		token := assetToken{
			ID:           id,
			Address:      common.HexToAddress(tok.Address),
			Symbol:       tok.Symbol,
			Decimals:     decimals,
			DefaultPrice: defaultTokenPrice(tok.Symbol),
			BizName:      tok.Symbol,
		}
		tokens = append(tokens, token)
		tokenMap[strings.ToLower(tok.Address)] = token
	}

	pairs := make([]assetPair, 0, len(dep.Pairs))
	for idx, pr := range dep.Pairs {
		id := parseInt64(pr.ID)
		if id == 0 {
			id = int64(idx + 1)
		}
		token0 := tokenMap[strings.ToLower(pr.Token0)]
		token1 := tokenMap[strings.ToLower(pr.Token1)]
		symbol := pr.PairName
		if symbol == "" {
			symbol = fmt.Sprintf("%s-%s LP", token0.Symbol, token1.Symbol)
		}
		pairs = append(pairs, assetPair{
			ID:           id,
			Address:      common.HexToAddress(pr.Address),
			Symbol:       symbol,
			DefaultPrice: averagePrice(token0.DefaultPrice, token1.DefaultPrice),
		})
	}

	return accounts, tokens, pairs, nil
}

func tagToMask(tag string) int {
	switch strings.ToLower(tag) {
	case "cex":
		return 1
	case "smart":
		return 2
	case "whale":
		return 4
	case "public":
		return 8
	case "fresh":
		return 16
	default:
		return 0
	}
}

func defaultTokenPrice(symbol string) string {
	switch strings.ToUpper(symbol) {
	case "USDC", "DAI":
		return "1"
	case "WETH":
		return "3000"
	case "TWI":
		return "50"
	case "WBTC":
		return "120000"
	default:
		return "1"
	}
}

func averagePrice(a, b string) string {
	fa, okA := new(big.Float).SetString(a)
	fb, okB := new(big.Float).SetString(b)
	if !okA || !okB {
		return "1"
	}
	sum := new(big.Float).Add(fa, fb)
	half := new(big.Float).Quo(sum, big.NewFloat(4))
	return half.Text('f', 18)
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	if v, ok := new(big.Int).SetString(s, 10); ok {
		return int(v.Int64())
	}
	return def
}

func parseInt64(s string) int64 {
	if s == "" {
		return 0
	}
	if v, ok := new(big.Int).SetString(s, 10); ok {
		return v.Int64()
	}
	return 0
}

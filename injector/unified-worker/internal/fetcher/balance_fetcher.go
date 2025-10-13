package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"
	
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// BalanceFetcher 余额获取器（参考listener的BalanceSnapshotGenerator）
type BalanceFetcher struct {
	client *ethclient.Client
}

// NewBalanceFetcher 创建余额获取器
func NewBalanceFetcher(client interface{}) DataFetcher {
	ethClient, ok := client.(*ethclient.Client)
	if !ok {
		log.Printf("[BalanceFetcher] 警告: client类型不匹配，期望*ethclient.Client")
		return &BalanceFetcher{}
	}
	
	return &BalanceFetcher{
		client: ethClient,
	}
}

// Name 返回fetcher名称
func (bf *BalanceFetcher) Name() string {
	return "balance"
}

// Fetch 获取余额数据
// config应包含: accounts([]string), tokens([]map[string]interface{})
func (bf *BalanceFetcher) Fetch(ctx context.Context, config map[string]interface{}) ([]byte, error) {
	if bf.client == nil {
		return nil, fmt.Errorf("ethereum client未初始化")
	}
	
	// 解析账户列表
	accountsRaw, ok := config["accounts"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("缺少accounts配置")
	}
	
	accounts := make([]string, 0, len(accountsRaw))
	for _, acc := range accountsRaw {
		if accStr, ok := acc.(string); ok {
			accounts = append(accounts, accStr)
		}
	}
	
	// 解析token列表
	tokensRaw, ok := config["tokens"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("缺少tokens配置")
	}
	
	tokens := make([]map[string]string, 0, len(tokensRaw))
	for _, tok := range tokensRaw {
		if tokMap, ok := tok.(map[string]interface{}); ok {
			token := make(map[string]string)
			if addr, ok := tokMap["address"].(string); ok {
				token["address"] = addr
			}
			if symbol, ok := tokMap["symbol"].(string); ok {
				token["symbol"] = symbol
			}
			tokens = append(tokens, token)
		}
	}
	
	// 获取当前区块号和时间戳
	blockNum, err := bf.client.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取区块号失败: %w", err)
	}
	
	timestamp := time.Now().UnixMilli()
	
	// 收集所有余额快照
	snapshots := []map[string]interface{}{}
	
	for _, account := range accounts {
		for _, token := range tokens {
			balance, err := bf.getERC20Balance(ctx, token["address"], account)
			if err != nil {
				log.Printf("[BalanceFetcher] 获取余额失败: account=%s, token=%s, error=%v", 
					account, token["address"], err)
				continue
			}
			
			snapshot := map[string]interface{}{
				"account":     account,
				"tokenSymbol": token["symbol"],
				"token":       token["address"],
				"balance":     balance.String(),
				"blockNumber": blockNum,
				"timestamp":   timestamp,
				"price":       "1", // TODO: 从Redis获取价格
			}
			
			snapshots = append(snapshots, snapshot)
		}
	}
	
	// 返回JSON数组
	return json.Marshal(snapshots)
}

// getERC20Balance 获取ERC20 token余额（参考listener实现）
func (bf *BalanceFetcher) getERC20Balance(ctx context.Context, tokenAddress, accountAddress string) (*big.Int, error) {
	contractAddress := common.HexToAddress(tokenAddress)
	account := common.HexToAddress(accountAddress)
	
	// balanceOf function signature: 0x70a08231
	data := append([]byte{0x70, 0xa0, 0x82, 0x31}, common.LeftPadBytes(account.Bytes(), 32)...)
	
	result, err := bf.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddress,
		Data: data,
	}, nil)
	
	if err != nil {
		return nil, fmt.Errorf("调用balanceOf失败: %w", err)
	}
	
	if len(result) == 0 {
		return big.NewInt(0), nil
	}
	
	balance := new(big.Int).SetBytes(result)
	return balance, nil
}

// getTokenPrice 获取token价格（简化版，TODO: 从Redis获取）
func (bf *BalanceFetcher) getTokenPrice(tokenAddress string) string {
	address := strings.ToLower(tokenAddress)
	
	// TODO: 从Redis获取价格
	_ = address
	
	// 默认价格
	return "1"
}

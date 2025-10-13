package fetcher

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// BlockFetcher 区块获取器（参考listener实现）
type BlockFetcher struct {
	client *ethclient.Client
}

// NewBlockFetcher 创建区块获取器
func NewBlockFetcher(client interface{}) DataFetcher {
	ethClient, ok := client.(*ethclient.Client)
	if !ok {
		return &BlockFetcher{}
	}
	
	return &BlockFetcher{
		client: ethClient,
	}
}

// Name 返回fetcher名称
func (bf *BlockFetcher) Name() string {
	return "block"
}

// Fetch 获取区块数据（参考listener的processBlock逻辑）
// config应包含: block_number(string), include_receipts(bool)
func (bf *BlockFetcher) Fetch(ctx context.Context, config map[string]interface{}) ([]byte, error) {
	if bf.client == nil {
		return nil, fmt.Errorf("ethereum client未初始化")
	}
	
	// 解析区块号
	blockNum := bf.parseBlockNumber(config)
	
	// 获取区块（参考listener: client.BlockByNumber）
	block, err := bf.client.BlockByNumber(ctx, blockNum)
	if err != nil {
		return nil, fmt.Errorf("获取区块失败: %w", err)
	}
	
	// 是否包含receipts（优化：可选）
	includeReceipts := true
	if ir, ok := config["include_receipts"].(bool); ok {
		includeReceipts = ir
	}
	
	// 转换为JSON（参考listener的输出格式）
	return bf.blockToJSON(ctx, block, includeReceipts)
}

// parseBlockNumber 解析区块号
func (bf *BlockFetcher) parseBlockNumber(config map[string]interface{}) *big.Int {
	if bn, ok := config["block_number"].(string); ok {
		if bn == "latest" {
			return nil // nil表示latest
		}
		// 解析十六进制或十进制
		blockNum := new(big.Int)
		if _, ok := blockNum.SetString(bn, 0); ok {
			return blockNum
		}
	}
	
	if bn, ok := config["block_number"].(int64); ok {
		return big.NewInt(bn)
	}
	
	return nil // 默认latest
}

// blockToJSON 将区块转换为JSON（参考listener格式）
func (bf *BlockFetcher) blockToJSON(ctx context.Context, block *types.Block, includeReceipts bool) ([]byte, error) {
	// 基础区块信息
	result := map[string]interface{}{
		"number":     fmt.Sprintf("0x%x", block.Number().Uint64()),
		"hash":       block.Hash().Hex(),
		"timestamp":  fmt.Sprintf("0x%x", block.Time()),
		"parentHash": block.ParentHash().Hex(),
		"miner":      block.Coinbase().Hex(),
		"gasLimit":   fmt.Sprintf("0x%x", block.GasLimit()),
		"gasUsed":    fmt.Sprintf("0x%x", block.GasUsed()),
	}
	
	// 处理交易（参考listener的processTransaction逻辑）
	transactions := make([]interface{}, 0)
	
	for _, tx := range block.Transactions() {
		txData := bf.transactionToJSON(tx)
		
		// 如果需要receipts，获取交易收据和logs（参考listener）
		if includeReceipts {
			receipt, err := bf.client.TransactionReceipt(ctx, tx.Hash())
			if err != nil {
				// 日志错误但继续（优化：更健壮）
				txData["logs"] = []interface{}{}
			} else {
				txData["logs"] = bf.logsToJSON(receipt.Logs)
				txData["status"] = fmt.Sprintf("0x%x", receipt.Status)
				txData["gasUsed"] = fmt.Sprintf("0x%x", receipt.GasUsed)
			}
		}
		
		transactions = append(transactions, txData)
	}
	
	result["transactions"] = transactions
	
	return json.Marshal(result)
}

// transactionToJSON 将交易转换为JSON
func (bf *BlockFetcher) transactionToJSON(tx *types.Transaction) map[string]interface{} {
	v, r, s := tx.RawSignatureValues()
	
	txData := map[string]interface{}{
		"hash":     tx.Hash().Hex(),
		"nonce":    fmt.Sprintf("0x%x", tx.Nonce()),
		"gas":      fmt.Sprintf("0x%x", tx.Gas()),
		"gasPrice": fmt.Sprintf("0x%x", tx.GasPrice().Uint64()),
		"value":    fmt.Sprintf("0x%x", tx.Value().Uint64()),
		"input":    "0x" + hex.EncodeToString(tx.Data()),
		"v":        fmt.Sprintf("0x%x", v),
		"r":        fmt.Sprintf("0x%x", r),
		"s":        fmt.Sprintf("0x%x", s),
	}
	
	if tx.To() != nil {
		txData["to"] = tx.To().Hex()
	} else {
		txData["to"] = nil // 合约创建
	}
	
	return txData
}

// logsToJSON 将logs转换为JSON（参考listener的log处理）
func (bf *BlockFetcher) logsToJSON(logs []*types.Log) []interface{} {
	result := make([]interface{}, 0, len(logs))
	
	for _, log := range logs {
		topics := make([]string, 0, len(log.Topics))
		for _, topic := range log.Topics {
			topics = append(topics, topic.Hex())
		}
		
		logData := map[string]interface{}{
			"address":          log.Address.Hex(),
			"topics":           topics,
			"data":             "0x" + hex.EncodeToString(log.Data),
			"blockNumber":      fmt.Sprintf("0x%x", log.BlockNumber),
			"transactionHash":  log.TxHash.Hex(),
			"transactionIndex": fmt.Sprintf("0x%x", log.TxIndex),
			"blockHash":        log.BlockHash.Hex(),
			"logIndex":         fmt.Sprintf("0x%x", log.Index),
			"removed":          log.Removed,
		}
		
		result = append(result, logData)
	}
	
	return result
}

// 优化点：支持批量获取区块（listener中是逐个获取）
// FetchRange 批量获取区块范围
func (bf *BlockFetcher) FetchRange(ctx context.Context, fromBlock, toBlock uint64) ([][]byte, error) {
	if bf.client == nil {
		return nil, fmt.Errorf("ethereum client未初始化")
	}
	
	results := make([][]byte, 0, toBlock-fromBlock+1)
	
	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		config := map[string]interface{}{
			"block_number":     int64(blockNum),
			"include_receipts": true,
		}
		
		data, err := bf.Fetch(ctx, config)
		if err != nil {
			return nil, fmt.Errorf("获取区块%d失败: %w", blockNum, err)
		}
		
		results = append(results, data)
	}
	
	return results, nil
}

// 优化点：支持过滤指定合约的logs（listener中会过滤关注的合约）
// FetchWithFilter 获取区块并过滤特定合约的logs
func (bf *BlockFetcher) FetchWithFilter(ctx context.Context, config map[string]interface{}, contractAddresses []common.Address) ([]byte, error) {
	// 先获取完整区块
	data, err := bf.Fetch(ctx, config)
	if err != nil {
		return nil, err
	}
	
	// 如果没有过滤条件，直接返回
	if len(contractAddresses) == 0 {
		return data, nil
	}
	
	// 解析并过滤（优化：减少不必要的数据传输）
	var blockData map[string]interface{}
	if err := json.Unmarshal(data, &blockData); err != nil {
		return data, nil // 解析失败，返回原始数据
	}
	
	// 过滤transactions中的logs
	if txs, ok := blockData["transactions"].([]interface{}); ok {
		for _, tx := range txs {
			if txMap, ok := tx.(map[string]interface{}); ok {
				if logs, ok := txMap["logs"].([]interface{}); ok {
					filteredLogs := make([]interface{}, 0)
					for _, log := range logs {
						if logMap, ok := log.(map[string]interface{}); ok {
							if addr, ok := logMap["address"].(string); ok {
								// 检查是否在过滤列表中
								logAddr := common.HexToAddress(addr)
								for _, targetAddr := range contractAddresses {
									if logAddr == targetAddr {
										filteredLogs = append(filteredLogs, log)
										break
									}
								}
							}
						}
					}
					txMap["logs"] = filteredLogs
				}
			}
		}
	}
	
	return json.Marshal(blockData)
}

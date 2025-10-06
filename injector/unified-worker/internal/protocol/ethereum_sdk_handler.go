package protocol

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strings"
	
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	
	utypes "unified-worker/pkg/types"
)

// EthereumSDKHandler go-ethereum SDK协议处理器
// 优势：内置重连、连接池、心跳等能力，无需额外Runtime支持
type EthereumSDKHandler struct {
	client   *ethclient.Client
	endpoint string
	chainID  *big.Int
}

// NewEthereumSDKHandler 创建Ethereum SDK处理器
func NewEthereumSDKHandler() *EthereumSDKHandler {
	return &EthereumSDKHandler{}
}

// Type 返回协议类型
func (h *EthereumSDKHandler) Type() utypes.ProtocolType {
	return utypes.ProtocolEthereumSDK
}

// Initialize 初始化SDK
func (h *EthereumSDKHandler) Initialize(ctx context.Context, config map[string]interface{}) error {
	// 解析endpoint
	endpoint, ok := config["endpoint"].(string)
	if !ok {
		// 兼容url字段
		endpoint, ok = config["url"].(string)
		if !ok {
			return fmt.Errorf("缺少endpoint配置")
		}
	}
	h.endpoint = endpoint
	
	log.Printf("[EthereumSDK] 连接到节点: %s", endpoint)
	
	// 创建客户端（go-ethereum内置重连、连接池等能力）
	client, err := ethclient.DialContext(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("连接Ethereum节点失败: %w", err)
	}
	h.client = client
	
	// 获取chain ID
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("获取chain ID失败: %w", err)
	}
	h.chainID = chainID
	
	log.Printf("[EthereumSDK] 连接成功: chain_id=%s", chainID.String())
	
	return nil
}

// Send 发送请求（支持JSON-RPC调用）
func (h *EthereumSDKHandler) Send(ctx context.Context, message []byte) ([]byte, error) {
	// 解析JSON-RPC请求
	var request map[string]interface{}
	if err := json.Unmarshal(message, &request); err != nil {
		return nil, fmt.Errorf("解析JSON-RPC请求失败: %w", err)
	}
	
	method, ok := request["method"].(string)
	if !ok {
		return nil, fmt.Errorf("缺少method字段")
	}
	
	params, _ := request["params"].([]interface{})
	
	log.Printf("[EthereumSDK] 调用方法: %s", method)
	
	// 根据method调用SDK方法
	var result interface{}
	var err error
	
	switch method {
	case "eth_getBlockByNumber":
		result, err = h.getBlockByNumber(ctx, params)
	case "eth_blockNumber":
		result, err = h.blockNumber(ctx)
	case "eth_getTransactionReceipt":
		result, err = h.getTransactionReceipt(ctx, params)
	default:
		return nil, fmt.Errorf("不支持的方法: %s", method)
	}
	
	if err != nil {
		return nil, err
	}
	
	// 构造JSON-RPC响应
	response := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      request["id"],
		"result":  result,
	}
	
	return json.Marshal(response)
}

// getBlockByNumber 获取区块（参考listener实现）
func (h *EthereumSDKHandler) getBlockByNumber(ctx context.Context, params []interface{}) (interface{}, error) {
	if len(params) < 1 {
		return nil, fmt.Errorf("缺少block参数")
	}
	
	blockParam, ok := params[0].(string)
	if !ok {
		return nil, fmt.Errorf("无效的block参数")
	}
	
	var blockNumber *big.Int
	if blockParam == "latest" {
		blockNumber = nil // nil表示latest
	} else {
		// 解析十六进制区块号
		blockNumber = new(big.Int)
		blockParam = strings.TrimPrefix(blockParam, "0x")
		if _, ok := blockNumber.SetString(blockParam, 16); !ok {
			return nil, fmt.Errorf("无效的区块号: %s", blockParam)
		}
	}
	
	// 判断是否需要完整交易信息
	fullTx := false
	if len(params) > 1 {
		fullTx, _ = params[1].(bool)
	}
	
	// 使用SDK获取区块（参考listener的l.client.BlockByNumber）
	block, err := h.client.BlockByNumber(ctx, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("获取区块失败: %w", err)
	}
	
	// 转换为JSON格式
	return h.blockToJSON(ctx, block, fullTx)
}

// blockNumber 获取当前区块号
func (h *EthereumSDKHandler) blockNumber(ctx context.Context) (interface{}, error) {
	num, err := h.client.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取区块号失败: %w", err)
	}
	
	return fmt.Sprintf("0x%x", num), nil
}

// getTransactionReceipt 获取交易收据
func (h *EthereumSDKHandler) getTransactionReceipt(ctx context.Context, params []interface{}) (interface{}, error) {
	if len(params) < 1 {
		return nil, fmt.Errorf("缺少交易哈希参数")
	}
	
	txHashStr, ok := params[0].(string)
	if !ok {
		return nil, fmt.Errorf("无效的交易哈希")
	}
	
	txHash := common.HexToHash(txHashStr)
	receipt, err := h.client.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("获取交易收据失败: %w", err)
	}
	
	return h.receiptToJSON(receipt), nil
}

// blockToJSON 将区块转换为JSON（参考listener的实现）
func (h *EthereumSDKHandler) blockToJSON(ctx context.Context, block *types.Block, includeLogs bool) (map[string]interface{}, error) {
	// 基础区块信息
	result := map[string]interface{}{
		"number":       fmt.Sprintf("0x%x", block.Number().Uint64()),
		"hash":         block.Hash().Hex(),
		"timestamp":    fmt.Sprintf("0x%x", block.Time()),
		"parentHash":   block.ParentHash().Hex(),
		"miner":        block.Coinbase().Hex(),
		"difficulty":   fmt.Sprintf("0x%x", block.Difficulty().Uint64()),
		"gasLimit":     fmt.Sprintf("0x%x", block.GasLimit()),
		"gasUsed":      fmt.Sprintf("0x%x", block.GasUsed()),
	}
	
	// 处理交易（参考listener的block.Transactions()遍历）
	transactions := make([]interface{}, 0)
	
	for _, tx := range block.Transactions() {
		txMap := h.transactionToJSON(tx)
		
		// 如果需要logs，获取交易收据（参考listener的l.client.TransactionReceipt）
		if includeLogs {
			receipt, err := h.client.TransactionReceipt(ctx, tx.Hash())
			if err != nil {
				log.Printf("[EthereumSDK] 获取交易收据失败: %s, error: %v", tx.Hash().Hex(), err)
				txMap["logs"] = []interface{}{}
			} else {
				txMap["logs"] = h.logsToJSON(receipt.Logs)
			}
		}
		
		transactions = append(transactions, txMap)
	}
	
	result["transactions"] = transactions
	
	return result, nil
}

// transactionToJSON 将交易转换为JSON
func (h *EthereumSDKHandler) transactionToJSON(tx *types.Transaction) map[string]interface{} {
	v, r, s := tx.RawSignatureValues()
	
	txMap := map[string]interface{}{
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
		txMap["to"] = tx.To().Hex()
	}
	
	return txMap
}

// logsToJSON 将logs转换为JSON
func (h *EthereumSDKHandler) logsToJSON(logs []*types.Log) []interface{} {
	result := make([]interface{}, 0, len(logs))
	
	for _, log := range logs {
		topics := make([]string, 0, len(log.Topics))
		for _, topic := range log.Topics {
			topics = append(topics, topic.Hex())
		}
		
		logMap := map[string]interface{}{
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
		
		result = append(result, logMap)
	}
	
	return result
}

// receiptToJSON 将交易收据转换为JSON
func (h *EthereumSDKHandler) receiptToJSON(receipt *types.Receipt) map[string]interface{} {
	return map[string]interface{}{
		"transactionHash":  receipt.TxHash.Hex(),
		"blockNumber":      fmt.Sprintf("0x%x", receipt.BlockNumber.Uint64()),
		"blockHash":        receipt.BlockHash.Hex(),
		"gasUsed":          fmt.Sprintf("0x%x", receipt.GasUsed),
		"cumulativeGasUsed": fmt.Sprintf("0x%x", receipt.CumulativeGasUsed),
		"contractAddress":  receipt.ContractAddress.Hex(),
		"logs":             h.logsToJSON(receipt.Logs),
		"status":           fmt.Sprintf("0x%x", receipt.Status),
	}
}

// Receive 接收消息（SDK不支持订阅模式）
func (h *EthereumSDKHandler) Receive(ctx context.Context) (<-chan []byte, <-chan error) {
	// Ethereum SDK用于轮询，不支持订阅
	dataChan := make(chan []byte)
	errChan := make(chan error, 1)
	errChan <- fmt.Errorf("EthereumSDK不支持订阅模式")
	return dataChan, errChan
}

// HealthCheck 健康检查
func (h *EthereumSDKHandler) HealthCheck(ctx context.Context) error {
	_, err := h.client.BlockNumber(ctx)
	return err
}

// Close 关闭连接
func (h *EthereumSDKHandler) Close() error {
	if h.client != nil {
		h.client.Close()
	}
	return nil
}

// Metadata 返回协议元数据（声明SDK内置能力）
func (h *EthereumSDKHandler) Metadata() utypes.ProtocolMetadata {
	return utypes.ProtocolMetadata{
		SupportsBidirectional:  false, // SDK用于轮询，不支持双向
		RequiresHeartbeat:      false, // SDK不需要心跳
		RequiresReconnect:      false, // SDK不需要重连（已内置）
		RequiresConnectionPool: false, // SDK不需要连接池（已内置）
		RequiresRateLimit:      true,  // SDK仍需要限流
		
		// SDK内置能力声明
		HasBuiltInReconnect: true,  // go-ethereum内置自动重连
		HasBuiltInRateLimit: false, // 需要外部限流
		HasBuiltInHeartbeat: true,  // go-ethereum内置keep-alive
	}
}

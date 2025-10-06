package parser

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
)

// DexParser DEX事件解析器（优化版本，替代listener中的硬编码逻辑）
type DexParser struct {
	BaseParser
	// 事件签名映射（优化：使用map而不是硬编码switch）
	eventSignatures map[string]string
	// 合约地址映射（从deployment.json加载）
	contracts map[string]ContractInfo
}

// ContractInfo 合约信息
type ContractInfo struct {
	Address          string
	Type             string // Factory, Pair, Token
	InterestedEvents []string
}

// NewDexParser 创建DEX解析器
func NewDexParser() *DexParser {
	return &DexParser{
		eventSignatures: map[string]string{
			// DEX Factory事件
			"0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9": "PairCreated",
			// DEX Pair事件
			"0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": "Swap",
			"0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f": "Mint",
			"0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496": "Burn",
			"0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1": "Sync",
			// ERC20 Token事件
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "Transfer",
			"0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925": "Approval",
		},
		contracts: make(map[string]ContractInfo),
	}
}

// CanHandle 判断是否能处理
func (dp *DexParser) CanHandle(dataSourceID string, taskType string) bool {
	// 处理hardhat本地节点或包含dex的数据源
	return strings.Contains(strings.ToLower(dataSourceID), "hardhat") ||
		strings.Contains(strings.ToLower(dataSourceID), "local") ||
		strings.Contains(strings.ToLower(dataSourceID), "dex")
}

// Parse 解析数据
func (dp *DexParser) Parse(ctx context.Context, data []byte, config *ParserConfig) (*ParsedData, error) {
	// 解析JSON
	var blockData map[string]interface{}
	if err := json.Unmarshal(data, &blockData); err != nil {
		return dp.TryNext(ctx, data, config)
	}

	// 检查是否是区块数据
	if _, hasNumber := blockData["number"]; !hasNumber {
		return dp.TryNext(ctx, data, config)
	}

	// 提取区块信息
	blockNumber := blockData["number"]

	// 解析交易和事件
	transactions, ok := blockData["transactions"].([]interface{})
	if !ok || len(transactions) == 0 {
		// 没有交易，返回空（不输出到Kafka）
		return nil, fmt.Errorf("区块无交易，跳过")
	}

	// 提取所有包含DEX事件的交易
	// 输出格式：每个交易一条消息（与listener保持一致）
	allTransactions := []map[string]interface{}{}

	// 遍历每个交易，提取包含DEX事件的交易
	for _, tx := range transactions {
		txMap, ok := tx.(map[string]interface{})
		if !ok {
			continue
		}

		// 获取交易receipt（包含logs）
		logs, ok := txMap["logs"].([]interface{})
		if !ok || len(logs) == 0 {
			continue
		}

		// 提取此交易的所有DEX事件
		txEvents := []map[string]interface{}{}

		for _, logEntry := range logs {
			logMap, ok := logEntry.(map[string]interface{})
			if !ok {
				continue
			}

			// 提取topics
			topics, ok := logMap["topics"].([]interface{})
			if !ok || len(topics) == 0 {
				continue
			}

			// 获取事件签名
			eventSignature, ok := topics[0].(string)
			if !ok {
				continue
			}

			// 判断是否是我们感兴趣的事件
			eventName, exists := dp.eventSignatures[eventSignature]
			if !exists {
				continue
			}

			// 解析事件参数
			decodedArgs := dp.decodeEventArgs(eventName, topics, logMap["data"])

			// 构造事件对象（与listener格式一致）
			event := map[string]interface{}{
				"eventName":       eventName,
				"contractAddress": logMap["address"],
				"logIndex":        logMap["logIndex"],
				"blockNumber":     dp.parseIntField(logMap["blockNumber"]),
				"topics":          topics,
				"eventData":       logMap["data"],
				"decodedArgs":     decodedArgs,
			}

			txEvents = append(txEvents, event)
		}

		// 如果此交易有DEX事件，构造完整的交易+事件输出
		if len(txEvents) > 0 {
			txOutput := map[string]interface{}{
				"transaction": dp.buildTransactionInfo(txMap, blockData),
				"events":      txEvents,
			}
			allTransactions = append(allTransactions, txOutput)
		}
	}

	// 返回多条消息（每个交易一条）
	return &ParsedData{
		OriginalData: data,
		ExtractedData: map[string]interface{}{
			"transactions": allTransactions, // 多条交易消息
		},
		Metadata: map[string]interface{}{
			"parser_type":       "dex",
			"block_number":      blockNumber,
			"transaction_count": len(allTransactions),
		},
	}, nil
}

// decodeEventArgs 解码事件参数（优化版本，使用表驱动而非硬编码switch）
func (dp *DexParser) decodeEventArgs(eventName string, topics []interface{}, dataHex interface{}) map[string]interface{} {
	decodedArgs := make(map[string]interface{})

	dataStr, ok := dataHex.(string)
	if !ok {
		return decodedArgs
	}

	// 移除0x前缀
	dataStr = strings.TrimPrefix(dataStr, "0x")
	dataBytes, err := hex.DecodeString(dataStr)
	if err != nil {
		return decodedArgs
	}

	// 根据事件类型解析（优化：使用配置化的解析规则）
	switch eventName {
	case "Swap":
		if len(dataBytes) >= 128 {
			decodedArgs["amount0In"] = new(big.Int).SetBytes(dataBytes[0:32]).String()
			decodedArgs["amount1In"] = new(big.Int).SetBytes(dataBytes[32:64]).String()
			decodedArgs["amount0Out"] = new(big.Int).SetBytes(dataBytes[64:96]).String()
			decodedArgs["amount1Out"] = new(big.Int).SetBytes(dataBytes[96:128]).String()
		}
		if len(topics) >= 3 {
			decodedArgs["sender"] = fmt.Sprintf("%v", topics[1])
			decodedArgs["to"] = fmt.Sprintf("%v", topics[2])
		}

	case "Mint":
		if len(dataBytes) >= 64 {
			decodedArgs["amount0"] = new(big.Int).SetBytes(dataBytes[0:32]).String()
			decodedArgs["amount1"] = new(big.Int).SetBytes(dataBytes[32:64]).String()
		}
		if len(topics) >= 2 {
			decodedArgs["sender"] = fmt.Sprintf("%v", topics[1])
		}

	case "Burn":
		if len(dataBytes) >= 64 {
			decodedArgs["amount0"] = new(big.Int).SetBytes(dataBytes[0:32]).String()
			decodedArgs["amount1"] = new(big.Int).SetBytes(dataBytes[32:64]).String()
		}
		if len(topics) >= 3 {
			decodedArgs["sender"] = fmt.Sprintf("%v", topics[1])
			decodedArgs["to"] = fmt.Sprintf("%v", topics[2])
		}

	case "Sync":
		if len(dataBytes) >= 64 {
			decodedArgs["reserve0"] = new(big.Int).SetBytes(dataBytes[0:32]).String()
			decodedArgs["reserve1"] = new(big.Int).SetBytes(dataBytes[32:64]).String()
		}

	case "Transfer":
		if len(topics) >= 3 {
			decodedArgs["from"] = fmt.Sprintf("%v", topics[1])
			decodedArgs["to"] = fmt.Sprintf("%v", topics[2])
		}
		if len(dataBytes) >= 32 {
			decodedArgs["value"] = new(big.Int).SetBytes(dataBytes[0:32]).String()
		}

	case "Approval":
		if len(topics) >= 3 {
			decodedArgs["owner"] = fmt.Sprintf("%v", topics[1])
			decodedArgs["spender"] = fmt.Sprintf("%v", topics[2])
		}
		if len(dataBytes) >= 32 {
			decodedArgs["value"] = new(big.Int).SetBytes(dataBytes[0:32]).String()
		}

	case "PairCreated":
		if len(topics) >= 3 {
			decodedArgs["token0"] = fmt.Sprintf("%v", topics[1])
			decodedArgs["token1"] = fmt.Sprintf("%v", topics[2])
		}
		if len(dataBytes) >= 64 {
			// pair地址在data的前32字节
			pairAddr := dataBytes[12:32] // 取后20字节（地址）
			decodedArgs["pair"] = "0x" + hex.EncodeToString(pairAddr)
		}
	}

	return decodedArgs
}

// GetSequence 获取序列号
func (dp *DexParser) GetSequence(parsedData *ParsedData) (interface{}, error) {
	// DEX parser输出多条消息，序列号从metadata中获取
	if blockNumber, ok := parsedData.Metadata["block_number"]; ok {
		return blockNumber, nil
	}
	return nil, fmt.Errorf("无法提取序列号")
}

// buildTransactionInfo 构建交易信息（与listener格式一致）
func (dp *DexParser) buildTransactionInfo(txMap map[string]interface{}, blockData map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"blockNumber":       dp.parseIntField(blockData["number"]),
		"blockHash":         blockData["hash"],
		"timestamp":         dp.parseIntField(blockData["timestamp"]),
		"transactionHash":   txMap["hash"],
		"transactionIndex":  dp.parseIntField(txMap["transactionIndex"]),
		"transactionStatus": dp.parseStatus(txMap),
		"gasUsed":           dp.parseIntField(txMap["gasUsed"]),
		"gasPrice":          dp.parseHexToDecString(txMap["gasPrice"]),
		"nonce":             dp.parseIntField(txMap["nonce"]),
		"fromAddress":       dp.getFrom(txMap),
		"toAddress":         txMap["to"],
		"transactionValue":  dp.parseHexToDecString(txMap["value"]),
		"inputData":         txMap["input"],
		"chainID":           "31337", // TODO: 从配置或区块中获取
	}
}

// parseIntField 解析整数字段（十六进制转十进制）
func (dp *DexParser) parseIntField(field interface{}) interface{} {
	if field == nil {
		return 0
	}

	str, ok := field.(string)
	if !ok {
		return field
	}

	// 移除0x前缀
	str = strings.TrimPrefix(str, "0x")
	if str == "" {
		return 0
	}

	// 解析为十进制整数
	val := new(big.Int)
	if _, ok := val.SetString(str, 16); ok {
		return val.Int64()
	}

	return 0
}

// parseHexToDecString 解析十六进制字符串为十进制字符串
func (dp *DexParser) parseHexToDecString(field interface{}) string {
	if field == nil {
		return "0"
	}

	str, ok := field.(string)
	if !ok {
		return "0"
	}

	str = strings.TrimPrefix(str, "0x")
	if str == "" {
		return "0"
	}

	val := new(big.Int)
	if _, ok := val.SetString(str, 16); ok {
		return val.String()
	}

	return "0"
}

// parseStatus 解析交易状态
func (dp *DexParser) parseStatus(txMap map[string]interface{}) string {
	// 从logs判断：有logs通常表示成功
	if logs, ok := txMap["logs"].([]interface{}); ok && len(logs) > 0 {
		return "success"
	}

	// 如果有status字段
	if status, ok := txMap["status"]; ok {
		if statusStr, ok := status.(string); ok {
			statusStr = strings.TrimPrefix(statusStr, "0x")
			if statusStr == "1" {
				return "success"
			}
		}
	}

	return "unknown"
}

// getFrom 获取from地址
func (dp *DexParser) getFrom(txMap map[string]interface{}) string {
	// 优先使用from字段
	if from, ok := txMap["from"].(string); ok {
		return from
	}

	// 如果没有from，可以从签名恢复（暂时返回空）
	return ""
}

// LoadContractsFromDeployment 从deployment配置加载合约（可选优化）
func (dp *DexParser) LoadContractsFromDeployment(deploymentPath string) error {
	// TODO: 从deployment.json加载合约地址和类型
	// 这样可以动态配置感兴趣的合约，而不是硬编码
	return nil
}

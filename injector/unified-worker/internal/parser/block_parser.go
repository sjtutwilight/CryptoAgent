package parser

import (
	"context"
	"encoding/json"
	"fmt"
)

// BlockParser 区块数据解析器（处理以太坊区块数据）
type BlockParser struct {
	BaseParser
}

// NewBlockParser 创建区块解析器
func NewBlockParser() *BlockParser {
	return &BlockParser{}
}

// CanHandle 判断是否能处理
func (bp *BlockParser) CanHandle(dataSourceID string, taskType string) bool {
	// 处理包含"ethereum"或"block"关键字的数据源
	return contains(dataSourceID, "ethereum") || contains(dataSourceID, "block")
}

// Parse 解析区块数据
func (bp *BlockParser) Parse(ctx context.Context, rawData []byte, config *ParserConfig) (*ParsedData, error) {
	if !bp.CanHandle(config.DataSourceID, config.TaskType) {
		return bp.TryNext(ctx, rawData, config)
	}
	
	// 解析JSON-RPC响应或WebSocket通知
	var data map[string]interface{}
	if err := json.Unmarshal(rawData, &data); err != nil {
		return nil, fmt.Errorf("解析JSON失败: %w", err)
	}
	
	// 提取区块数据（可能在result字段或params.result字段）
	var blockData map[string]interface{}
	
	// 检查是否是JSON-RPC响应
	if result, ok := data["result"].(map[string]interface{}); ok {
		blockData = result
	} else if params, ok := data["params"].(map[string]interface{}); ok {
		// WebSocket订阅通知格式
		if result, ok := params["result"].(map[string]interface{}); ok {
			blockData = result
		}
	} else {
		// 直接是区块数据
		blockData = data
	}
	
	if blockData == nil {
		return nil, fmt.Errorf("无法提取区块数据")
	}
	
	// 提取关键字段
	extracted := make(map[string]interface{})
	if number, ok := blockData["number"]; ok {
		extracted["number"] = number
	}
	if hash, ok := blockData["hash"]; ok {
		extracted["hash"] = hash
	}
	if timestamp, ok := blockData["timestamp"]; ok {
		extracted["timestamp"] = timestamp
	}
	if parentHash, ok := blockData["parentHash"]; ok {
		extracted["parentHash"] = parentHash
	}
	
	return &ParsedData{
		OriginalData:  rawData,
		ExtractedData: extracted,
		Metadata: map[string]interface{}{
			"parser_type": "block",
		},
	}, nil
}

// GetSequence 提取序列号（区块号）
func (bp *BlockParser) GetSequence(parsedData *ParsedData) (interface{}, error) {
	// 尝试从提取的数据中获取number字段
	if number, ok := parsedData.ExtractedData["number"]; ok {
		// 处理十六进制字符串
		if numStr, ok := number.(string); ok {
			return hexToInt64(numStr), nil
		}
		return number, nil
	}
	
	return nil, fmt.Errorf("未找到区块序列号")
}

// helper函数
func contains(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr) >= 0
}

func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func hexToInt64(hexStr string) int64 {
	// 简单的十六进制转换
	if len(hexStr) > 2 && hexStr[:2] == "0x" {
		hexStr = hexStr[2:]
	}
	var result int64
	fmt.Sscanf(hexStr, "%x", &result)
	return result
}

package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// BalanceParser 余额快照解析器
type BalanceParser struct {
	BaseParser
}

// NewBalanceParser 创建余额解析器
func NewBalanceParser() *BalanceParser {
	return &BalanceParser{}
}

// CanHandle 判断是否能处理
func (bp *BalanceParser) CanHandle(dataSourceID string, taskType string) bool {
	// 处理包含"balance"或"snapshot"关键字的数据源
	return strings.Contains(dataSourceID, "balance") || strings.Contains(dataSourceID, "snapshot")
}

// Parse 解析余额快照数据
func (bp *BalanceParser) Parse(ctx context.Context, rawData []byte, config *ParserConfig) (*ParsedData, error) {
	if !bp.CanHandle(config.DataSourceID, config.TaskType) {
		return bp.TryNext(ctx, rawData, config)
	}

	// 解析余额快照JSON
	var data map[string]interface{}
	if err := json.Unmarshal(rawData, &data); err != nil {
		return nil, fmt.Errorf("解析余额数据失败: %w", err)
	}

	// 提取关键字段
	extracted := make(map[string]interface{})

	// 余额快照的关键字段
	fields := []string{
		"account_id", "observed_time", "block_id",
		"asset_type", "biz_id", "amount",
		"price_usd", "value_usd", "label_mask",
	}

	for _, field := range fields {
		if value, ok := data[field]; ok {
			extracted[field] = value
		}
	}

	return &ParsedData{
		OriginalData:  rawData,
		ExtractedData: extracted,
		Metadata: map[string]interface{}{
			"parser_type": "balance",
		},
	}, nil
}

// GetSequence 提取序列号（使用block_id作为序列号）
func (bp *BalanceParser) GetSequence(parsedData *ParsedData) (interface{}, error) {
	// 余额快照使用block_id作为序列号
	if blockID, ok := parsedData.ExtractedData["block_id"]; ok {
		return blockID, nil
	}

	// 如果没有block_id，尝试使用observed_time
	if observedTime, ok := parsedData.ExtractedData["observed_time"]; ok {
		return observedTime, nil
	}

	return nil, fmt.Errorf("未找到余额快照序列号")
}

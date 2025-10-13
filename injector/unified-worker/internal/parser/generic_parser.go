package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// GenericParser 通用解析器（兜底解析器）
type GenericParser struct {
	BaseParser
}

// NewGenericParser 创建通用解析器
func NewGenericParser() *GenericParser {
	return &GenericParser{}
}

// CanHandle 总是返回true（兜底）
func (gp *GenericParser) CanHandle(dataSourceID string, taskType string) bool {
	return true
}

// Parse 通用解析逻辑
func (gp *GenericParser) Parse(ctx context.Context, rawData []byte, config *ParserConfig) (*ParsedData, error) {
	// 尝试解析为JSON
	var data map[string]interface{}
	if err := json.Unmarshal(rawData, &data); err != nil {
		// 如果不是JSON，直接返回原始数据
		return &ParsedData{
			OriginalData: rawData,
			ExtractedData: map[string]interface{}{
				"raw": string(rawData),
			},
			Metadata: map[string]interface{}{
				"parser_type": "generic",
				"format":      "raw",
			},
		}, nil
	}

	// 递归提取所有字段
	extracted := flattenMap(data)

	return &ParsedData{
		OriginalData:  rawData,
		ExtractedData: extracted,
		Metadata: map[string]interface{}{
			"parser_type": "generic",
			"format":      "json",
		},
	}, nil
}

// GetSequence 根据配置的sequence_field提取序列号
func (gp *GenericParser) GetSequence(parsedData *ParsedData) (interface{}, error) {
	// 通用解析器无法自动提取序列号，返回nil
	// 需要上层通过config.SequenceField指定
	return nil, fmt.Errorf("通用解析器需要指定sequence_field")
}

// flattenMap 递归展平map（便于通用查找）
func flattenMap(data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range data {
		// 如果值是map，递归展平
		if nestedMap, ok := value.(map[string]interface{}); ok {
			flattened := flattenMap(nestedMap)
			for k, v := range flattened {
				result[key+"."+k] = v
			}
		} else {
			result[key] = value
		}
	}

	return result
}

// ExtractFieldByPath 通用字段提取（支持嵌套路径）
func ExtractFieldByPath(data map[string]interface{}, path string) (interface{}, error) {
	// 支持点号分隔的路径，如"result.number"
	keys := splitPath(path)

	current := interface{}(data)
	for _, key := range keys {
		if m, ok := current.(map[string]interface{}); ok {
			if val, exists := m[key]; exists {
				current = val
			} else {
				return nil, fmt.Errorf("路径不存在: %s", path)
			}
		} else {
			return nil, fmt.Errorf("无法继续访问路径: %s", path)
		}
	}

	return current, nil
}

func splitPath(path string) []string {
	// 简单的点号分割
	var keys []string
	var current string

	for _, ch := range path {
		if ch == '.' {
			if current != "" {
				keys = append(keys, current)
				current = ""
			}
		} else {
			current += string(ch)
		}
	}

	if current != "" {
		keys = append(keys, current)
	}

	return keys
}

// ConvertToInt64 将interface{}转换为int64
func ConvertToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return parseHexToInt64(v), nil
	default:
		return 0, fmt.Errorf("无法转换为int64: %v (type: %v)", value, reflect.TypeOf(value))
	}
}

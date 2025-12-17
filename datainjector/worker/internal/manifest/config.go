package manifest

import (
	"fmt"
	"strings"
)

// ManifestFieldConfig Manifest 自定义字段配置
type ManifestFieldConfig struct {
	Version           string              `yaml:"version" json:"version"`                       // Manifest 版本
	ChecksumAlgorithm string              `yaml:"checksum_algorithm" json:"checksum_algorithm"` // md5/sha256/none
	CustomFields      []CustomFieldConfig `yaml:"custom_fields" json:"custom_fields"`           // 自定义字段配置
}

// CustomFieldConfig 单个自定义字段配置
type CustomFieldConfig struct {
	Name   string `yaml:"name" json:"name"`     // 字段名
	Source string `yaml:"source" json:"source"` // 数据来源路径，如 "params.address"、"first_record.wallet_address"
}

// ExtractCustomFields 从上下文中提取自定义字段值
// ctx 包含：
//   - "params": 任务参数 map[string]any
//   - "first_record": 第一条记录 map[string]any
//   - "last_record": 最后一条记录 map[string]any
func (c *ManifestFieldConfig) ExtractCustomFields(ctx map[string]any) map[string]any {
	if len(c.CustomFields) == 0 {
		return nil
	}

	result := make(map[string]any)
	for _, field := range c.CustomFields {
		if value := extractValue(ctx, field.Source); value != nil {
			result[field.Name] = value
		}
	}
	return result
}

// extractValue 从嵌套 map 中提取值，支持点分隔路径
// 例如：extractValue(ctx, "params.address") 会查找 ctx["params"].(map)["address"]
func extractValue(data map[string]any, path string) any {
	if data == nil || path == "" {
		return nil
	}

	parts := strings.Split(path, ".")
	current := any(data)

	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current, ok = m[part]
		if !ok {
			return nil
		}
	}

	return current
}

// ParseManifestConfig 从 YAML 配置中解析 Manifest 配置
func ParseManifestConfig(cfg map[string]any) (*ManifestFieldConfig, error) {
	config := &ManifestFieldConfig{
		Version:           "1.0",
		ChecksumAlgorithm: "md5",
	}

	if version, ok := cfg["version"].(string); ok && version != "" {
		config.Version = version
	}

	if algo, ok := cfg["checksum_algorithm"].(string); ok && algo != "" {
		config.ChecksumAlgorithm = strings.ToLower(algo)
	}

	// 解析自定义字段
	if fields, ok := cfg["custom_fields"].([]any); ok {
		for _, f := range fields {
			fieldMap, ok := f.(map[string]any)
			if !ok {
				continue
			}
			name, _ := fieldMap["name"].(string)
			source, _ := fieldMap["source"].(string)
			if name != "" && source != "" {
				config.CustomFields = append(config.CustomFields, CustomFieldConfig{
					Name:   name,
					Source: source,
				})
			}
		}
	}

	return config, nil
}

// ValidateChecksumAlgorithm 验证校验和算法是否支持
func (c *ManifestFieldConfig) ValidateChecksumAlgorithm() error {
	switch c.ChecksumAlgorithm {
	case "md5", "sha256", "none", "":
		return nil
	default:
		return fmt.Errorf("unsupported checksum algorithm: %s", c.ChecksumAlgorithm)
	}
}


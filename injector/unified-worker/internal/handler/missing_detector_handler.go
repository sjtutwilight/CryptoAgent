package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

// MissingDetectorHandler 缺失检测处理器
// 检测数据序列中的缺失并触发补数据
type MissingDetectorHandler struct {
	*BaseHandler
	sequenceField string
	threshold     int                                                         // 缺失阈值
	maxGap        int                                                         // 最大允许gap
	refiller      RefillerHandler                                             // 补数据处理器引用
	lastSequence  int64                                                       // 上次序列号
	onMissing     func(ctx context.Context, start, end int64) ([]byte, error) // 补数据回调
}

// MissingDetectorConfig 缺失检测配置
type MissingDetectorConfig struct {
	SequenceField string `json:"sequence_field"`
	Threshold     int    `json:"threshold"` // 小于此值触发补数据
	MaxGap        int    `json:"max_gap"`   // 超过此值仅告警
}

// NewMissingDetectorHandler 创建缺失检测处理器
func NewMissingDetectorHandler(config MissingDetectorConfig) *MissingDetectorHandler {
	return &MissingDetectorHandler{
		BaseHandler:   NewBaseHandler("MissingDetectorHandler"),
		sequenceField: config.SequenceField,
		threshold:     config.Threshold,
		maxGap:        config.MaxGap,
		lastSequence:  -1,
	}
}

// SetRefiller 设置补数据处理器
func (h *MissingDetectorHandler) SetRefiller(refiller *RefillerHandler) {
	h.refiller = *refiller
}

// Handle 处理数据
func (h *MissingDetectorHandler) Handle(ctx context.Context, data []byte) ([]byte, error) {
	log.Printf("[%s] 开始检测缺失", h.Name())

	// 解析数据提取序列号
	currentSeq, err := h.extractSequence(data)
	if err != nil {
		log.Printf("[%s] 序列号提取失败: %v, 跳过检测", h.Name(), err)
		return h.CallNext(ctx, data)
	}

	// 首次运行，记录序列号
	if h.lastSequence == -1 {
		h.lastSequence = currentSeq
		log.Printf("[%s] 初始化序列号: %d", h.Name(), currentSeq)
		return h.CallNext(ctx, data)
	}

	// 检测缺失
	gap := currentSeq - h.lastSequence - 1
	if gap > 0 {
		log.Printf("[%s] 检测到缺失: 范围[%d, %d], gap=%d", h.Name(), h.lastSequence+1, currentSeq-1, gap)

		if gap > int64(h.maxGap) {
			// 超过最大gap，仅告警
			log.Printf("[%s] ⚠️ 缺失过大(gap=%d > max=%d), 仅记录告警", h.Name(), gap, h.maxGap)
		} else if gap <= int64(h.threshold) && h.refiller.fetcher != nil {
			// 小于阈值，触发补数据
			log.Printf("[%s] 触发补数据: 范围[%d, %d]", h.Name(), h.lastSequence+1, currentSeq-1)

			missingData, err := h.refiller.Refill(ctx, h.lastSequence+1, currentSeq-1)
			if err != nil {
				log.Printf("[%s] 补数据失败: %v", h.Name(), err)
			} else {
				log.Printf("[%s] 补数据成功: %d bytes", h.Name(), len(missingData))
				// 补数据成功后，先处理补的数据
				if _, err := h.CallNext(ctx, missingData); err != nil {
					log.Printf("[%s] 处理补数据失败: %v", h.Name(), err)
				}
			}
		}
	}

	// 更新序列号
	h.lastSequence = currentSeq

	// 传递当前数据给下一个处理器
	return h.CallNext(ctx, data)
}

// extractSequence 提取序列号（支持单条或数组）
func (h *MissingDetectorHandler) extractSequence(data []byte) (int64, error) {
	var parsed interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return 0, fmt.Errorf("JSON解析失败: %w", err)
	}

	switch v := parsed.(type) {
	case map[string]interface{}:
		if seq, exists := v[h.sequenceField]; exists {
			return toInt64(seq)
		}
		return 0, fmt.Errorf("字段 %s 不存在", h.sequenceField)

	case []interface{}:
		if len(v) == 0 {
			return 0, fmt.Errorf("空数组")
		}
		if first, ok := v[0].(map[string]interface{}); ok {
			if seq, exists := first[h.sequenceField]; exists {
				return toInt64(seq)
			}
		}
		return 0, fmt.Errorf("首条数据无字段 %s", h.sequenceField)

	default:
		return 0, fmt.Errorf("不支持的数据类型: %T", parsed)
	}
}

// toInt64 转换为int64
func toInt64(v interface{}) (int64, error) {
	switch val := v.(type) {
	case float64:
		return int64(val), nil
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("无法转换为int64: %T", v)
	}
}

package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

// SequenceHandler 序列号提取处理器
// 从数据中提取序列号字段（如timestamp、block_number）
type SequenceHandler struct {
	*BaseHandler
	sequenceField string
	lastSequence  interface{}
}

// NewSequenceHandler 创建序列号处理器
func NewSequenceHandler(sequenceField string) *SequenceHandler {
	return &SequenceHandler{
		BaseHandler:   NewBaseHandler(fmt.Sprintf("SequenceHandler[%s]", sequenceField)),
		sequenceField: sequenceField,
	}
}

// Handle 处理数据
func (h *SequenceHandler) Handle(ctx context.Context, data []byte) ([]byte, error) {
	log.Printf("[%s] 开始提取序列号", h.Name())

	// 解析JSON
	var parsed interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	// 提取序列号
	sequence, err := h.extractSequence(parsed)
	if err != nil {
		log.Printf("[%s] 序列号提取失败: %v, 跳过检测", h.Name(), err)
		// 序列号提取失败不阻塞流程，继续传递
		return h.CallNext(ctx, data)
	}

	log.Printf("[%s] 当前序列号: %v, 上次: %v", h.Name(), sequence, h.lastSequence)
	h.lastSequence = sequence

	// 传递给下一个处理器
	return h.CallNext(ctx, data)
}

// extractSequence 从数据中提取序列号
func (h *SequenceHandler) extractSequence(data interface{}) (interface{}, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		// 单条数据
		if seq, exists := v[h.sequenceField]; exists {
			return seq, nil
		}
		return nil, fmt.Errorf("字段 %s 不存在", h.sequenceField)

	case []interface{}:
		// 数组数据，取第一条的序列号
		if len(v) == 0 {
			return nil, fmt.Errorf("空数组")
		}
		if first, ok := v[0].(map[string]interface{}); ok {
			if seq, exists := first[h.sequenceField]; exists {
				return seq, nil
			}
		}
		return nil, fmt.Errorf("首条数据无字段 %s", h.sequenceField)

	default:
		return nil, fmt.Errorf("不支持的数据类型: %T", data)
	}
}

// GetLastSequence 获取最后的序列号
func (h *SequenceHandler) GetLastSequence() interface{} {
	return h.lastSequence
}

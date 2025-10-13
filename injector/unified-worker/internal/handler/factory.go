package handler

import (
	"fmt"
	"log"

	"unified-worker/internal/parser"
)

// HandlerConfig 处理器配置
type HandlerConfig struct {
	Type   string                 `json:"type"`   // "parser", "sequence", "missing_detector", "refiller", "kafka_sink"
	Name   string                 `json:"name"`   // Parser名称
	Config map[string]interface{} `json:"config"` // 特定配置
}

// HandlerFactory 处理器工厂
type HandlerFactory struct {
	parserFactory *parser.ParserFactory
}

// NewHandlerFactory 创建处理器工厂
func NewHandlerFactory() *HandlerFactory {
	return &HandlerFactory{
		parserFactory: parser.NewParserFactory(),
	}
}

// BuildChain 根据配置构建处理链
func (f *HandlerFactory) BuildChain(configs []HandlerConfig) (Handler, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("处理器配置为空")
	}

	var handlers []Handler
	var refiller *RefillerHandler

	// 第一遍：创建所有处理器
	for i, config := range configs {
		handler, err := f.createHandler(config)
		if err != nil {
			return nil, fmt.Errorf("创建处理器[%d]失败: %w", i, err)
		}

		handlers = append(handlers, handler)

		// 记录Refiller引用
		if config.Type == "refiller" {
			if r, ok := handler.(*RefillerHandler); ok {
				refiller = r
			}
		}
	}

	// 第二遍：构建责任链
	for i := 0; i < len(handlers)-1; i++ {
		handlers[i].SetNext(handlers[i+1])
	}

	// 第三遍：关联MissingDetector和Refiller
	if refiller != nil {
		for _, h := range handlers {
			if md, ok := h.(*MissingDetectorHandler); ok {
				md.SetRefiller(refiller)
				log.Printf("[HandlerFactory] 关联MissingDetector和Refiller")
			}
		}
	}

	log.Printf("[HandlerFactory] 构建处理链完成: %d个处理器", len(handlers))
	for i, h := range handlers {
		log.Printf("  [%d] %s", i, h.Name())
	}

	return handlers[0], nil
}

// createHandler 创建单个处理器
func (f *HandlerFactory) createHandler(config HandlerConfig) (Handler, error) {
	switch config.Type {
	case "parser":
		return f.createParserHandler(config)

	case "sequence":
		return f.createSequenceHandler(config)

	case "missing_detector":
		return f.createMissingDetectorHandler(config)

	case "refiller":
		return f.createRefillerHandler(config)

	case "kafka_sink":
		return f.createKafkaSinkHandler(config)

	default:
		return nil, fmt.Errorf("未知处理器类型: %s", config.Type)
	}
}

// createParserHandler 创建Parser处理器
func (f *HandlerFactory) createParserHandler(config HandlerConfig) (Handler, error) {
	if config.Name == "" {
		return nil, fmt.Errorf("Parser名称为空")
	}

	parserInstance, err := f.parserFactory.Create(config.Name)
	if err != nil {
		return nil, fmt.Errorf("创建Parser失败: %w", err)
	}

	return NewParserHandler(config.Name, parserInstance), nil
}

// createSequenceHandler 创建序列号处理器
func (f *HandlerFactory) createSequenceHandler(config HandlerConfig) (Handler, error) {
	field, ok := config.Config["field"].(string)
	if !ok {
		return nil, fmt.Errorf("sequence handler需要field配置")
	}

	return NewSequenceHandler(field), nil
}

// createMissingDetectorHandler 创建缺失检测处理器
func (f *HandlerFactory) createMissingDetectorHandler(config HandlerConfig) (Handler, error) {
	mdConfig := MissingDetectorConfig{
		SequenceField: getStringOrDefault(config.Config, "sequence_field", "timestamp"),
		Threshold:     getIntOrDefault(config.Config, "threshold", 5),
		MaxGap:        getIntOrDefault(config.Config, "max_gap", 100),
	}

	return NewMissingDetectorHandler(mdConfig), nil
}

// createRefillerHandler 创建补数据处理器
func (f *HandlerFactory) createRefillerHandler(config HandlerConfig) (Handler, error) {
	rConfig := RefillerConfig{
		Method: getStringOrDefault(config.Config, "method", "websocket"),
		MaxGap: getIntOrDefault(config.Config, "max_gap", 100),
	}

	return NewRefillerHandler(rConfig), nil
}

// createKafkaSinkHandler 创建Kafka输出处理器
func (f *HandlerFactory) createKafkaSinkHandler(config HandlerConfig) (Handler, error) {
	topic, ok := config.Config["topic"].(string)
	if !ok {
		return nil, fmt.Errorf("kafka_sink需要topic配置")
	}

	brokers := []string{"localhost:9092"} // 默认broker
	if b, ok := config.Config["brokers"].([]interface{}); ok {
		brokers = make([]string, len(b))
		for i, v := range b {
			brokers[i] = v.(string)
		}
	}

	sinkConfig := KafkaSinkConfig{
		Topic:   topic,
		Brokers: brokers,
	}

	return NewKafkaSinkHandler(sinkConfig)
}

// 辅助函数
func getStringOrDefault(m map[string]interface{}, key, defaultValue string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return defaultValue
}

func getIntOrDefault(m map[string]interface{}, key string, defaultValue int) int {
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	if v, ok := m[key].(int); ok {
		return v
	}
	return defaultValue
}

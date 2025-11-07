package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

type KafkaConfig struct {
	Brokers    []string
	Topic      string
	KeyFrom    []string
	Linger     time.Duration
	TopicField string
	TopicMap   map[string]string
}

type Kafka struct {
	defaultWriter *kafka.Writer
	brokers       []string
	defaultTopic  string
	keyFrom       []string
	linger        time.Duration
	topicField    string
	topicMap      map[string]string

	mu      sync.Mutex
	writers map[string]*kafka.Writer
}

func init() {
	Register("kafka", func(cfg map[string]any) (Sink, error) {
		kc, err := parseKafkaConfig(cfg)
		if err != nil {
			return nil, err
		}
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers:      kc.Brokers,
			Topic:        kc.Topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: kc.Linger,
		})
		return &Kafka{
			defaultWriter: writer,
			brokers:       kc.Brokers,
			defaultTopic:  kc.Topic,
			keyFrom:       kc.KeyFrom,
			linger:        kc.Linger,
			topicField:    kc.TopicField,
			topicMap:      kc.TopicMap,
			writers:       make(map[string]*kafka.Writer),
		}, nil
	})
}

func parseKafkaConfig(cfg map[string]any) (*KafkaConfig, error) {
	kc := &KafkaConfig{}
	switch brokers := cfg["brokers"].(type) {
	case []any:
		for _, b := range brokers {
			if s, ok := b.(string); ok {
				kc.Brokers = append(kc.Brokers, s)
			}
		}
	case []string:
		kc.Brokers = append(kc.Brokers, brokers...)
	case string:
		if brokers != "" {
			kc.Brokers = append(kc.Brokers, brokers)
		}
	}
	if len(kc.Brokers) == 0 {
		return nil, fmt.Errorf("kafka sink: brokers required")
	}
	if topic, ok := cfg["topic"].(string); ok {
		kc.Topic = topic
	}
	if kc.Topic == "" {
		return nil, fmt.Errorf("kafka sink: topic required")
	}
	switch key := cfg["key_from"].(type) {
	case []any:
		for _, v := range key {
			if s, ok := v.(string); ok {
				kc.KeyFrom = append(kc.KeyFrom, s)
			}
		}
	case []string:
		kc.KeyFrom = append(kc.KeyFrom, key...)
	case string:
		if key != "" {
			kc.KeyFrom = append(kc.KeyFrom, key)
		}
	}
	kc.Linger = 50 * time.Millisecond
	if linger, ok := cfg["linger_ms"]; ok {
		switch v := linger.(type) {
		case int:
			if v > 0 {
				kc.Linger = time.Duration(v) * time.Millisecond
			}
		case int64:
			if v > 0 {
				kc.Linger = time.Duration(v) * time.Millisecond
			}
		case float64:
			if v > 0 {
				kc.Linger = time.Duration(v) * time.Millisecond
			}
		}
	}
	if topicField, ok := cfg["topic_field"].(string); ok {
		if tf := strings.TrimSpace(topicField); tf != "" {
			kc.TopicField = tf
		}
	}
	if topicMap, ok := cfg["topic_map"].(map[string]any); ok {
		kc.TopicMap = make(map[string]string, len(topicMap))
		for k, v := range topicMap {
			if s, ok := v.(string); ok {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					kc.TopicMap[k] = trimmed
				}
			}
		}
	}
	return kc, nil
}

func (k *Kafka) Write(msg *types.Message) error {
	writer, _, err := k.writerForMessage(msg)
	if err != nil {
		return err
	}
	key := k.buildKey(msg)
	return writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(key),
		Value: msg.Payload,
	})
}

func (k *Kafka) Close() error {
	var err error
	if k.defaultWriter != nil {
		if cerr := k.defaultWriter.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	for topic, writer := range k.writers {
		if writer == nil {
			continue
		}
		if cerr := writer.Close(); cerr != nil && err == nil {
			err = cerr
		}
		delete(k.writers, topic)
	}
	return err
}

func (k *Kafka) buildKey(msg *types.Message) string {
	if len(k.keyFrom) == 0 {
		return ""
	}
	parts := make([]string, 0, len(k.keyFrom))
	for _, field := range k.keyFrom {
		if msg.Metadata != nil {
			if v, ok := msg.Metadata[field]; ok {
				switch vv := v.(type) {
				case string:
					parts = append(parts, vv)
				case fmt.Stringer:
					parts = append(parts, vv.String())
				default:
					b, _ := json.Marshal(vv)
					parts = append(parts, string(b))
				}
				continue
			}
		}
		parts = append(parts, "")
	}
	return strings.Join(parts, "|")
}

func (k *Kafka) writerForMessage(msg *types.Message) (*kafka.Writer, string, error) {
	if k.defaultWriter == nil {
		return nil, "", fmt.Errorf("kafka sink not initialized")
	}
	topic := k.defaultTopic
	if k.topicField != "" && msg != nil && msg.Metadata != nil {
		if val, ok := msg.Metadata[k.topicField]; ok {
			str := strings.TrimSpace(util.ToString(val))
			if mapped, ok := k.topicMap[str]; ok && strings.TrimSpace(mapped) != "" {
				str = strings.TrimSpace(mapped)
			}
			if str != "" {
				topic = str
			}
		}
	}
	if topic == "" {
		return nil, "", fmt.Errorf("kafka sink: topic not resolved")
	}
	if topic == k.defaultTopic {
		return k.defaultWriter, topic, nil
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	if writer, ok := k.writers[topic]; ok {
		return writer, topic, nil
	}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      k.brokers,
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: k.linger,
	})
	k.writers[topic] = writer
	return writer, topic, nil
}

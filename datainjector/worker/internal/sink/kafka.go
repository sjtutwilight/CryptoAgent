package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type KafkaConfig struct {
	Brokers []string
	Topic   string
	KeyFrom []string
	Linger  time.Duration
}

type Kafka struct {
	writer  *kafka.Writer
	keyFrom []string
}

func init() {
	Register("kafka", func(cfg map[string]any) (Sink, error) {
		kc, err := parseKafkaConfig(cfg)
		if err != nil {
			return nil, err
		}
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers:      kc.Brokers,
			Topic:        kc.Topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: kc.Linger,
		})
		return &Kafka{writer: w, keyFrom: kc.KeyFrom}, nil
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
	return kc, nil
}

func (k *Kafka) Write(msg *types.Message) error {
	if k.writer == nil {
		return fmt.Errorf("kafka sink not initialized")
	}
	key := k.buildKey(msg)
	return k.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(key),
		Value: msg.Payload,
	})
}

func (k *Kafka) Close() error {
	if k.writer != nil {
		return k.writer.Close()
	}
	return nil
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

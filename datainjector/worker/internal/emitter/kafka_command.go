package emitter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

type KafkaCommandConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
}

type KafkaCommand struct {
	reader *kafka.Reader
}

func NewKafkaCommand(cfg KafkaCommandConfig) (*KafkaCommand, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka_command: brokers required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka_command: topic required")
	}
	if cfg.GroupID == "" {
		cfg.GroupID = fmt.Sprintf("worker-%s", cfg.Topic)
	}
	if cfg.MinBytes <= 0 {
		cfg.MinBytes = 1
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10 << 20 // 10MB
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: cfg.MinBytes,
		MaxBytes: cfg.MaxBytes,
	})

	return &KafkaCommand{reader: reader}, nil
}

func (k *KafkaCommand) Start(ctx context.Context, fire func(args map[string]any)) error {
	defer k.reader.Close()

	for {
		m, err := k.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka_command: read message failed: %w", err)
		}

		log.Printf("[KafkaCommand] 收到消息: topic=%s partition=%d offset=%d size=%d",
			m.Topic, m.Partition, m.Offset, len(m.Value))

		var payload map[string]any
		if err := json.Unmarshal(m.Value, &payload); err != nil {
			log.Printf("[KafkaCommand] invalid message, offset=%d error=%v, raw=%s", m.Offset, err, string(m.Value))
			_ = k.reader.CommitMessages(ctx, m)
			continue
		}

		log.Printf("[KafkaCommand] 解析成功，payload keys: %v", getKeys(payload))
		fire(payload)
		log.Printf("[KafkaCommand] fire() 调用完成")

		if err := k.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("[KafkaCommand] commit failed offset=%d err=%v", m.Offset, err)
		}
	}
}

func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

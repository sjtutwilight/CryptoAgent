package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	message := map[string]any{
		"taskId":   "dune-chainlink-" + fmt.Sprintf("%d", time.Now().Unix()),
		"taskType": "batch_file",
		"payload": map[string]any{
			"task_id":  "dune-chainlink-" + fmt.Sprintf("%d", time.Now().Unix()),
			"chain_id": "1",
			"address":  "0x514910771af9ca656af840dff83e8264ecf986ca",
		},
		"metadata": map[string]any{
			"datasourceId": "DuneSim",
			"test":         true,
		},
	}

	data, err := json.Marshal(message)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "batch.tasks",
	})
	defer writer.Close()

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: data,
	})
	if err != nil {
		log.Fatalf("write: %v", err)
	}

	fmt.Printf("✓ 任务已发送到 Kafka\n")
	fmt.Printf("TaskID: %s\n", message["taskId"])
	fmt.Printf("消息内容: %s\n", string(data))
}


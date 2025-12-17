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
	taskID := "dune-chainlink-" + fmt.Sprintf("%d", time.Now().Unix())
	
	// Kafka 消息格式：将 payload 的内容展开到顶层，供 caller 直接使用
	message := map[string]any{
		"task_id":  taskID,
		"chain_id": "1",
		"address":  "0x514910771af9ca656af840dff83e8264ecf986ca",
		// 元数据字段
		"taskId":   taskID,
		"taskType": "batch_file",
		"datasourceId": "DuneSim",
		"test":     true,
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
	fmt.Printf("TaskID: %s\n", taskID)
	fmt.Printf("消息内容: %s\n", string(data))
}


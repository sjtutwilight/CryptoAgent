package main

import (
	"encoding/json"
	"fmt"
	"http-worker/internal/model"
	"os"
)

func main() {
	// 创建测试任务
	tasks := []*model.HttpTask{
		{
			TaskID: "kafka-test-001",
			Payload: model.TaskPayload{
				DataSourceURL: "http://localhost:8080",
				Method:        "POST",
				Params: map[string]interface{}{
					"method": "eth_getBlockByNumber",
					"params": []interface{}{"latest", false},
					"id":     1,
				},
				DataSourceID: "mock-ethereum",
			},
		},
		{
			TaskID: "kafka-test-002",
			Payload: model.TaskPayload{
				DataSourceURL: "http://localhost:8080",
				Method:        "POST", 
				Params: map[string]interface{}{
					"method": "eth_blockNumber",
					"params": []interface{}{},
					"id":     2,
				},
				DataSourceID: "mock-ethereum",
			},
		},
		{
			TaskID: "kafka-test-003",
			Payload: model.TaskPayload{
				DataSourceURL: "http://localhost:8080/health",
				Method:        "GET",
				DataSourceID:  "mock-ethereum",
			},
		},
	}
	
	fmt.Println("🔧 生成Kafka测试任务")
	fmt.Println("====================")
	
	// 生成任务JSON
	for i, task := range tasks {
		taskJSON, err := json.MarshalIndent(task, "", "  ")
		if err != nil {
			fmt.Printf("序列化任务失败: %v\n", err)
			continue
		}
		
		fmt.Printf("\n📋 任务 %d - %s:\n", i+1, task.TaskID)
		fmt.Printf("%s\n", string(taskJSON))
		
		// 保存到文件
		filename := fmt.Sprintf("kafka_task_%d.json", i+1)
		if err := os.WriteFile(filename, taskJSON, 0644); err != nil {
			fmt.Printf("保存任务文件失败: %v\n", err)
		} else {
			fmt.Printf("✅ 任务已保存到: %s\n", filename)
		}
	}
	
	fmt.Println("\n📖 使用说明:")
	fmt.Println("1. 启动Kafka服务")
	fmt.Println("2. 创建topic: kafka-topics --create --topic http.tasks --bootstrap-server localhost:9092")
	fmt.Println("3. 发送任务到Kafka:")
	fmt.Println("   cat kafka_task_1.json | kafka-console-producer --topic http.tasks --bootstrap-server localhost:9092")
	fmt.Println("4. 启动HTTP Worker:")
	fmt.Println("   ./http-worker")
	fmt.Println("5. 观察输出topic:")
	fmt.Println("   kafka-console-consumer --topic data.mock-ethereum --bootstrap-server localhost:9092")
	fmt.Println("   kafka-console-consumer --topic tasks.status --bootstrap-server localhost:9092")
}
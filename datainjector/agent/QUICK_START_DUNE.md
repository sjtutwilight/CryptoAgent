# Dune Token Holders 快速开始指南

## 5 分钟快速测试

### 1. 设置环境变量

```bash
export DUNE_SIM_API_KEY="sim_ZmoRtMDsmW0WWNeTUpFr2hjU8pIHEaAY"
```

### 2. 启动基础设施

```bash
cd DataPlatform
docker-compose up -d kafka postgresql redis
```

### 3. 运行集成测试

```bash
cd DataPlatform
./scripts/data.sh datainjector:test:dune
```

### 4. 查看结果

```bash
# 查看输出目录
ls -lh runtime/data/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/

# 查看 Manifest
cat runtime/data/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/manifest.json | jq '.'

# 查看数据样例
head -3 runtime/data/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/holders_000.json | jq '.'
```

## 手动运行

### 启动 Worker

```bash
cd datainjector/worker
export DUNE_SIM_API_KEY="sim_ZmoRtMDsmW0WWNeTUpFr2hjU8pIHEaAY"
go run ./cmd/worker --config ./configs/dune_token_holders.yaml
```

### 发送任务

```bash
# 创建任务消息
cat > /tmp/task.json <<EOF
{
  "taskId": "test-chainlink-$(date +%s)",
  "taskType": "batch_file",
  "payload": {
    "task_id": "test-chainlink-$(date +%s)",
    "chain_id": "1",
    "address": "0x514910771af9ca656af840dff83e8264ecf986ca"
  },
  "metadata": {
    "datasourceId": "DuneSim"
  }
}
EOF

# 发送到 Kafka
cat /tmp/task.json | docker exec -i $(docker ps -qf "name=kafka") \
    kafka-console-producer.sh \
    --broker-list localhost:9092 \
    --topic batch.tasks
```

### 监控执行

```bash
# 实时查看游标状态
watch -n 1 'cat runtime/data/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/.cursor.json 2>/dev/null | jq .'

# 查看 Worker 日志
tail -f /tmp/dune_worker.log
```

## 常见问题

### Q: 如何修改输出格式？

修改配置文件中的 `output_format`:
```yaml
output_format: "csv"  # json/csv/parquet
```

### Q: 如何调整限流？

修改配置文件中的 `rate_limit`:
```yaml
rate_limit:
  capacity: 5      # 令牌桶容量
  refill_rate: 1   # 每秒补充速率
```

### Q: 如何接入其他 Token？

修改任务参数：
```json
{
  "chain_id": "1",
  "address": "0xdac17f958d2ee523a2206206994597c13d831ec7"  // USDT
}
```

### Q: 如何实现断点续传？

重新发送相同的 `task_id`，Worker 会自动从 `.cursor.json` 恢复。

## 下一步

- 查看完整文档：`DUNE_INTEGRATION.md`
- 查看实现总结：`IMPLEMENTATION_SUMMARY.md`
- 查看架构设计：`worker/DESIGN.md`

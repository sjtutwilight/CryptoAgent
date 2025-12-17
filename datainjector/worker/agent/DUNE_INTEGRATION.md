# Dune Token Holders 数据接入集成指南

## 概述

本文档说明如何使用改造后的数据接入层接入 Dune Sim API 的 Token Holders 数据。

## 架构特点

1. **批量文件 Caller**：分页拉取 REST API 数据，直接写入本地文件
2. **断点续传**：通过本地游标文件 `.cursor.json` 支持任务中断后继续
3. **多格式输出**：支持 JSON Lines、CSV、Parquet 格式
4. **Manifest 完整性校验**：生成 `manifest.json` 用于数据完整性验证

## 快速开始

### 1. 环境准备

```bash
# 设置 API Key（从环境变量读取）
export DUNE_SIM_API_KEY="sim_ZmoRtMDsmW0WWNeTUpFr2hjU8pIHEaAY"

# 确保 Kafka 已启动
docker-compose up -d kafka
```

### 2. 运行集成测试

```bash
cd datainjector/worker
./test_dune_integration.sh
```

测试脚本会：
1. 检查 Kafka 服务
2. 启动 Worker
3. 发送测试任务（Chainlink Token）
4. 监控任务执行
5. 验证输出结果

### 3. 手动运行 Worker

```bash
# 使用 Dune 配置启动 Worker
export DUNE_SIM_API_KEY="your_api_key"
go run ./cmd/worker --config ./configs/dune_token_holders.yaml
```

### 4. 发送批量拉取任务

通过 Kafka 发送任务消息到 `batch.tasks` topic：

```json
{
  "taskId": "task-chainlink-001",
  "taskType": "batch_file",
  "payload": {
    "task_id": "task-chainlink-001",
    "chain_id": "1",
    "address": "0x514910771af9ca656af840dff83e8264ecf986ca"
  },
  "metadata": {
    "datasourceId": "DuneSim"
  }
}
```

## 配置说明

### Worker 配置 (`configs/dune_token_holders.yaml`)

```yaml
caller: "batch_file"
caller_config:
  # API 配置
  endpoint: "https://api.sim.dune.com"
  path_template: "/v1/evm/token-holders/{chain_id}/{address}"
  headers:
    X-Sim-Api-Key: "${DUNE_SIM_API_KEY}"
  
  # 分页配置
  page_size: 500                # 每页记录数
  cursor_param: "offset"        # 游标参数名
  cursor_field: "next_offset"   # 响应中的游标字段
  data_field: "holders"         # 响应中的数据数组字段
  
  # 输出配置
  output_dir: "/tmp/dune/token-holders/{chain_id}/{address}"
  output_format: "json"         # json/csv/parquet
  filename_prefix: "holders"
  max_records_per_file: 10000   # 单文件最大记录数
  
  # Manifest 配置
  manifest:
    version: "1.0"
    checksum_algorithm: "md5"   # md5/sha256/none
    custom_fields:
      - name: "token_address"
        source: "params.address"
      - name: "chain_id"
        source: "params.chain_id"
```

### 控制面配置 (`application.yml`)

```yaml
datasources:
  configs:
    DuneSim:
      dataSourceId: "DuneSim"
      rateLimitWeight: 100      # 每60秒100次请求
      rateLimitInterval: 60
      maxRetryCount: 3
      enabled: true
```

## 输出文件结构

```
/tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/
├── holders_000.json          # 数据文件（JSON Lines 格式）
├── holders_001.json
├── holders_002.json
├── .cursor.json              # 游标文件（任务完成后自动删除）
└── manifest.json             # 完整性清单
```

### manifest.json 示例

```json
{
  "version": "1.0",
  "task_id": "task-chainlink-001",
  "data_source": "batch_file",
  "created_at": "2025-12-16T10:00:00Z",
  "completed_at": "2025-12-16T10:05:30Z",
  "status": "completed",
  "total_records": 25000,
  "total_files": 3,
  "files": [
    {
      "filename": "holders_000.json",
      "record_count": 10000,
      "size_bytes": 2048576,
      "checksum": "d41d8cd98f00b204e9800998ecf8427e"
    },
    {
      "filename": "holders_001.json",
      "record_count": 10000,
      "size_bytes": 2048576,
      "checksum": "098f6bcd4621d373cade4e832627b4f6"
    },
    {
      "filename": "holders_002.json",
      "record_count": 5000,
      "size_bytes": 1024288,
      "checksum": "5d41402abc4b2a76b9719d911017c592"
    }
  ],
  "custom_fields": {
    "token_address": "0x514910771af9ca656af840dff83e8264ecf986ca",
    "chain_id": "1",
    "first_holder_address": "0x...",
    "first_holder_balance": "13794442047246482254818",
    "last_holder_address": "0x...",
    "last_holder_balance": "100000000000000000"
  }
}
```

## 断点续传

如果任务执行过程中中断（Worker 崩溃、网络故障等），重新发送相同的任务消息即可从断点继续：

1. Worker 启动时检查 `.cursor.json` 文件
2. 如果存在且 `task_id` 匹配，则从 `next_offset` 继续拉取
3. 继续写入当前文件，直到达到 `max_records_per_file`
4. 任务完成后删除游标文件

### .cursor.json 示例

```json
{
  "task_id": "task-chainlink-001",
  "next_offset": "eyJwYWdlIjoyLCJsaW1pdCI6NTAwfQ==",
  "current_file_index": 1,
  "records_in_current_file": 3500,
  "total_records": 13500,
  "files_written": ["holders_000.json"],
  "last_updated": "2025-12-16T10:03:00Z"
}
```

## Manifest 校验

控制面的 `ManifestValidatorService` 提供以下校验：

1. **基础校验**：status == "completed"
2. **文件校验**：所有文件存在且 checksum 匹配（需要共享存储）
3. **记录数校验**：sum(file.record_count) == total_records
4. **自定义校验**：根据配置执行额外检查

## 测试用例

### 以太坊 Chainlink Token (LINK)

- **Chain ID**: 1
- **Address**: `0x514910771af9ca656af840dff83e8264ecf986ca`
- **API Key**: `sim_ZmoRtMDsmW0WWNeTUpFr2hjU8pIHEaAY`

```bash
# 运行集成测试
./test_dune_integration.sh

# 查看输出
ls -lh /tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/

# 查看 manifest
cat /tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/manifest.json | jq '.'

# 查看前几条记录
head -3 /tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/holders_000.json | jq '.'
```

## 扩展到其他 Token

只需修改任务参数：

```json
{
  "taskId": "task-usdt-001",
  "taskType": "batch_file",
  "payload": {
    "task_id": "task-usdt-001",
    "chain_id": "1",
    "address": "0xdac17f958d2ee523a2206206994597c13d831ec7"  // USDT
  }
}
```

## 故障排查

### 1. Worker 无法启动

```bash
# 检查配置文件
cat configs/dune_token_holders.yaml

# 检查环境变量
echo $DUNE_SIM_API_KEY

# 查看日志
tail -f /tmp/dune_worker.log
```

### 2. 任务未执行

```bash
# 检查 Kafka 是否收到消息
docker exec -it $(docker ps -qf "name=kafka") \
    kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic batch.tasks \
    --from-beginning

# 检查 Worker 是否订阅了 topic
docker exec -it $(docker ps -qf "name=kafka") \
    kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group worker.dune.batch
```

### 3. 限流错误

如果遇到 429 错误，调整限流配置：

```yaml
rate_limit:
  capacity: 5      # 降低容量
  refill_rate: 1   # 降低补充速率
```

## 性能优化

1. **并行拉取**：启动多个 Worker 实例，使用不同的 `group_id`
2. **增大页面大小**：`page_size: 500`（API 最大值）
3. **增大文件大小**：`max_records_per_file: 50000`
4. **使用 Parquet 格式**：`output_format: "parquet"`（更高压缩率）

## 监控指标

Worker 暴露 Prometheus 指标（端口 9100）：

```bash
# 查看指标
curl http://localhost:9100/metrics

# 关键指标
# - batch_file_records_total: 累计拉取记录数
# - batch_file_files_total: 累计生成文件数
# - batch_file_duration_seconds: 任务执行时长
```

## 下一步

1. 接入更多 Dune API（如 Token Info、Activity 等）
2. 实现 Parquet 格式写入器
3. 支持增量更新（基于时间戳）
4. 集成到数据处理流水线（Flink/Spark）


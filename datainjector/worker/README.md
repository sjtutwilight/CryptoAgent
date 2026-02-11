## 模块角色（一句话）
Worker 是 DataInjector 的数据接入执行器，按配置拉取数据并进入处理链/下沉。

## 目录 & 关键文件索引
cmd/worker/main.go - 入口与服务启动
internal/role/role.go - Role 构建与执行链
internal/role/manager.go - Role 运行时管理与 Apply
internal/config/config.go - 配置解析/模板注入/校验
internal/caller/ - 数据源调用
internal/handler/ - 解析与业务处理
internal/sink/ - Kafka/File/Console 下沉
internal/api/server.go - 控制面 API
LOGGING_SPEC.md - Worker 结构化日志规范（事件字典/字段/触发点）

## 主要逻辑
Emitter -> Caller -> (Queue) -> Handler Chain -> Sink
Role 负责 glue；Caller/Handler/Sink 为可插拔组件。

## 对外接口 / CLI / 配置项
控制面: /api/roles, /api/roles/apply, /api/roles/stop, /api/roles/validate

## 关键约束 & 不变量（这块我来维护）

## 录制数据转 ODS
将 websocket 录制目录（`recording_run*`）转换为 `python_experiment/data/ods` 目录规范：

```bash
python3 DataPlatform/datainjector/worker/tools/recording_to_ods.py \
  --input-dir DataPlatform/runtime/data/recording \
  --output-root python_experiment/data/ods \
  --datasource-id binance.usdm.ws
```

默认会生成：
- `response_0000.json`（分片响应文件）
- `metadata.json`（ODS 语义元数据）
- `manifest.json`（转换运行信息）
- 并自动追加到 `_catalog/ods_dataset_registry.jsonl`

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

## AAVE 微观结构默认路径
- AAVE orderbook 默认链路为 `diff + snapshot` 双 topic：`*.orderbook.diff`、`*.orderbook.snapshot`。
- AAVE aggtrades 保持独立 topic（`*.aggtrades`）不回退。
- 角色工件: `configs/aave/roles_aave_full_stable.json`
- 回滚工件: `configs/aave/roles_aave_full_stable_file_rollback.json`
- 发布与回滚说明: `doc/aave_microstructure_kafka_rollout.md`

## 关键约束 & 不变量（这块我来维护）

## 录制数据转 ODS
将 websocket 录制目录（`recording_run*`）转换为 `crypto_research_lab/data/ods` 目录规范：

```bash
python3 DataPlatform/datainjector/worker/tools/recording_to_ods.py \
  --input-dir DataPlatform/runtime/data/recording \
  --output-root crypto_research_lab/data/ods \
  --datasource-id binance.usdm.ws
```

默认会生成：
- `response_0000.json`（分片响应文件）
- `metadata.json`（ODS 语义元数据）
- `manifest.json`（转换运行信息）
- 并自动追加到 `_catalog/ods_dataset_registry.jsonl`

## 架构质量门禁（golangci-lint + go-arch-lint）

### 1) 安装工具（本地）

```bash
cd datainjector/worker
source tools/quality/tool_versions.env

go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v${GOLANGCI_LINT_MIN_VERSION}
go install github.com/fe3dback/go-arch-lint@v${GO_ARCH_LINT_MIN_VERSION}
```

### 2) 首次生成历史基线

```bash
cd datainjector/worker
./tools/quality_gate.sh --mode update-baseline
```

说明：
- 基线文件路径：`tools/quality/baseline.json`
- 运行产物目录：`runtime/quality_gate/`

### 3) 日常检查（仅阻断新增违规）

```bash
cd datainjector/worker
./tools/quality_gate.sh --mode check
```

检查策略：
- 仅命中基线问题：通过
- 出现基线外新问题：失败（exit code=1）
- 工具执行异常：失败（exit code=2）

### 4) 最小回归验证

```bash
cd datainjector/worker
./tools/quality_gate_regression.sh
```

覆盖场景：
- 成功路径（无新增违规）
- lint 新违规路径
- 架构新违规路径

### 5) CI 接入

仓库已提供 GitHub Actions 工作流：
- `.github/workflows/worker-quality-gate.yml`

CI 会执行：
1. 安装固定版本工具
2. 运行 `quality_gate_regression.sh`
3. 运行 `quality_gate.sh --mode check`
4. 上传 `runtime/quality_gate/` 报告工件

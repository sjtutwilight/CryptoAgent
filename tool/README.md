# Tool Entrypoints

统一入口脚本，提供 infra/编排/数据初始化/测试/观测能力。

## 设计原则

**入口脚本职责：**
- 提供统一的命令行界面
- 参数解析与验证
- 调用具体实现模块（不实现具体功能）

**具体实现位置：**
- 测试逻辑 → `automation/test/probes/` 和 `automation/test/scenarios/`
- 数据处理 → `process/batch/spark/jobs/`
- 编排逻辑 → `automation/orchestration/`

## Scripts

- `tool/orchestration.sh` - 编排与开发场景入口（调用 docker-compose、服务启停脚本）
- `tool/ops.sh`           - 数据初始化与操作脚本统一入口
- `tool/test.sh`          - 测试入口（调用 automation/test 下的 Scenario）
- `tool/git-branch.sh`    - Git 分支工作流入口（建分支/同步主干/合并主干）

Ops spec: `tool/OPS.md`

## MCP 集成

提供 MCP (Model Context Protocol) 接口，使 Cursor AI 能够直接调用这些运维工具。

详见：[tool/mcp/README.md](mcp/README.md)

## Examples
```bash
./tool/ops.sh init:schema
./tool/ops.sh role:task binance-spot-link-kline-batch
./tool/ops.sh flink:list
./tool/ops.sh flink:upload
./tool/ops.sh flink:run --entry-class com.twilight.aggregator.KlineSignalJob
./tool/ops.sh flink:job kline
./tool/ops.sh flink:job perp
./tool/ops.sh flink:cancel kline
./tool/test.sh scenario:run binance_kline --stages=infra,ingress
```

### Git 分支提效命令

```bash
# 1) 同步 main 并创建新分支
./tool/git-branch.sh new feat/ws-buffer-refactor

# 2) 单独同步主干
./tool/git-branch.sh sync-main

# 3) 将最新主干合并到当前分支
./tool/git-branch.sh merge-main
```

### Kafka 微结构增量导出到 ODS

将 `perp/spot` 六个微结构 topic（`orderbook.diff`、`orderbook.snapshot`、`aggtrades`）导出到 `runtime/data/ods`。
脚本会按 `exchange_ts` 自动拆分到 `YYYY-MM-DD` 分区，避免跨天数据混到同一天目录。

```bash
python3 tool/export_kafka_microstructure_to_ods.py --once
```

按角色配置自动提取 topic：

```bash
python3 tool/export_kafka_microstructure_to_ods.py \
  --roles-config datainjector/worker/configs/aave/roles_aave_full_stable.json \
  --once
```

持续运行（每 5 分钟增量导出一次）：

```bash
python3 tool/export_kafka_microstructure_to_ods.py --interval-seconds 300
```

首次需要补历史时可启用：

```bash
python3 tool/export_kafka_microstructure_to_ods.py --from-beginning --once
```

全量快速导出（多 topic 并发 + topic 内批量并发，追到当前末尾立即退出）：

```bash
python3 tool/export_kafka_microstructure_to_ods.py \
  --roles-config datainjector/worker/configs/aave/roles_aave_full_stable.json \
  --consumer-group ods-microstructure-exporter-full-$(date +%Y%m%d-%H%M%S) \
  --from-beginning --stop-at-log-end --once \
  --topic-concurrency 6 \
  --intra-topic-concurrency 6 \
  --topic-batch-size 100000 \
  --poll-timeout-ms 30000 \
  --batch-retry 3
```

Notes:
- `flink:upload` / `flink:run` 默认使用最新构建的 jar。
- `flink:status` 默认展示全部 job（可传 job id 查看单个）。
- `flink:cancel` 默认取消全部运行中的 job；关键词按 Flink job 名称匹配。

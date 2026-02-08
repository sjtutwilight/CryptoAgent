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

Notes:
- `flink:upload` / `flink:run` 默认使用最新构建的 jar。
- `flink:status` 默认展示全部 job（可传 job id 查看单个）。
- `flink:cancel` 默认取消全部运行中的 job；关键词按 Flink job 名称匹配。

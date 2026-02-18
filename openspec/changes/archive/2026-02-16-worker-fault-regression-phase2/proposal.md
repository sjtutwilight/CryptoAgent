## Why

当前 `datainjector/worker` 在接入真实节点时的断连、重连、补数验证主要依赖手工命令与人工读日志，执行成本高且容易漏判。需要将验证流程沉淀为可重复、可自动判定、可按 role 输出结果的一键回归能力，以支持稳定上线和持续回归。

## What Changes

- 新增基于 `DataPlatform/automation/test` 的 DataInjector 故障回归场景，覆盖断连重连与数据缺失补数验证。
- 新增回归判定器：按 role 聚合结构化日志事件，输出 PASS/FAIL、缺失事件、失败证据。
- 新增 mock/real 两类注入编排：
  - mock 模式复用 `mockDataProvider` 的故障注入参数。
  - real 模式通过节点级故障动作（如容器暂停/恢复）触发重连链路。
- 新增固定化回归产物（`summary.json`、`summary.txt`、`evidence.jsonl`）并接入现有 `automation/test/runs/<run_id>/`。
- 调整 worker 可观测事件模型，补齐完整性/补数关键事件的结构化日志，减少对文本日志正则的依赖。

## Capabilities

### New Capabilities
- `datainjector-fault-regression`: 一键执行故障注入与日志核验，按 role 输出回归结果与缺失事件。
- `worker-integrity-structured-events`: 提供完整性与 backfill 关键路径的结构化事件，供自动判定稳定消费。

### Modified Capabilities
- 无。

## Impact

- 影响代码：
  - `automation/test/scenarios/`、`automation/test/probes/`、`automation/test/tools/`
  - `datainjector/worker/internal/observability/logging/`
  - `datainjector/worker/internal/handler/integrity/`、`datainjector/worker/internal/role/`
- 影响流程：验证从“人工执行 + 人工读日志”升级为“脚本编排 + 自动判定 + 标准报告”。
- 外部接口：不引入新的对外业务 API，主要新增测试与观测能力。

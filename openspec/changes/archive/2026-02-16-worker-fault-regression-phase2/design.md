## Context

当前 DataInjector 的故障验证流程分散在手工命令与人工日志核验中，执行路径不稳定，且无法形成可重复、可比较的回归结果。`automation/test` 已具备 Scenario/Stage/Probe 框架与运行产物体系，但缺少面向故障注入与日志规则判定的专用场景。另一方面，worker 在完整性与 backfill 关键链路仍存在 `log.Printf` 文本日志，导致自动判定器无法仅依赖结构化事件稳定工作。

该变更的上下文约束：
- 优先复用 `DataPlatform/automation/test`，不新增平行测试框架。
- mock 故障来源优先复用 `datainjector/mockDataProvider` 已有注入能力。
- 真实节点验证需要可脚本化执行故障动作并可恢复。
- 回归结果必须按 role 聚合并输出缺失事件与失败证据。

## Goals / Non-Goals

**Goals:**
- 在 `automation/test` 中新增一键故障回归场景，覆盖断连重连、数据缺失补数两类核心验证。
- 构建统一判定器：基于结构化日志事件进行规则判定，输出 role 维度 PASS/FAIL。
- 补齐 worker 完整性/backfill 关键路径结构化事件，使判定器可稳定消费。
- 固化运行产物格式，支持回归比较与流水线接入。

**Non-Goals:**
- 不修改业务数据处理语义（如 orderbook/trade 业务字段和下游 topic 协议）。
- 不引入新的外部基础设施或替换现有测试框架。
- 不在本次变更中实现复杂网络编排平台（如专用 chaos 平台）。

## Decisions

1. 复用 `automation/test` 作为唯一编排与产物框架。
- 决策：新增故障回归场景与 probe，不新增独立脚本体系。
- 原因：保持执行入口统一（`tool/test.sh`），降低维护成本。
- 备选：新增独立 make/shell 流程。未选：会造成运行协议与产物分叉。

2. 故障注入采用双模式（mock/real）并统一阶段化流程。
- 决策：场景统一分为 prepare/inject/observe/assert/report 阶段；mock 模式调用 mock provider 配置注入，real 模式执行可回滚的容器级故障动作（pause/unpause）。
- 原因：同时满足快速回归与真实节点验证。
- 备选：仅 mock 模式。未选：无法覆盖真实网络环境重连稳定性。

3. 判定规则采用“强必需事件 + 失败事件 + 窗口恢复”三段式。
- 决策：
  - 断连重连：必须命中 `ws.reconnect.start` 与 `ws.reconnect.success`，并在恢复窗口出现 `caller.response`/`pipeline.finish`。
  - 补数恢复：必须命中 `integrity/backfill trigger` 与 `backfill success`；命中 `backfill exhausted` 直接失败。
  - 守护规则：命中 `handler.error`/`sink.error`/`pipeline.error` 直接失败。
- 原因：兼顾链路完整性与失败快速识别。
- 备选：仅统计错误数。未选：无法表达“缺失关键事件”的结构化结论。

4. Phase 2 默认要求完整性/backfill关键节点结构化事件化。
- 决策：在 worker 中补齐 backfill 与 integrity 事件常量与结构化日志埋点，判定器优先消费结构化日志；文本日志仅作临时兼容证据源。
- 原因：文本日志格式不稳定，难以长期作为自动回归依据。
- 备选：继续依赖正则匹配 `log.Printf`。未选：漏判与误判风险高。

5. 结果输出标准化为三类产物。
- 决策：每次运行输出 `summary.json`（机器读）、`summary.txt`（人工读）、`evidence.jsonl`（证据明细）并挂载到 `automation/test/runs/<run_id>/`。
- 原因：兼容现有运行目录并支持后续 CI 门禁。
- 备选：仅终端输出。未选：不可追溯、不可对比。

## Risks / Trade-offs

- [风险] 真实节点故障动作（pause/unpause）与线上故障形态存在偏差。→ 缓解：保留动作抽象接口，后续可扩展 iptables/netem。
- [风险] 事件补齐前，判定器对旧日志的兼容逻辑较复杂。→ 缓解：分层实现“结构化优先 + 文本兜底”，并逐步淘汰兜底路径。
- [风险] 场景等待窗口设置不当会导致假失败。→ 缓解：将观察窗口参数化并输出窗口与阈值到报告。
- [风险] 按 role 聚合时可能受到缺失 `role_id` 事件影响。→ 缓解：在 worker 新增事件中强制带 `role_id`，并在报告中标注 `unscoped` 事件。

## Migration Plan

1. 在 `automation/test` 增加故障回归场景、probe 与报告写出逻辑，接入 `tool/test.sh scenario:run`。
2. 在 worker 增加完整性/backfill结构化事件常量与埋点，保留原有文本日志作为短期兼容。
3. 联调 mock 模式（`integrity_clean`/`integrity_gap`）与 real 模式（容器暂停/恢复）并校准阈值。
4. 在预发执行回归并冻结基线报告格式。
5. 回滚策略：
   - 回归场景可直接停用，不影响业务链路；
   - worker 事件埋点回滚为上一版本，保持原处理链路不变。

## Open Questions

- real 模式默认故障动作是否仅支持容器级，还是首版即纳入网络级注入（iptables/netem）。
- 断连恢复与补数恢复的默认观察窗口（例如 30s/60s）是否按 role 类型差异化。
- 是否在首版引入 CI 门禁阈值（例如允许部分 role flaky）还是先以人工审核报告为主。

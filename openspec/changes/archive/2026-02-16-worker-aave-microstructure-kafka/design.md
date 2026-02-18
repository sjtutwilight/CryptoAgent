## Context

当前 `datainjector/worker` 的 AAVE 微观结构接入默认落到本地录制文件（`runtime/data/recording/...`），再通过离线步骤转换，适合调试回放但不适合实时数据面。实时下游期望直接消费 Kafka，因此需要把 AAVE 微观结构链路切为 Kafka 直出。

worker 现有能力已经支持 Kafka sink 与控制 API 的 validate/apply 流程，但 AAVE 稳定角色配置仍是 file sink 形态。本次变更仅覆盖 worker 侧，不涉及 dbt 与 strategy_engine。

约束：
- 必须保持 orderbook / aggtrades 规范化消息字段契约不变。
- 必须保持治理化发布路径（`--config` + `--roles`，以及 `/api/roles/validate` -> `/api/roles/apply`）。
- 不引入新的外部依赖，不扩大为全局配置系统重构。

相关方：
- 数据接入运维（worker 发布）
- 实时消费方（微观结构计算任务）
- 平台治理方（控制面/数据面规范）

## Goals / Non-Goals

**Goals:**
- 将 AAVE spot/perp 的 orderbook 与 aggtrades 角色从 file sink 迁移到 Kafka sink。
- 为 AAVE 微观结构流定义确定性的 topic 与 key 规则。
- 通过角色注册与控制 API 形成可验证、可回滚的发布路径。
- 在迁移过程中保持下游消息契约兼容。

**Non-Goals:**
- 不修改 dbt 模型。
- 不修改 strategy_engine 的特征与训练逻辑。
- 不在本次内重构 worker 全量模板体系。
- 不移除录制工具（仍可作为调试辅助路径）。

## Decisions

### Decision 1: AAVE 微观结构默认数据面输出改为 Kafka
- 选择：将四个 AAVE 角色（spot/perp × orderbook/aggtrades）统一改为 `sink.type=kafka`。
- 原因：满足实时链路要求，移除默认 file->离线转换依赖。
- 备选：同一角色内双写 file+kafka。未采用，因为当前角色仅支持单 sink，双写会扩大改造范围。

### Decision 2: 仅迁移传输层，不变更消息结构
- 选择：保留现有 handler 产出的 orderbook/aggtrades 字段，变更仅限 sink 从 file 到 Kafka。
- 原因：最小化对下游消费者影响。
- 备选：同步引入新版本 envelope。未采用，因为会增加发布与回滚复杂度。

### Decision 3: 使用 `symbol + exchange` 作为确定性 key
- 选择：Kafka sink 使用 `key_from: ["symbol", "exchange"]`。
- 原因：保持同交易对分区内顺序稳定，利于状态型消费者。
- 备选：空 key/随机分布。未采用，因为顺序稳定性差。

### Decision 4: 发布流程治理化
- 选择：将角色文件作为发布工件，要求先 `validate`，再 `apply`。
- 原因：降低配置错误导致的线上失败概率。
- 备选：直接 apply。未采用，因为可控性不足。

### Decision 5: 在迁移中并行修复低风险可靠性问题
- 选择：移除 aggtrade handler 的调试输出；为 Kafka sink 增加写超时配置。
- 原因：避免高频日志污染与潜在写阻塞。
- 备选：后续再修。未采用，因为两项问题直接影响新默认链路稳定性。

## Risks / Trade-offs

- [Topic 约定与下游不一致] → Mitigation: 使用平台 topic 清单并在 staging 做探针校验。
- [分区策略变化影响顺序] → Mitigation: 强制 deterministic key，并验证同 symbol 的消费有序性。
- [角色发布误操作] → Mitigation: 固化 validate/resolve/apply 顺序，预备回滚角色文件。
- [历史脚本依赖 file 输出] → Mitigation: 明确 BREAKING 默认路径变化，保留录制工具作为非默认调试路径。
- [写超时参数不合适] → Mitigation: 提供可配置值与保守默认，并监控 sink 错误率/延迟。

## Migration Plan

1. 修改 AAVE 角色注册文件：
   - sink 切换到 Kafka；
   - 配置目标 topic 与 `key_from`。
2. 准备 topic 与权限检查（如需）。
3. 在 staging 部署 worker 变更。
4. 使用 `/api/roles/validate` 校验角色。
5. 在 staging apply 后验证：
   - 目标 topic 有消息；
   - 字段契约一致；
   - 无明显 sink/handler 错误。
6. 生产发布：
   - 按治理流程 apply；
   - 监控 lag、错误率、消费健康。
7. 回滚策略：
   - 回切到旧版 file-sink 角色文件；
   - 保持代码兼容，便于快速恢复。

## Open Questions

- AAVE spot 是否使用独立 `binance.spot.*` topic，还是短期复用现有统一 topic？
- 后续多 token 扩展时，是否需要 role 级 topic 动态映射能力？
- Kafka 写超时默认值应设为多少以平衡背压与稳定性？

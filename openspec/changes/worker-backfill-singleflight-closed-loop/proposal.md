## Why

当前 `rec-binance-perp-aave-orderbook-full` 在缺失检测触发补数时会出现高频 `integrity.backfill.enqueue.error (enqueue_timeout)`，表现为补数重试风暴与告警洪泛。问题不是单点超时参数，而是缺失检测与 backfill 执行耦合过紧，缺少“单飞会话 + 结果驱动”的闭环控制，必须在需求层面重构补数编排语义。

## What Changes

- 引入 backfill 单飞会话（single-flight session）机制：同一 `role_id + stream_key + backfill_type` 任一时刻只允许一个 in-flight backfill。
- 将触发逻辑从“超时持续入队”改为“结果驱动状态机”：pending 期间禁止重复调度，只有收到 success/fail 结果后才进入下一步。
- 为 backfill 命令与结果引入显式协议字段（如 `cmd_id/session_id/attempt/error_class/snapshot_seq`），保证幂等关联与可追踪。
- 增加 keyed 去重调度队列：同 key 的 pending 请求合并/覆盖，而不是无界重复写入 channel。
- 为 backfill failure 增加统一退避策略（指数退避 + 最大尝试 + 冷却窗），避免失败期间触发洪泛。
- 更新可观测性契约，新增/规范 backfill 生命周期事件与指标，用于验证“不洪泛、不并发重复调度”的不变量。

## Capabilities

### New Capabilities
- `worker-backfill-singleflight-closed-loop`: 定义 backfill 单飞会话、结果驱动状态迁移、去重调度与退避控制的统一行为契约。

### Modified Capabilities
- `worker-backfill-command-delivery`: 从“尽力投递命令”扩展为“带命令身份与结果回执的闭环投递语义”。

## Impact

- 主要影响代码：`datainjector/worker/internal/handler/integrity/*`、`datainjector/worker/internal/role/role.go`、`datainjector/worker/internal/types/backfill.go`、相关 metrics/logging。
- 运行影响：降低 `enqueue_timeout` 风暴概率，提升补数稳定性并减少噪音告警。
- 运维影响：新增 backfill 状态与结果事件后，可直接按会话维度做 SLO 监控与故障定位。

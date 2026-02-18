## Why

当前 worker 的 roles 热更新采用“先全停再逐个启动”，在新配置部分失败时会出现中断和半成功状态，直接影响在线数据连续性与完整性。该问题已达到 P0 级别，需要优先将切换流程改为失败可回滚、单角色故障隔离的机制。

## What Changes

- 将 `Apply` 从全量替换改为按 `role_id` 的增量 reconcile（add/update/remove/unchanged）。
- 引入两阶段切换流程：先完成新角色实例的构建与就绪检查，再执行提交切换。
- 提交阶段按角色独立执行 swap，单角色失败触发该角色回滚，不影响其他角色。
- 移除热更新路径中对 `stopAll()` 的依赖，`stop` 仅作用于目标角色集合。
- 增加热更新结果回执，返回每个 role 的执行结果与失败原因，便于控制面重试。

## Capabilities

### New Capabilities
- `worker-role-atomic-reconcile`: worker 支持按角色独立、可回滚的原子化热更新流程，避免全局中断和半成功状态。

### Modified Capabilities
- 无

## Impact

- 受影响代码：`datainjector/worker/internal/role/manager.go`、`datainjector/worker/internal/api/server.go`。
- 运行行为影响：roles 热更新由“全停全起”改为“按 role 增量切换”。
- API 影响：`/api/roles/apply` 的返回体需要包含按 role 的详细结果。
- 测试影响：需新增热更新原子性与单角色故障隔离相关测试。

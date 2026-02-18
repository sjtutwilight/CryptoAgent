## 1. Reconcile 与两阶段切换框架

- [ ] 1.1 在 `internal/role/manager.go` 新增 apply 串行锁与 apply 上下文结构（diff、prepare 缓存、per-role result）。
- [ ] 1.2 实现按 `role_id` 的差异计算（add/update/remove/unchanged），替换当前全量 stop/start 流程。
- [ ] 1.3 实现 Prepare 阶段：对 add/update 角色完成 build + validate + readiness 检查，任一失败则中止提交。
- [ ] 1.4 实现 Commit 阶段：按角色 swap 新旧 runner，并在单角色失败时执行该角色回滚。

## 2. 生命周期与并发安全加固

- [ ] 2.1 重构 `stopAll` 依赖，确保 apply 路径不再触发全局停机。
- [ ] 2.2 为角色停止等待增加超时控制，避免 `<-done` 无界阻塞。
- [ ] 2.3 校正运行态 map 的写入时机，避免未就绪 runner 过早暴露为“运行中”。

## 3. 控制面 API 与回执

- [ ] 3.1 扩展 `/api/roles/apply` 响应结构，增加 `results[]`（role_id/action/status/error）。
- [ ] 3.2 保留兼容字段（如 `status`）并定义部分成功语义，确保现有调用方可平滑过渡。

## 4. 测试与回归验证

- [ ] 4.1 新增管理器单测：prepare 失败时不提交且在线角色不中断。
- [ ] 4.2 新增管理器单测：单角色 commit 失败触发该角色回滚，其他角色不受影响。
- [ ] 4.3 新增管理器单测：remove 仅停止目标角色，不影响 unchanged 角色。
- [ ] 4.4 新增 API 层测试：apply 返回按角色结果，包含部分成功与失败场景。

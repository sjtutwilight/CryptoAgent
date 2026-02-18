## Why

`datainjector/worker/internal/caller/native_call_websocket.go` 已超过 1500 行并混合了配置解析、协议分发、订阅路由、缓冲回压和 RPC pending 管理等职责。继续在单文件上迭代不仅增加认知成本，也使并发状态边界与路由语义更容易出现隐患扩散。

## What Changes

- 对 `native_call_websocket.go` 进行轻量重构，按职责拆分到多个文件，保持外部接口与核心语义不变。
- 将“已识别的明显隐患”纳入同一 change 同步治理，重点覆盖并发状态访问边界、共享路由状态一致性与缓冲回压关键路径。
- 采用分阶段执行：先完成零行为变更重构，再在同一 change 内完成治理性改动与验证。
- 增加行为一致性与风险收敛保护：补充回归测试与并发检查，确保改造结果可合并。

## Capabilities

### New Capabilities
- `worker-websocket-caller-light-refactor`: 约束 WebSocket caller 在不改变外部契约前提下进行轻量模块化重构，并在同一 change 中分阶段完成隐患治理与验证。

### Modified Capabilities
- 无

## Impact

- 受影响代码：`datainjector/worker/internal/caller/native_call_websocket.go` 及同目录新增拆分文件。
- 受影响测试：`datainjector/worker/internal/caller/*websocket*_test.go`（新增或调整回归/并发用例）。
- 对外接口与配置：保持兼容，不引入新的外部依赖。

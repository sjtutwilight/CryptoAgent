## Why

`datainjector/worker` 当前存在高复杂度大文件、模块边界漂移与隐式耦合问题，导致改动回归风险高、排障成本高。现在需要先落地轻量且可脚本化的质量门禁，确保后续重构在可控轨道内推进。

## What Changes

- 在 `datainjector/worker` 引入 `golangci-lint` 质量门禁，统一执行静态检查、复杂度与可维护性规则。
- 在 `datainjector/worker` 引入 `go-arch-lint` 架构依赖规则，约束 `role/emitter/caller/handler/sink/protocol` 的跨包依赖方向。
- 增加一键执行脚本（本地/CI 共用）与最小文档，输出可供 Codex 消费的检查结果。
- 采用“先告警后收敛”的阈值策略，优先阻断新增违规，避免一次性清债导致落地失败。

## Capabilities

### New Capabilities
- `worker-architecture-quality-gates`: 为 worker 模块提供可执行的静态质量门禁与架构依赖门禁，支持本地和 CI 一致执行。

### Modified Capabilities
- 无。

## Impact

- 受影响代码：`datainjector/worker`（新增 lint/arch 配置、执行脚本、文档）。
- 受影响流程：开发者本地检查流程、CI 检查流程。
- 新增依赖：`golangci-lint`、`go-arch-lint`（均为命令行工具，非常驻进程）。
- 预期收益：降低架构退化速度，提供后续模块优化的机器可读依据。

## ADDED Requirements

### Requirement: WebSocket caller 轻量重构必须保持行为等价
系统 MUST 在拆分 `native_call_websocket.go` 后保持既有调用行为不变，包括订阅触发时机、消息分发路径、metadata 关键字段与错误返回语义。

#### Scenario: JSON-RPC 订阅路径行为不变
- **WHEN** 调用方在重构后执行与重构前相同的 JSON-RPC 订阅与消息拉取流程
- **THEN** 系统 MUST 产生等价的订阅确认、消息入缓冲和返回行为

#### Scenario: 非 JSON-RPC 协议消息路由保持一致
- **WHEN** 调用方处理 Binance 或 Hyperliquid 消息格式
- **THEN** 系统 MUST 保持与重构前一致的 stream/channel 路由与过滤结果

### Requirement: WebSocket caller 必须按职责形成清晰边界
系统 MUST 将配置解析、协议消息处理、订阅路由状态、缓冲与背压策略拆分为可独立演进的实现单元，且调用入口保持稳定。

#### Scenario: 调用入口保持稳定
- **WHEN** 上层通过现有 factory 与 `WebSocketCall` 入口调用 caller
- **THEN** 系统 MUST 无需变更上层调用代码即可完成编译与运行

#### Scenario: 单点改动影响范围收敛
- **WHEN** 开发者只调整缓冲/背压策略实现
- **THEN** 改动 MUST 可限定在对应职责文件内，避免触及协议消息处理主流程

### Requirement: 明显隐患必须在同一 change 内分阶段治理
系统 MUST 在同一 change 中完成“先重构、后治理”的两阶段执行，且治理阶段 MUST 覆盖并发状态边界与共享路由一致性等已识别隐患。

#### Scenario: 阶段化执行可追踪
- **WHEN** change 进入实施阶段
- **THEN** 任务清单 MUST 明确标注重构阶段与治理阶段，并可独立验收

#### Scenario: 治理范围可验证
- **WHEN** 完成治理阶段代码改动
- **THEN** 系统 MUST 能证明并发状态访问边界与共享路由状态同步已被显式收敛

### Requirement: 重构与治理必须由回归测试保护关键路径
系统 MUST 在重构与治理中保留并补充关键测试，覆盖订阅路由、共享连接过滤、缓冲丢弃与 RPC pending 响应匹配路径，并执行并发检查。

#### Scenario: 共享路由与订阅过滤回归通过
- **WHEN** 执行 caller 目录 websocket 路由相关测试
- **THEN** 系统 MUST 通过并保持既有共享路由过滤语义

#### Scenario: 缓冲与 RPC 匹配回归通过
- **WHEN** 执行 caller 目录 websocket 缓冲与 RPC 响应匹配相关测试
- **THEN** 系统 MUST 通过并保持既有缓冲丢弃策略与 pending 响应交付语义

#### Scenario: 并发检查通过
- **WHEN** 对 websocket caller 相关包执行并发检查
- **THEN** 系统 MUST 不出现新增并发竞态告警

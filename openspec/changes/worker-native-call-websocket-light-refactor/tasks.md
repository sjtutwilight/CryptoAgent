## 1. 阶段一：代码结构轻量拆分（零行为变更）

- [ ] 1.1 将 `native_call_websocket.go` 中配置解析与构建逻辑拆分到独立实现文件，保持 `NewWebSocketCall` 对外行为不变
- [ ] 1.2 将消息处理分发（jsonrpc/binance/hyperliquid）拆分到独立实现文件，保持现有入口与路由语义
- [ ] 1.3 将订阅路由状态与共享 hub 同步逻辑拆分到独立实现文件，保持现有共享过滤行为
- [ ] 1.4 完成阶段一回归自检（不含治理改动），确保可作为治理阶段基线

## 2. 阶段二：隐患治理（在同一 change 内完成）

- [ ] 2.1 在拆分过程中保持 `WebSocketCall` 外部接口、关键 metadata 字段与错误语义不变
- [ ] 2.2 收敛共享状态读写边界（含订阅路由、allowed streams、buffer/backpressure 相关状态），消除明显并发隐患
- [ ] 2.3 治理共享路由与订阅状态同步隐患，确保共享连接场景下过滤行为稳定可预测
- [ ] 2.4 在治理完成后复核并保持缓冲丢弃策略（`drop_oldest`/`drop_newest`）与 backpressure 切换语义不变

## 3. 阶段三：回归验证与交付

- [ ] 3.1 保留并更新 caller 目录 websocket 路由、共享连接、缓冲相关测试以匹配拆分后结构
- [ ] 3.2 增加或补强 RPC pending 响应匹配路径回归测试，验证重构前后行为一致
- [ ] 3.3 执行并发检查（如 `go test -race` 的 websocket caller 相关范围）并记录结果
- [ ] 3.4 运行并记录关键测试结果，确认 change 达到可实施与可合并状态

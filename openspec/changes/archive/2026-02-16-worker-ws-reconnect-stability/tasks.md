## 1. 连接生命周期幂等治理

- [x] 1.1 在 `WebSocketClient.Connect()` 增加幂等保护，防止重复启动 read/heartbeat goroutine
- [x] 1.2 校准 close/失败路径的生命周期状态重置，确保重连后仍保持单一活动生命周期

## 2. 重连归属与心跳协同

- [x] 2.1 改造 reconnect 入口，接收失败连接上下文并忽略 stale reconnect 触发
- [x] 2.2 调整 read loop 错误处理，将活动连接上下文传入 reconnect 逻辑
- [x] 2.3 引入 reconnecting 状态标记，在重连进行期间跳过 heartbeat 写入

## 3. 传输层稳定性加固

- [x] 3.1 在 dialer 默认配置中关闭 websocket 压缩
- [x] 3.2 在连接建立后增加 read deadline 与 pong handler（pong 到达时续期）
- [x] 3.3 根据 heartbeat interval 设定并固化 deadline 策略（含必要代码注释）

## 4. 回归测试补齐

- [x] 4.1 增加重复 `Connect()` 场景测试，验证不会创建重复生命周期 worker
- [x] 4.2 增加 stale 连接读错误场景测试，验证不会误伤新活动连接
- [x] 4.3 增加重连窗口内 heartbeat tick 行为测试（不写入、不告警）
- [x] 4.4 增加传输默认值测试（压缩关闭、deadline/pong 生效）

## 5. 运行验证与观测

- [x] 5.1 执行 worker 相关测试并确认 websocket 协议模块用例通过
- [ ] 5.2 在本地/预发验证 `ws.read.error`、`ws.heartbeat.error` 风暴明显下降
- [ ] 5.3 输出 `ws.reconnect.*`、`ws.read.error`、`ws.heartbeat.error` 前后对比数据

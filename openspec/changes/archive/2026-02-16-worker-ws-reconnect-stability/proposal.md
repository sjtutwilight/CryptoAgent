## 为什么

当前 `datainjector-worker` 在 Binance WebSocket 链路中持续出现 `ws.read.error` 与 `ws.heartbeat.error`，连接与重连路径会并发重叠，导致读循环/心跳循环重复启动并放大重连风暴。该问题已影响流式采集稳定性，需要优先修复。

## 变更内容

- 将 `WebSocketClient.Connect()` 改为幂等，避免重复启动 read/heartbeat goroutine。
- 为重连增加“失败连接归属校验”，仅允许当前活动连接触发 teardown + reconnect。
- 在重连进行期间跳过心跳发送，避免 `websocket未连接` 告警刷屏。
- 关闭 WebSocket 压缩（per-message compression）以降低当前网络路径下的解码异常概率。
- 增加 read deadline 与 pong handler，增强半开连接检测能力。
- 补充针对 Connect 幂等、重连归属、心跳与重连协调的回归测试。

## 能力范围

### 新增能力
- `worker-websocket-connection-stability`：稳定 worker 原生 websocket 调用链路中的连接生命周期（connect/reconnect/heartbeat/read loop 协调）。

### 修改现有能力
- 无。

## 影响

- 影响代码：`datainjector/worker/internal/protocol/websocket.go` 及相关测试代码。
- 运行时影响：降低重连风暴频率、减少非故障型心跳告警、减少 malformed frame / flate 相关读错误。
- 外部 API/协议：无对外契约变更，属于内部稳定性增强。

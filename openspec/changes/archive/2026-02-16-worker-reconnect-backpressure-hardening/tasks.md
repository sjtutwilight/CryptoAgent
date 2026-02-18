## 1. 部署基线与配置准备

- [ ] 1.1 重建并发布最新 `worker-app` 镜像到运行容器，确认移除旧 `aggtrade backfill` 404 路径
- [x] 1.2 为重连控制、订阅去重、连接复用、backfill 冷静期、背压保护新增配置项与默认值
- [x] 1.3 增加配置热加载或重启加载校验，确保缺省配置可安全启动

## 2. 重连控制器实现

- [x] 2.1 在 websocket 管理层实现指数退避+jitter 计算器（含上限）
- [x] 2.2 增加最小重连间隔门控，禁止短时间连续重连
- [x] 2.3 为每条连接实现 singleflight 重连锁，合并并发重连触发
- [x] 2.4 增加 `1008 policy violation` 连续触发计数与冷静期逻辑

## 3. 订阅幂等与连接复用

- [x] 3.1 引入 `desired_subscriptions` 期望集合与当前订阅状态缓存
- [x] 3.2 实现重连成功后的差量恢复，仅发送缺失订阅一次
- [x] 3.3 实现短窗重复订阅去重与去重计数
- [x] 3.4 按 endpoint 合并连接并支持同连接多 stream 订阅

## 4. Backfill 失败闭环

- [x] 4.1 为 range/snapshot backfill 增加有限重试与退避策略
- [x] 4.2 实现 backfill exhausted 标记与 `healthy/degraded/cooldown` 状态回写
- [x] 4.3 在冷静期内阻断重复 backfill 触发并输出剩余冷静时间
- [x] 4.4 为 backfill 失败链路补充结构化日志与错误分类

## 5. 背压治理

- [x] 5.1 为 WS 缓冲队列增加高低水位阈值与背压保护动作
- [x] 5.2 将解析与 sink 写入解耦，接入有限并发 worker 池
- [x] 5.3 为 sink 实现按条数/时间窗批量写入
- [x] 5.4 在背压状态下对缺口检测与补数调度增加限频/延迟

## 6. 可观测与告警

- [x] 6.1 输出 `ws.reconnect.start/success/failure`、`ws.buffer.drop`、`backfill.exhausted`、`ws.policy.1008` 指标
- [x] 6.2 为关键指标补充 endpoint/role 等标签并统一命名空间
- [x] 6.3 配置告警阈值（重连频率、drop、exhausted、1008）及严重级别
- [x] 6.4 在告警负载中增加主因分类字段（链路抖动/限流/背压）

## 7. 验证与回滚

- [x] 7.1 增加单元测试覆盖重连节流、单飞重连、订阅去重、backfill 冷静期
- [x] 7.2 增加集成测试覆盖 endpoint 合并连接与背压场景
- [x] 7.3 进行灰度验证，确认 30 分钟窗口内 `ws.read.error` 与 `ws.buffer.drop` 显著下降
- [x] 7.4 验证配置开关回滚路径，确保可逐项关闭新能力

## 1. 角色配置迁移

- [x] 1.1 将 `datainjector/worker/configs/aave/roles_aave_full_stable.json` 中 AAVE orderbook/aggtrades 角色的 `sink.type` 从 `file` 改为 `kafka`。
- [x] 1.2 为 spot/perp AAVE 微观结构角色配置目标 topic 与确定性分区键（`key_from: ["symbol", "exchange"]`）。
- [x] 1.3 在角色说明中标注治理化启动方式（`--config` + `--roles`），明确 file 录制不再是默认链路。

## 2. Worker 可靠性修复

- [x] 2.1 删除 `datainjector/worker/internal/handler/binance_handlers.go` 中 aggtrade 路径的 `fmt.Println` 调试输出。
- [x] 2.2 在 `datainjector/worker/internal/sink/kafka.go` 增加 Kafka 写入超时配置，并以有界 context 执行写入。
- [x] 2.3 保持 Kafka sink 配置向后兼容：未提供超时参数时仍可按默认策略运行。

## 3. 验证与契约检查

- [x] 3.1 通过 `/api/roles/validate` 校验更新后的 AAVE 角色定义，修复校验失败项。
- [x] 3.2 在 staging 执行 apply 后验证 orderbook 消息在目标 topic 到达且字段完整。
- [x] 3.3 验证 aggtrades 消息字段契约与 key 规则符合预期，并确认下游可正常消费。

## 4. 发布与回滚准备

- [x] 4.1 更新 worker 文档，说明 AAVE 微观结构 Kafka-first 默认接入路径与影响范围。
- [x] 4.2 准备旧版 file-sink 角色回滚工件与运维操作说明。
- [x] 4.3 形成上线核对清单（topic 健康、consumer lag、sink 错误率）并用于生产发布签收。

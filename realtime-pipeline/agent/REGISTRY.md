# Realtime Pipeline REGISTRY（仅限本模块）
## 1) 权威文档（Single Source of Truth）
| Path | What | Freshness |
|---|---|---|
| ../../.note/ARCHITECTURE.md | 提供实时链路与其它子系统的边界说明，帮助定位上游 injector 与下游 ClickHouse | reviewed: 2025-12-16 |
| ../DESIGN.md | Realtime Pipeline 模块设计文档，描述 DWD 层处理流程与数据清洗策略 | reviewed: 2025-12-17 |
| README.md | 描述 DexSwapDwdJob 的编译依赖、Kafka 主题以及常见问题排查步骤 | reviewed: 2025-12-17 |

## 2) 脚本
| Path | What | Usage |
|---|---|---|
| run-job.sh | 构建依赖、生成 JAR 并以本地 JVM 启动 DexSwapDwdJob 的主脚本 | ./agent/run-job.sh |
| check-sink.sh | 执行 Kafka Topic 存在性检查、统计消息总数并展示最新消息，辅助验证 sink 健康 | ./agent/check-sink.sh |
| watch-sink.sh | 长时间消费 dwd_dex_swap Topic 以观测增量数据与 key/timestamp，适合联调阶段 | ./agent/watch-sink.sh |

## 3) 你生成的临时文档（允许短期存在，必须可合并或删除）
| Path | What |
|---|---|
| _暂无_ | 当前信息均沉淀在 readme.md 中，后续若新增实验或 FAQ 文档需首先登记于此 |

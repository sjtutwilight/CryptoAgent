# Aggregator REGISTRY（仅限本模块）
## 1) 权威文档（Single Source of Truth）
| Path | What | Freshness |
|---|---|---|
| ../../.note/ARCHITECTURE.md | 平台级架构蓝图，描述数据接入、Flink 聚合以及下游消费的边界与数据流 | reviewed: 2025-12-16 |
| PROJECT_STATUS.md | 当前 Aggregator 各 Job 的推进状态、依赖配置和联调约束，暂时代替 DESIGN 作为模块内的唯一事实来源 | reviewed: 2025-02-14 |

## 2) 脚本
| Path | What | Usage |
|---|---|---|
| run-job.sh | 提供本地、Docker 以及 Flink 集群三种运行模式的作业入口脚本，涵盖 PnL/Token/Perp 等全量 Job | ./agent/run-job.sh pnl [local\|docker\|cluster] |

## 3) 你生成的临时文档（允许短期存在，必须可合并或删除）
| Path | What |
|---|---|
| .note/perp.md | 汇总永续指标流水线的实验记录、指标准入评估及未提交的参数，后续需要沉淀到 DESIGN | 
| .note/production-config-example.yaml | 生产环境配置示例，列出 Kafka/Flink/ClickHouse 等关键参数，便于上线前复核 | 

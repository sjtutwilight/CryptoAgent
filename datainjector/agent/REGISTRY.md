# DataInjector REGISTRY（仅限本模块）
## 1) 权威文档（Single Source of Truth）
| Path | What | Freshness |
|---|---|---|
| ../../.note/ARCHITECTURE.md | 说明控制面、Worker 与下游 Kafka 的互动方式，是理解任务生命周期的先决文档 | reviewed: 2025-12-16 |
| IMPLEMENTATION_SUMMARY.md | 汇总数据接入层的组件划分、配置驱动设计及当前支持的数据源，供新成员快速了解实现 | reviewed: 2025-02-14 |

## 2) 脚本
| Path | What | Usage |
|---|---|---|
| test_dune_real.sh | 一键调起 Worker、发送真实 Chainlink 任务并校验 Kafka/输出目录的连通性，可验证整条链路 | ./agent/test_dune_real.sh |

## 3) 你生成的临时文档（允许短期存在，必须可合并或删除）
| Path | What |
|---|---|
| QUICK_START_DUNE.md | 针对 Dune Token Holders 场景的本地环境搭建与调试笔记，未来需并入 DESIGN 或操作手册 | 

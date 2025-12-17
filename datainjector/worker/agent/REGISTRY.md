# Worker REGISTRY（仅限本模块）
## 1) 权威文档（Single Source of Truth）
| Path | What | Freshness |
|---|---|---|
| ../DESIGN.md | Worker 配置驱动架构设计，定义 Emitter/Caller/Handler/Sink 抽象与数据完整性保障机制 | reviewed: 2025-12-17 |
| README.md | Worker 使用说明，包含主要模块、工作流与配置示例 | reviewed: 2025-12-17 |

## 2) 脚本
| Path | What | Usage |
|---|---|---|
| test_dune_integration.sh | Dune Token Holders 场景的集成测试脚本，验证 Worker 端到端流程 | ./agent/test_dune_integration.sh |

## 3) 你生成的临时文档（允许短期存在，必须可合并或删除）
| Path | What |
|---|---|
| integrity模块架构文档.md | Integrity 模块完整架构文档，描述序列控制、缺失检测、补数调度等核心设计 |
| 订单簿维护逻辑解析.md | Binance 订单簿快照与 diff 合并逻辑的详细解析 |
| 数据正确性与订单簿设计.md | 订单簿数据正确性保障设计文档 |
| DUNE_INTEGRATION.md | Dune Token Holders 场景的集成说明文档 |
| ARCHITECTURE_ANALYSIS.md | Worker 架构分析文档，描述模块设计与数据流 |


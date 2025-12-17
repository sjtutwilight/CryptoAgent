# Backend REGISTRY（仅限本模块）
## 1) 权威文档（Single Source of Truth）
| Path | What | Freshness |
|---|---|---|
| ../../.note/ARCHITECTURE.md | 描述 API 层与 Aggregator/Frontend 的交互、数据流及部署拓扑，作为跨模块沟通基础 | reviewed: 2025-12-16 |
| ../DESIGN.md | Backend 模块设计文档，描述 API 架构、数据流与查询优化策略 | reviewed: 2025-12-17 |
| API_PROBLEM_RESOLUTION_REPORT.md | 后端 API 故障根因、修复与验证记录，确保对历史问题可回溯 | reviewed: 2025-02-14 |

## 2) 脚本
| Path | What | Usage |
|---|---|---|
| quick_test.sh | 调用健康检查、Token 概览与多条核心 API 的一键体检脚本，可快速验证实例是否可用 | ./agent/quick_test.sh |

## 3) 你生成的临时文档（允许短期存在，必须可合并或删除）
| Path | What |
|---|---|
| FRONTEND_BACKEND_ALIGNMENT_ANALYSIS.md | 前后端字段与接口对齐分析，包含变更影响与联调计划 | 
| api-test-examples.md | 常用 API 请求与示例响应集合，便于 QA/前端复用 | 
| kline_analytics_api.md | K 线分析接口参数、分页策略与示例响应说明 | 
| perp_analytics_api.md | 永续分析 API 分类、风控指标及查询示例 | 

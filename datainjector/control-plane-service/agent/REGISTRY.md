# Control Plane Service REGISTRY（仅限本模块）
## 1) 权威文档（Single Source of Truth）
| Path | What | Freshness |
|---|---|---|
| ../DESIGN.md | 控制面服务设计文档，描述任务调度、状态管理、限流控制与 Worker 协调机制 | reviewed: 2025-12-17 |
| INTEGRATION_GUIDE.md | 控制面服务集成指南，说明与 Worker 的交互协议与接口定义 | reviewed: 2025-12-17 |
| README.md | 控制面服务使用说明，包含启动方式、API 接口与配置说明 | reviewed: 2025-12-17 |

## 2) 脚本
| Path | What | Usage |
|---|---|---|
| test_integration.sh | 控制面服务集成测试脚本，验证任务下发、状态上报、限流等核心功能 | ./agent/test_integration.sh |

## 3) 你生成的临时文档（允许短期存在，必须可合并或删除）
| Path | What |
|---|---|
| _暂无_ | 当前控制面说明均在 DESIGN.md 与 INTEGRATION_GUIDE.md 中 |


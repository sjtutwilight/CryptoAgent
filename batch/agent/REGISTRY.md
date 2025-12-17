# Batch REGISTRY（仅限本模块）
## 1) 权威文档（Single Source of Truth）
| Path | What | Freshness |
|---|---|---|
| README_SPARK.md | 介绍批处理集群的 Airflow/Spark/StarRocks 角色、依赖与 docker-compose 运行说明 | reviewed: 2025-12-17 |
| README_TOKEN_HOLDERS.md | Token Holders 数据分析完整说明，包含数据流、表结构、查询示例 | reviewed: 2025-12-17 |

## 2) 脚本
| Path | What | Usage |
|---|---|---|
| scripts/run-token-holders-import.sh | Token Holders 数据导入脚本，从 Dune 导出文件导入 Paimon 表 | INPUT_PATH=/path DRY_RUN=false ./scripts/run-token-holders-import.sh |
| scripts/test-token-holders-flow.sh | Token Holders 完整流程测试脚本，验证数据导入、查询、分析全流程 | ./scripts/test-token-holders-flow.sh |
| scripts/run-dex-job.sh | DEX 批处理作业脚本，运行 DEX 数据的 ODS/DWD/DWS 层处理 | DRY_RUN=false WRITE_STARROCKS=true ./scripts/run-dex-job.sh |
| scripts/start-lab.sh | Spark 实验环境启动脚本，启动 Spark/Paimon/MinIO 等服务 | ./scripts/start-lab.sh |
| scripts/stop-lab.sh | Spark 实验环境停止脚本，停止所有实验环境服务 | ./scripts/stop-lab.sh |

## 3) 你生成的临时文档（允许短期存在，必须可合并或删除）
| Path | What |
|---|---|
| _暂无_ | 当前批处理说明均在 README.md 与 README_TOKEN_HOLDERS.md 中 | 

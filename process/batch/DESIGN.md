# Batch 设计文档

## 模块定位
批处理层，提供Spark批处理作业与数据分析能力。

## 核心功能
- Spark批处理：DEX数据ODS/DWD/DWS层处理
- Token Holders分析：Dune数据导入、持仓分布分析
- 数据湖仓：Paimon表管理、StarRocks数据写入
- 调度编排：Airflow DAG管理

## 技术栈
- Apache Spark
- Apache Paimon（数据湖）
- StarRocks（OLAP引擎）
- Apache Airflow（调度）
- MinIO（对象存储）

## 数据流
```
数据源/Dune导出 → Spark Job → Paimon/StarRocks
                              ↓
                         分析查询/可视化
```

## 关键设计
- 流式湖仓：Paimon作为统一存储层
- 分层处理：ODS（原始）→ DWD（明细）→ DWS（汇总）
- 实验环境：docker-compose一键启动Spark集群

## 详细说明
- 批处理环境：参见 `agent/README.md`
- Token Holders：参见 `spark/README_TOKEN_HOLDERS.md`

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架
- 2025-12-23: 增加 tool/data.sh 作为数据初始化入口，包含 Postgres 维表与 Spark->Paimon 导入命令
- 2025-12-23: tool/data.sh 增加 schema:init，一键初始化 Postgres/ClickHouse/Kafka schema
- 2025-12-23: init_topics.sh 移除对 load-infra-env.sh 的依赖
- 2025-12-23: 完成 Token Holders 数据通路调通
  - 数据来源：Dune API -> runtime/data/dune/token-holders/{chain_id}/{address}/*.json
  - 数据上传：tool/test.sh spark:upload-test-data 上传至 MinIO s3a://paimon-warehouse/test-data/
  - 数据导入：tool/data.sh spark:token-holders 使用 Spark 读取 JSON 写入 Paimon
  - 数据验证：tool/test.sh spark:verify-paimon 验证 Paimon 表数据
  - 表结构：paimon.crypto_analytics.token_holders_snapshot，826k+条记录导入成功
  - 技术要点：修复 Paimon Spark SQL 建表语法（primary-key 使用 TBLPROPERTIES，write 使用 insertInto）
- 2025-12-23: 重构测试代码结构符合项目规范
  - 新增 automation/test/probes/spark_probe.py 实现 Spark 测试逻辑
  - tool/test.sh 仅作为入口，调用 automation/test 下的 Probe
  - 更新 tool/README.md 明确入口脚本职责：参数解析+调用实现，不实现具体功能




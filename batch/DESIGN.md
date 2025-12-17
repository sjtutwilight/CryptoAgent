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


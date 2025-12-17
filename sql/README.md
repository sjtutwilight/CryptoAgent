# SQL数据结构索引

本目录包含项目所有数据库的DDL、视图、ETL和查询脚本，按存储类型和操作类型组织。

## 目录结构

```
sql/
├── clickhouse/          # ClickHouse数据库
│   ├── ddl/            # 表结构定义
│   │   ├── onchain.sql      # 链上数据：trade_fact/pnl/token_metrics
│   │   ├── holder.sql       # 持仓分析：balance_snapshot/holder_balance
│   │   ├── perp.sql         # 永续合约：exec/ctx/panel/signals
│   │   ├── kline.sql        # K线分析：kline_metrics/indicators
│   │   └── quality.sql      # 数据质量：quality_metrics/alerts
│   └── view/           # 视图定义
│       ├── onchain_views.sql    # 链上视图：trades/macro/pnl
│       ├── holder_views.sql     # 持仓视图：top_holders/distribution/tags
│       └── quality_views.sql    # 质量视图：stream_health/rule_health
├── postgres/            # PostgreSQL数据库
│   ├── ddl/            # 表结构定义
│   │   ├── control_plane.sql    # 控制面：tasks表
│   │   └── quality.sql          # 质量引擎：alert_records/rule_configs
│   └── migration/      # 数据迁移脚本
│       └── account_bitmap.sql   # 账户标签位图迁移
├── starrocks/           # StarRocks数据库
│   ├── ddl/            # 表结构定义
│   │   ├── catalog.sql          # Paimon Catalog配置
│   │   └── analytics.sql        # 分析表：token_metrics/holder_metrics
│   └── query/          # 分析查询脚本
│       └── token_holders_analysis.sql  # 持仓分析查询集
├── paimon/              # Paimon数据湖
│   ├── ddl/            # 表结构定义
│   │   └── lake_bronze.sql      # Bronze层：tx_transaction/tx_events
│   ├── etl/            # ETL作业
│   │   └── dex_ingest.sql       # DEX数据入湖
│   └── demo/           # 演示脚本
│       └── datagen.sql          # 测试数据生成
└── schema/              # 数据字典与规范
    ├── DATA_DICTIONARY.md       # 核心表数据字典
    └── TOPIC_MAPPING.md         # Topic-表映射规范
```

## Quick Start

### ClickHouse

```bash
# 初始化所有DDL
cat sql/clickhouse/ddl/*.sql | clickhouse-client -mn

# 创建视图
cat sql/clickhouse/view/*.sql | clickhouse-client -mn

# 单独初始化某个业务域
clickhouse-client -mn < sql/clickhouse/ddl/onchain.sql
```

### PostgreSQL

```bash
# 初始化控制面
psql -U twilight -d twilight -f sql/postgres/ddl/control_plane.sql

# 初始化质量引擎
psql -U twilight -d twilight -f sql/postgres/ddl/quality.sql

# 执行迁移脚本
psql -U twilight -d twilight -f sql/postgres/migration/account_bitmap.sql
```

### StarRocks

```bash
# 配置Paimon Catalog
mysql -h starrocks -P 9030 < sql/starrocks/ddl/catalog.sql

# 创建分析表
mysql -h starrocks -P 9030 < sql/starrocks/ddl/analytics.sql

# 执行分析查询
mysql -h starrocks -P 9030 < sql/starrocks/query/token_holders_analysis.sql
```

### Paimon (Flink SQL)

```bash
# 创建数据湖表
sql-client.sh -f sql/paimon/ddl/lake_bronze.sql

# 运行ETL作业
sql-client.sh -f sql/paimon/etl/dex_ingest.sql

# 运行Demo（测试数据生成）
sql-client.sh -f sql/paimon/demo/datagen.sql
```

## 核心表索引

### ClickHouse核心表

| 表名 | 业务域 | 用途 | DDL文件 |
|-----|-------|------|---------|
| ch_account_trade_fact | 链上数据 | 账户交易事实表 | clickhouse/ddl/onchain.sql |
| ch_account_pnl_current_ma | 链上数据 | 账户PnL当前状态 | clickhouse/ddl/onchain.sql |
| token_recent_metric_ch | 链上数据 | Token时序指标 | clickhouse/ddl/onchain.sql |
| ch_account_balance_snapshot | 持仓分析 | 账户余额快照 | clickhouse/ddl/holder.sql |
| ch_token_holder_balance_latest | 持仓分析 | 最新持币地址 | clickhouse/ddl/holder.sql |
| dws_exec_1s | 永续合约 | 执行面秒级指标 | clickhouse/ddl/perp.sql |
| dws_perps_ctx_1m | 永续合约 | 语境面分钟级指标 | clickhouse/ddl/perp.sql |
| dws_perps_panel_1m | 永续合约 | 汇合面板指标 | clickhouse/ddl/perp.sql |
| perp_signals | 永续合约 | 异常信号表 | clickhouse/ddl/perp.sql |
| kline_metrics | K线分析 | K线指标表 | clickhouse/ddl/kline.sql |
| kline_indicator_metrics | K线分析 | 技术指标表 | clickhouse/ddl/kline.sql |
| quality_metrics | 数据质量 | 质量指标表 | clickhouse/ddl/quality.sql |

### PostgreSQL核心表

| 表名 | 业务域 | 用途 | DDL文件 |
|-----|-------|------|---------|
| tasks | 控制面 | 任务调度表 | postgres/ddl/control_plane.sql |
| quality_alert_records | 数据质量 | 告警记录表 | postgres/ddl/quality.sql |
| quality_rule_configs | 数据质量 | 规则配置表 | postgres/ddl/quality.sql |

### StarRocks核心表

| 表名 | 业务域 | 用途 | DDL文件 |
|-----|-------|------|---------|
| token_recent_metric_sr | 链上数据 | Token时序指标 | starrocks/ddl/analytics.sql |
| token_holder_metrics | 持仓分析 | 持有者指标汇总 | starrocks/ddl/analytics.sql |

### Paimon核心表

| 表名 | 业务域 | 用途 | DDL文件 |
|-----|-------|------|---------|
| tx_transaction | 数据湖 | DEX交易事实表 | paimon/ddl/lake_bronze.sql |
| tx_events | 数据湖 | DEX事件表 | paimon/ddl/lake_bronze.sql |

## 视图索引

| 视图名 | 依赖表 | 用途 | 文件 |
|-------|-------|------|------|
| v_token_trades_detail | ch_account_trade_fact | Token交易明细 | clickhouse/view/onchain_views.sql |
| v_token_macro_latest | ch_account_pnl_current_ma | Token宏观指标(NUPL/MVRV/SOPR) | clickhouse/view/onchain_views.sql |
| v_token_top_holders_latest | ch_token_holder_balance_latest | Top持币地址 | clickhouse/view/holder_views.sql |
| v_token_distribution_minute | ch_token_holder_balance_latest | Token分布统计 | clickhouse/view/holder_views.sql |
| v_token_holder_tag_minute | ch_token_holder_balance_latest | 标签分布及变化率 | clickhouse/view/holder_views.sql |
| v_stream_health_1h | quality_metrics | 流健康度(1小时) | clickhouse/view/quality_views.sql |
| v_rule_health_1h | quality_metrics | 规则健康度(1小时) | clickhouse/view/quality_views.sql |

## 命名规范

| 对象类型 | 规范 | 示例 |
|---------|------|------|
| ClickHouse表 | `ch_` + 业务域 + 功能 | `ch_account_trade_fact` |
| 视图 | `v_` + 业务域 + 功能 | `v_token_top_holders_latest` |
| 物化视图 | `mv_` + 源表 + 聚合粒度 | `mv_holder_balance_latest` |
| StarRocks表 | `_sr` 后缀 | `token_recent_metric_sr` |
| Kafka Topic | 业务域.子域 | `perp.orderbook` |

## 相关文档

- **数据字典**: `schema/DATA_DICTIONARY.md` - 核心表的详细字段说明
- **Topic映射**: `schema/TOPIC_MAPPING.md` - Kafka Topic与表的映射关系
- **架构文档**: `../.note/ARCHITECTURE.md` - 系统整体架构
- **ADR决策**: `../.note/ADR/004-sql-governance.md` - SQL治理决策记录

## 维护说明

### 新增表/视图流程

1. 在对应存储类型的`ddl/`或`view/`目录创建SQL文件
2. 在`schema/DATA_DICTIONARY.md`注册表结构
3. 在本文件更新索引表格
4. 如涉及Topic，更新`schema/TOPIC_MAPPING.md`
5. 在对应模块的`DESIGN.md`追加变更记录

### 文件头模板

```sql
-- ============================================================
-- 模块: [业务域名称]
-- 存储: [ClickHouse/PostgreSQL/StarRocks/Paimon]
-- 维护: [负责模块]
-- 上游Topic: [topic1, topic2]
-- 关联Job: [FlinkJobName]
-- 用途: [简要说明]
-- ============================================================
```

## 向后兼容

原SQL文件保持不变，新结构为增量补充：
- `aggregator/agent/clickhouse-init.sql` - 保留原文件
- `batch/sql/*.sql` - 保留原文件
- `batch/starrocks/*.sql` - 保留原文件
- `datainjector/*/rebuild_tasks_table.sql` - 保留原文件
- `onedata/agent/sql/*.sql` - 保留原文件


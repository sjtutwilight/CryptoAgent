# Token Holders 数据分析流程

将 Dune API 获取的 Token Holders JSON 数据导入 Paimon，并通过 StarRocks 进行分析。

## 架构流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    Token Holders 数据流                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. JSON 数据源                                                  │
│     /tmp/dune/token-holders/{chain_id}/{address}/holders_*.json │
│                          │                                      │
│                          ▼                                      │
│  2. Spark 作业 (token_holders_import.py)                        │
│     - 读取 JSON 文件                                             │
│     - 数据转换 (balance 精度处理)                                │
│     - 写入 Parquet 格式                                          │
│                          │                                      │
│                          ▼                                      │
│  3. Paimon 数据湖 (MinIO S3)                                    │
│     Database: crypto_analytics                                  │
│     Table: token_holders_snapshot                               │
│     Partition: (chain_id, snapshot_date)                        │
│                          │                                      │
│                          ▼                                      │
│  4. StarRocks 查询分析                                           │
│     - Paimon Catalog 联邦查询                                    │
│     - 计算资产集中度指标                                          │
│     - Top Holders 分析                                          │
│     - 持仓分布统计                                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 快速开始

### 1. 启动环境

```bash
cd runtime/batch/spark
./scripts/start-lab.sh
```

确保以下服务正常运行：
- Spark Master & Worker
- MinIO (对象存储)
- StarRocks (OLAP 数据库)

### 2. 导入数据

```bash
# 基本用法 (自动从路径提取 chain_id 和 token_address)
./scripts/run-token-holders-import.sh

# 指定输入路径
INPUT_PATH=/tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca \
  ./scripts/run-token-holders-import.sh

# 手动指定参数
INPUT_PATH=/path/to/json \
  CHAIN_ID=1 \
  TOKEN_ADDRESS=0x514910771af9ca656af840dff83e8264ecf986ca \
  SNAPSHOT_DATE=2025-12-16 \
  ./scripts/run-token-holders-import.sh

# Dry Run 模式 (仅预览不写入)
DRY_RUN=true ./scripts/run-token-holders-import.sh
```

### 3. 配置 StarRocks Paimon Catalog

```bash
# 连接 StarRocks
mysql -h127.0.0.1 -P9030 -uroot

# 执行配置脚本
source /opt/spark-config/starrocks-paimon-catalog.sql
```

或手动执行：

```sql
-- 创建 Paimon Catalog
CREATE EXTERNAL CATALOG IF NOT EXISTS paimon_catalog
PROPERTIES (
    "type" = "paimon",
    "warehouse" = "s3://paimon-warehouse/wh",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "password123",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true"
);

-- 验证连接
SHOW DATABASES FROM paimon_catalog;
```

### 4. 执行分析查询

参考 `config/token_holders_analytics.sql` 中的 SQL 查询：

```sql
-- 查看数据概览
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    COUNT(*) as holder_count,
    SUM(balance_readable) as total_supply
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
GROUP BY chain_id, token_address, snapshot_date;

-- Top 10 Holders
SELECT 
    wallet_address,
    balance_readable,
    balance_readable / SUM(balance_readable) OVER () * 100 as percentage
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
WHERE chain_id = 1 
  AND token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
  AND snapshot_date = '2025-12-16'
ORDER BY balance_readable DESC
LIMIT 10;
```

## 数据模型

### Paimon 表结构

```sql
paimon.crypto_analytics.token_holders_snapshot
├── wallet_address (STRING, PK)        # 钱包地址
├── balance (DECIMAL(38,0))            # Token余额 (最小单位)
├── balance_readable (DECIMAL(38,18))  # Token余额 (可读格式)
├── first_acquired (TIMESTAMP)         # 首次获得时间
├── has_initiated_transfer (BOOLEAN)   # 是否发起过转账
├── chain_id (INT, PK, PARTITION)      # 链ID
├── token_address (STRING, PK)         # Token合约地址
├── snapshot_date (DATE, PK, PARTITION)# 快照日期
├── snapshot_timestamp (TIMESTAMP)     # 快照时间戳
└── data_source (STRING)               # 数据来源
```

**分区策略**: 按 `chain_id` 和 `snapshot_date` 分区  
**分桶策略**: 按 `wallet_address` 分4个桶

## 分析指标

### 1. Token 资产集中度

- **Top N 持仓占比**: Top 1/10/50/100 持有者的持仓比例
- **Gini 系数**: 衡量财富分配不平等程度 (0=完全平等, 1=完全不平等)
- **HHI 指数**: Herfindahl-Hirschman Index，市场集中度指标
  - HHI < 1500: 低集中度
  - 1500 ≤ HHI < 2500: 中等集中度
  - HHI ≥ 2500: 高集中度

### 2. Top Holders 分析

- Top 100 持有者列表
- 巨鲸地址识别 (持仓 > 1% 总供应量)
- 持仓占比分析
- 持有时长统计

### 3. 持仓分布

- 按余额区间分布 (< 1, 1-10, 10-100, 100-1K, 1K-10K, ...)
- 按持有时长分布 (< 1个月, 1-3个月, 3-6个月, ...)
- 活跃/非活跃钱包分类

## 示例查询

### 查询 Top 10 持有者

```sql
SELECT 
    wallet_address,
    balance_readable,
    balance_readable / SUM(balance_readable) OVER () * 100 as pct_of_supply,
    first_acquired,
    DATEDIFF(CURRENT_DATE, DATE(first_acquired)) as holding_days
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
WHERE chain_id = 1 
  AND token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
  AND snapshot_date = '2025-12-16'
ORDER BY balance_readable DESC
LIMIT 10;
```

### 计算 Top 10 集中度

```sql
WITH ranked AS (
    SELECT 
        balance_readable,
        ROW_NUMBER() OVER (ORDER BY balance_readable DESC) as rank
    FROM paimon_catalog.crypto_analytics.token_holders_snapshot
    WHERE chain_id = 1 
      AND token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
      AND snapshot_date = '2025-12-16'
)
SELECT 
    SUM(balance_readable) as top10_balance,
    SUM(balance_readable) / (
        SELECT SUM(balance_readable) 
        FROM paimon_catalog.crypto_analytics.token_holders_snapshot
        WHERE chain_id = 1 
          AND token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
          AND snapshot_date = '2025-12-16'
    ) * 100 as top10_percentage
FROM ranked
WHERE rank <= 10;
```

### 持仓区间分布

```sql
SELECT 
    CASE 
        WHEN balance_readable < 1 THEN '< 1'
        WHEN balance_readable < 10 THEN '1-10'
        WHEN balance_readable < 100 THEN '10-100'
        WHEN balance_readable < 1000 THEN '100-1K'
        WHEN balance_readable < 10000 THEN '1K-10K'
        WHEN balance_readable < 100000 THEN '10K-100K'
        WHEN balance_readable < 1000000 THEN '100K-1M'
        ELSE '> 1M'
    END as balance_range,
    COUNT(*) as holder_count,
    SUM(balance_readable) as total_balance,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as pct_of_holders,
    SUM(balance_readable) * 100.0 / SUM(SUM(balance_readable)) OVER () as pct_of_supply
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
WHERE chain_id = 1 
  AND token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
  AND snapshot_date = '2025-12-16'
GROUP BY balance_range
ORDER BY MIN(balance_readable);
```

## 文件说明

```
runtime/batch/spark/
├── jobs/
│   └── token_holders_import.py          # Spark 导入作业
├── config/
│   ├── token_holders_analytics.sql      # 分析 SQL 查询集合
│   └── starrocks-paimon-catalog.sql     # StarRocks Catalog 配置
├── scripts/
│   └── run-token-holders-import.sh      # 导入脚本
└── README_TOKEN_HOLDERS.md              # 本文档
```

## 性能优化建议

### 1. Paimon 表优化

- 合理设置分区：按 `chain_id` 和 `snapshot_date` 分区可以加速查询
- 分桶策略：按 `wallet_address` 分桶可以均匀分布数据
- 压缩格式：使用 Parquet 格式可以减少存储空间

### 2. StarRocks 查询优化

- 创建物化视图：将常用的聚合指标预计算
- 本地表同步：将热数据同步到 StarRocks 本地表
- 分区裁剪：查询时指定 `chain_id` 和 `snapshot_date` 可以减少扫描

### 3. 数据更新策略

- 增量更新：每次只导入新的快照数据
- 历史数据归档：定期归档旧的快照数据
- 定期清理：删除过期的快照数据

## 故障排除

### 1. Spark 作业失败

```bash
# 查看 Spark 日志
docker logs spark-lab-master
docker logs spark-lab-worker

# 查看作业详情
# 访问 http://localhost:8088
```

### 2. StarRocks 连接 Paimon 失败

```bash
# 检查 MinIO 连接
curl http://localhost:9000/minio/health/ready

# 检查 StarRocks 日志
docker logs spark-lab-starrocks

# 验证 Catalog 配置
mysql -h127.0.0.1 -P9030 -uroot -e "SHOW CATALOGS;"
```

### 3. 数据查询为空

```bash
# 验证数据是否写入 Paimon
docker exec -it spark-lab-client /opt/spark/bin/spark-sql \
  --packages org.apache.paimon:paimon-spark-3.5:1.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=s3a://paimon-warehouse/wh \
  -e "SELECT COUNT(*) FROM paimon.crypto_analytics.token_holders_snapshot;"
```

## 扩展功能

### 1. 多 Token 批量导入

```bash
# 循环导入多个 token
for token_dir in /tmp/dune/token-holders/1/*; do
    INPUT_PATH=$token_dir ./scripts/run-token-holders-import.sh
done
```

### 2. 定期快照

```bash
# 使用 cron 定期执行
0 0 * * * cd /path/to/runtime/batch/spark && ./scripts/run-token-holders-import.sh
```

### 3. 数据质量检查

```sql
-- 检查重复数据
SELECT 
    chain_id, token_address, wallet_address, snapshot_date, COUNT(*)
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
GROUP BY chain_id, token_address, wallet_address, snapshot_date
HAVING COUNT(*) > 1;

-- 检查异常余额
SELECT *
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
WHERE balance_readable < 0 OR balance_readable IS NULL;
```

## 参考资料

- [Apache Paimon 文档](https://paimon.apache.org/)
- [StarRocks 文档](https://docs.starrocks.io/)
- [Spark SQL 指南](https://spark.apache.org/docs/latest/sql-programming-guide.html)

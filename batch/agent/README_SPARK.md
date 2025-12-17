# Spark 实验环境

独立的 Spark + Paimon + StarRocks 实验环境，从主项目 `docker-compose.yml` 拆分出来用于单独实验。

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Spark 实验环境                            │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Spark Master │  │ Spark Worker │  │ Spark Client │       │
│  │   :8088      │  │   :8089      │  │  (提交作业)   │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│           │                │                │               │
│           └────────────────┴────────────────┘               │
│                            │                                │
│  ┌─────────────────────────┴─────────────────────────┐      │
│  │                                                   │      │
│  │  ┌──────────────┐              ┌──────────────┐   │      │
│  │  │    MinIO     │◄────────────►│  StarRocks   │   │      │
│  │  │ (S3 存储)    │              │   (OLAP)     │   │      │
│  │  │  :9000/:9001 │              │    :9030     │   │      │
│  │  └──────────────┘              └──────────────┘   │      │
│  │         │                                         │      │
│  │         ▼                                         │      │
│  │  ┌──────────────┐                                 │      │
│  │  │   Paimon     │                                 │      │
│  │  │  (数据湖)    │                                 │      │
│  │  └──────────────┘                                 │      │
│  └───────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## 快速开始

### 1. 启动环境

```bash
cd runtime/batch/spark
./scripts/start-lab.sh
```

或手动启动：

```bash
docker-compose -f docker-compose.spark-lab.yml up -d
```

### 2. 验证环境

```bash
./scripts/run-test.sh
```

### 3. 运行 DEX 批处理作业

```bash
# 正常运行（写入 Paimon + StarRocks）
./scripts/run-dex-job.sh

# Dry Run 模式（仅打印不写入）
DRY_RUN=true ./scripts/run-dex-job.sh

# 覆盖 ODS 分区
OVERWRITE_ODS=true ./scripts/run-dex-job.sh
```

### 4. 停止环境

```bash
./scripts/stop-lab.sh

# 清理所有数据
docker-compose -f docker-compose.spark-lab.yml down -v
```

## 访问地址

| 服务 | 地址 | 说明 |
|------|------|------|
| Spark Master UI | http://localhost:8088 | Spark 集群管理界面 |
| Spark Worker UI | http://localhost:8089 | Worker 状态 |
| MinIO Console | http://localhost:9001 | 对象存储管理 (admin/password123) |
| StarRocks MySQL | localhost:9030 | `mysql -h127.0.0.1 -P9030 -uroot` |

## 目录结构

```
runtime/batch/spark/
├── docker-compose.spark-lab.yml  # 实验环境 Docker Compose
├── README.md                      # 本文档
├── config/
│   └── dex_batch_job_config.json  # DEX 作业配置
├── jobs/
│   ├── dex_batch_job.py                  # DEX 批处理主作业
│   ├── token_holders_import.py           # Token Holders 导入作业
│   └── spark_env_test.py                 # 环境测试脚本
├── scripts/
│   ├── start-lab.sh                      # 启动脚本
│   ├── stop-lab.sh                       # 停止脚本
│   ├── run-test.sh                       # 运行测试
│   ├── run-dex-job.sh                    # 运行 DEX 作业
│   ├── run-token-holders-import.sh       # 运行 Token Holders 导入
│   └── test-token-holders-flow.sh        # Token Holders 完整流程测试
└── lib/                                  # JAR 依赖 (自动下载)
```

StarRocks 初始化脚本与分析 SQL 已迁移到 `runtime/batch/starrocks`，供 Spark Lab 和其它批处理环境共用。

## DEX 批处理作业

### 数据流

```
Mock 数据生成
     │
     ▼
┌─────────────────────────────────────────────────────┐
│ ODS (lake_bronze)                                   │
│  - tx_transaction: 交易记录                          │
│  - tx_events: Swap 事件                             │
└─────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────┐
│ DWD (lake_silver)                                   │
│  - dim_account: 账户维度                            │
│  - dim_token: Token 维度                            │
│  - dim_pair: 交易对维度                             │
│  - dwd_dex_trade_di: DEX 交易明细                   │
└─────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────┐
│ DWS (StarRocks)                                     │
│  - token_recent_metric_sr: Token 时序指标           │
│    (对齐 ClickHouse token_recent_metric_ch)         │
└─────────────────────────────────────────────────────┘
```

### 配置说明

`config/dex_batch_job_config.json` 包含：

- **mock**: 模拟数据生成参数
  - `hours`: 生成数据的小时数
  - `base_events_per_hour`: 每小时基础事件数
  - `qty_range`: 交易数量范围
  
- **accounts**: 测试账户列表
- **tokens**: Token 列表 (USDC, WETH, DAI, TWI, WBTC)
- **pairs**: 交易对列表

### 手动提交作业

```bash
docker exec -it spark-lab-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.paimon:paimon-spark-3.5:1.0.0,org.apache.hadoop:hadoop-aws:3.3.4,mysql:mysql-connector-java:8.0.33,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=s3a://paimon-warehouse/wh \
  /opt/spark-jobs/dex_batch_job.py \
  --config /opt/spark-config/dex_batch_job_config.json \
  --warehouse s3a://paimon-warehouse/wh \
  --write-starrocks
```

## 查询结果

### StarRocks

```sql
-- 连接
mysql -h127.0.0.1 -P9030 -uroot

-- 查询 Token 指标
SELECT token_id, time_window, end_time, tag, volume_usd
FROM analytics.token_recent_metric_sr
ORDER BY end_time DESC
LIMIT 20;
```

### Paimon (通过 Spark SQL)

```bash
docker exec -it spark-lab-client /opt/spark/bin/spark-sql \
  --packages org.apache.paimon:paimon-spark-3.5:1.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=s3a://paimon-warehouse/wh
```

```sql
-- 查询 ODS 数据
SELECT * FROM paimon.lake_bronze.tx_events LIMIT 10;

-- 查询 DWD 数据
SELECT * FROM paimon.lake_silver.dwd_dex_trade_di LIMIT 10;
```

## 故障排除

### 1. MinIO 连接失败

检查 MinIO 服务是否正常：
```bash
docker logs spark-lab-minio
curl http://localhost:9000/minio/health/ready
```

### 2. StarRocks 连接失败

检查 StarRocks 服务：
```bash
docker logs spark-lab-starrocks
mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW DATABASES;'
```

### 3. Spark 作业失败

查看 Spark 日志：
```bash
docker logs spark-lab-master
docker logs spark-lab-worker
```

### 4. 清理重建环境

```bash
./scripts/stop-lab.sh
docker-compose -f docker-compose.spark-lab.yml down -v
./scripts/start-lab.sh
```


# Paimon 数据湖说明

- **物理仓库**：`runtime/paimon-warehouse`（MinIO/S3 兼容，Spark & Flink & StarRocks 均通过该路径访问）。
- **挂载方式**：
  - Spark Lab：`docker-compose.spark-lab.yml` 将 `s3a://paimon-warehouse/wh` 映射到 MinIO bucket。
  - Flink：主仓 `docker-compose.yml` 中把 `./runtime/paimon-warehouse` 映射到 `/opt/flink/data/paimon-warehouse`。
  - StarRocks：通过 `runtime/batch/starrocks/starrocks-paimon-catalog.sql` 创建 `paimon_catalog` 进行联邦查询。

## 常用操作

### 1. 本地查看仓库
```bash
cd runtime/paimon-warehouse
tree -L 3
```

### 2. Spark SQL 查询
```bash
docker exec -it spark-lab-client /opt/spark/bin/spark-sql \
  --packages org.apache.paimon:paimon-spark-3.5:1.0.0 \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=s3a://paimon-warehouse/wh \
  -e "SELECT * FROM paimon.lake_bronze.tx_events LIMIT 5;"
```

### 3. StarRocks Link
```sql
USE paimon_catalog;
SHOW DATABASES;
SELECT COUNT(*) FROM crypto_analytics.token_holders_snapshot;
```

> 新增 Lakehouse 表或调试脚本时，务必记录在本 README，保持 Paimon 操作在 `runtime/batch/paimon` 下可追溯。

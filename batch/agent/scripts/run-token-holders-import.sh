#!/bin/bash
# Token Holders 数据导入脚本

set -e

# 默认配置
INPUT_PATH="${INPUT_PATH:-/tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca}"
WAREHOUSE="${WAREHOUSE:-s3a://paimon-warehouse/wh}"
DATABASE="${DATABASE:-crypto_analytics}"
TABLE="${TABLE:-token_holders_snapshot}"
CHAIN_ID="${CHAIN_ID:-}"
TOKEN_ADDRESS="${TOKEN_ADDRESS:-}"
SNAPSHOT_DATE="${SNAPSHOT_DATE:-}"
DRY_RUN="${DRY_RUN:-false}"

# Spark 配置
SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
SPARK_PACKAGES="org.apache.paimon:paimon-spark-3.5:1.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

echo "=========================================="
echo "Token Holders 数据导入"
echo "=========================================="
echo "输入路径: $INPUT_PATH"
echo "Warehouse: $WAREHOUSE"
echo "数据库: $DATABASE"
echo "表名: $TABLE"
echo "Chain ID: ${CHAIN_ID:-自动检测}"
echo "Token Address: ${TOKEN_ADDRESS:-自动检测}"
echo "快照日期: ${SNAPSHOT_DATE:-今天}"
echo "Dry Run: $DRY_RUN"
echo "=========================================="

# 构建参数
ARGS="--input-path $INPUT_PATH --warehouse $WAREHOUSE --database $DATABASE --table $TABLE"

if [ -n "$CHAIN_ID" ]; then
    ARGS="$ARGS --chain-id $CHAIN_ID"
fi

if [ -n "$TOKEN_ADDRESS" ]; then
    ARGS="$ARGS --token-address $TOKEN_ADDRESS"
fi

if [ -n "$SNAPSHOT_DATE" ]; then
    ARGS="$ARGS --snapshot-date $SNAPSHOT_DATE"
fi

if [ "$DRY_RUN" = "true" ]; then
    ARGS="$ARGS --dry-run"
fi

# 提交 Spark 作业
docker exec -it spark-lab-client /opt/spark/bin/spark-submit \
  --master "$SPARK_MASTER" \
  --packages "$SPARK_PACKAGES" \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse="$WAREHOUSE" \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  /opt/spark-jobs/token_holders_import.py $ARGS

echo ""
echo "=========================================="
echo "✅ 作业提交完成"
echo "=========================================="
echo ""
echo "查看 Spark UI: http://localhost:8088"
echo ""
echo "查询数据 (StarRocks):"
echo "  mysql -h127.0.0.1 -P9030 -uroot"
echo "  SELECT * FROM paimon_catalog.$DATABASE.$TABLE LIMIT 10;"
echo ""


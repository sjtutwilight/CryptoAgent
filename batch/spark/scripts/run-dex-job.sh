#!/bin/bash
# 运行 DEX 批处理作业脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================================="
echo "📊 运行 DEX 批处理作业"
echo "=================================================="

cd "$SPARK_DIR"

# 默认参数
DRY_RUN=${DRY_RUN:-false}
WRITE_STARROCKS=${WRITE_STARROCKS:-true}
OVERWRITE_ODS=${OVERWRITE_ODS:-false}

# 构建参数
ARGS=""
if [ "$DRY_RUN" = "true" ]; then
  ARGS="$ARGS --dry-run"
  echo "🔍 模式: Dry Run (仅打印不写入)"
else
  echo "📝 模式: 正常运行"
fi

if [ "$WRITE_STARROCKS" = "true" ] && [ "$DRY_RUN" != "true" ]; then
  ARGS="$ARGS --write-starrocks"
  echo "📤 启用 StarRocks 写入"
fi

if [ "$OVERWRITE_ODS" = "true" ]; then
  ARGS="$ARGS --overwrite-ods"
  echo "🔄 启用 ODS 覆盖模式"
fi

echo ""
echo "⏳ 提交 Spark 作业..."
echo ""

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
  --conf spark.sql.shuffle.partitions=20 \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark/work-dir/jobs/dex_batch_job.py \
  --config /opt/spark/work-dir/config/dex_batch_job_config.json \
  --warehouse s3a://paimon-warehouse/wh \
  --s3-endpoint http://minio:9000 \
  --s3-access-key admin \
  --s3-secret-key password123 \
  --starrocks-jdbc jdbc:mysql://starrocks-allin1:9030/analytics \
  --starrocks-table token_recent_metric_sr \
  $ARGS

echo ""
echo "=================================================="
echo "✅ DEX 批处理作业完成!"
echo "=================================================="
echo ""
echo "📌 查询 StarRocks 结果:"
echo "  mysql -h127.0.0.1 -P9030 -uroot -e 'SELECT * FROM analytics.token_recent_metric_sr LIMIT 10;'"
echo ""

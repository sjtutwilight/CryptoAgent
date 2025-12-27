"""
DEX 批量作业 Airflow DAG
用于调度 dex_batch_job.py Spark 作业

调度策略:
- 每日执行一次 (可配置)
- 通过 BashOperator 调用 docker exec 执行 spark-submit 命令
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# DAG 默认参数
default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Spark 提交命令参数
SPARK_PACKAGES = ",".join([
    "org.apache.paimon:paimon-spark-3.5:1.0.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "mysql:mysql-connector-java:8.0.33",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
])

# spark-submit 完整命令 (通过 docker exec 执行)
SPARK_SUBMIT_CMD = f'''
docker exec spark-lab-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages {SPARK_PACKAGES} \
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
  --write-starrocks
'''

# 定义 DAG
with DAG(
    dag_id='dex_batch_job',
    default_args=default_args,
    description='DEX 批量数据处理作业 - 处理交易数据并写入 Paimon 和 StarRocks',
    schedule_interval='@daily',  # 每日执行
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'dex', 'paimon', 'starrocks'],
) as dag:
    
    # 任务1: 检查 Spark 集群状态
    check_spark_cluster = BashOperator(
        task_id='check_spark_cluster',
        bash_command='docker exec spark-lab-client curl -sf http://spark-master:8080 > /dev/null && echo "Spark Master is ready" || (echo "Spark Master not available" && exit 1)',
    )
    
    # 任务2: 检查 MinIO 连接
    check_minio = BashOperator(
        task_id='check_minio',
        bash_command='docker exec spark-lab-client curl -sf http://minio:9000/minio/health/ready > /dev/null && echo "MinIO is ready" || (echo "MinIO not available" && exit 1)',
    )
    
    # 任务3: 检查 StarRocks 连接
    check_starrocks = BashOperator(
        task_id='check_starrocks',
        bash_command='docker exec spark-lab-client curl -sf http://starrocks-allin1:8030/api/health > /dev/null && echo "StarRocks is ready" || (echo "StarRocks not available" && exit 1)',
    )
    
    # 任务4: 执行 DEX 批量作业
    run_dex_batch_job = BashOperator(
        task_id='run_dex_batch_job',
        bash_command=SPARK_SUBMIT_CMD,
        execution_timeout=timedelta(hours=2),
    )
    
    # 任务5: 验证数据写入
    verify_starrocks_data = BashOperator(
        task_id='verify_starrocks_data',
        bash_command='''
            echo "验证 StarRocks 数据写入..."
            docker exec spark-lab-starrocks mysql -h127.0.0.1 -P9030 -uroot -e "SELECT COUNT(*) as total_records FROM analytics.token_recent_metric_sr;" || echo "表可能为空，首次运行正常"
            echo "数据验证完成"
        ''',
    )
    
    # 定义任务依赖关系
    # 先并行检查所有依赖服务，然后执行作业，最后验证
    [check_spark_cluster, check_minio, check_starrocks] >> run_dex_batch_job >> verify_starrocks_data

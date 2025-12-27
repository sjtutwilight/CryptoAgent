# Init Load Tasks

## dune_token_holders -> Paimon token_holders_snapshot

- task_id: init_dune_token_holders_paimon
- source_manifest: automation/data/manifest/dune_token_holders.yaml
- source_path: runtime/data/dune/token-holders/{chain_id}/{address}/
- target: paimon.crypto_analytics.token_holders_snapshot
- job: process/batch/spark/jobs/token_holders_import.py
- mode: append-only snapshot (partitioned by chain_id, snapshot_date)
- run (example):

```
docker exec spark-lab-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.paimon:paimon-spark-3.5:1.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=s3a://paimon-warehouse/wh \
  /opt/spark/work-dir/jobs/token_holders_import.py \
  --input-path /opt/spark/work-dir/runtime/data/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/ \
  --snapshot-date 2024-12-01
```

- notes:
  - The job auto-extracts chain_id and token_address from the input path if not provided.
  - Input folder may include holders_*.json; the job reads all JSON files under the input path.

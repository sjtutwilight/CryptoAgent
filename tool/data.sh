#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export ROOT_DIR

INFRA_ENV_FILE="${INFRA_ENV_FILE:-$ROOT_DIR/config/infrastructure/env/docker.env}"

SPARK_CLIENT_CONTAINER="${SPARK_CLIENT_CONTAINER:-spark-lab-client}"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
SPARK_WAREHOUSE="${SPARK_WAREHOUSE:-s3a://paimon-warehouse/wh}"
SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.paimon:paimon-spark-3.5:1.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262}"
SPARK_S3_ENDPOINT="${SPARK_S3_ENDPOINT:-http://minio:9000}"
SPARK_S3_ACCESS_KEY="${SPARK_S3_ACCESS_KEY:-admin}"
SPARK_S3_SECRET_KEY="${SPARK_S3_SECRET_KEY:-password123}"

STARROCKS_CONTAINER_NAME="${STARROCKS_CONTAINER_NAME:-spark-lab-starrocks}"
STARROCKS_MYSQL_HOST="${STARROCKS_MYSQL_HOST:-127.0.0.1}"
STARROCKS_MYSQL_PORT="${STARROCKS_MYSQL_PORT:-9030}"
STARROCKS_USER="${STARROCKS_USER:-root}"
STARROCKS_PASSWORD="${STARROCKS_PASSWORD:-}"

PAIMON_WAREHOUSE="${PAIMON_WAREHOUSE:-s3://paimon-warehouse/wh}"
PAIMON_S3_ENDPOINT="${PAIMON_S3_ENDPOINT:-http://minio:9000}"
PAIMON_S3_ACCESS_KEY="${PAIMON_S3_ACCESS_KEY:-admin}"
PAIMON_S3_SECRET_KEY="${PAIMON_S3_SECRET_KEY:-password123}"
PAIMON_S3_PATH_STYLE="${PAIMON_S3_PATH_STYLE:-true}"
PAIMON_S3_REGION="${PAIMON_S3_REGION:-us-east-1}"

POSTGRES_CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-crypto-postgres}"
POSTGRES_USER="${POSTGRES_USER:-twilight}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-twilight123}"
POSTGRES_DB="${POSTGRES_DB:-twilight}"

CLICKHOUSE_CONTAINER_NAME="${CLICKHOUSE_CONTAINER_NAME:-clickhouse}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-default}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"

KAFKA_CONTAINER_NAME="${KAFKA_CONTAINER_NAME:-crypto-kafka}"
KAFKA_BOOTSTRAP_SERVERS_LOCAL="${KAFKA_BOOTSTRAP_SERVERS_LOCAL:-localhost:9092}"

usage() {
  cat <<USAGE
Usage: ./tool/data.sh <command> [args]

Commands:
  schema:init [--skip-postgres] [--skip-clickhouse] [--skip-kafka]
    Init Postgres schema, ClickHouse schema, and Kafka topics.

  postgres:schema:init
    Apply automation/data/schema/postgres/ddl/*.sql to Postgres.

  clickhouse:schema:init
    Apply automation/data/schema/clickhouse/ddl/*.sql and view/*.sql to ClickHouse.

  kafka:topics:init [--topic-file PATH] [--dry-run]
    Create Kafka topics defined in automation/data/schema/topic/topics.list.

  postgres:init-evm-top-tokens [--manifest PATH]
    Init dim_token in Postgres using automation/data/init-load/evm_top_tokens_enhanced_pg_init.py

  spark:upload-test-data [--source-path PATH] [--minio-container NAME] [--bucket NAME] [--target-prefix PATH]
                         [--minio-url URL] [--access-key KEY] [--secret-key KEY]
    Upload token holders test data to MinIO for Spark processing.

  spark:token-holders --input-path PATH [--snapshot-date YYYY-MM-DD] [--chain-id N] [--token-address ADDR]
                     [--warehouse PATH] [--database DB] [--table NAME] [--dry-run]
    Load Dune token holders JSON into Paimon via Spark job.

  query:starrocks:paimon [--database DB] [--table NAME] [--chain-id N] [--token-address ADDR]
                          [--snapshot-date YYYY-MM-DD] [--mode count|sample] [--limit N]
    Query Paimon tables in MinIO via StarRocks external catalog.

Notes:
  - --input-path must be accessible inside the spark-lab-client container.
  - Use s3a://... paths or mount the data into the container.
USAGE
}

load_infra_env() {
  if [[ -f "$INFRA_ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$INFRA_ENV_FILE"
    set +a
  fi
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing command: $1" >&2
    exit 1
  fi
}

ensure_container_path() {
  local input_path="$1"
  case "$input_path" in
    s3a://*|s3://*|file://*|/opt/spark/*)
      return 0
      ;;
    "$ROOT_DIR"/*)
      echo "input-path is a host path: $input_path" >&2
      echo "please use s3a://... or mount the path into the spark client container" >&2
      exit 1
      ;;
    *)
      echo "input-path must be accessible inside the spark client container: $input_path" >&2
      exit 1
      ;;
  esac
}

run_postgres_schema_init() {
  local ddl_dir="$ROOT_DIR/automation/data/schema/postgres/ddl"
  local ddl_files=()

  if [[ ! -d "$ddl_dir" ]]; then
    echo "postgres ddl dir not found: $ddl_dir" >&2
    exit 1
  fi

  shopt -s nullglob
  ddl_files=("$ddl_dir"/*.sql)
  shopt -u nullglob

  if [[ ${#ddl_files[@]} -eq 0 ]]; then
    echo "no postgres ddl files found in $ddl_dir" >&2
    exit 1
  fi

  local docker_args=(docker exec -i -e "PGPASSWORD=$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB")
  for file in "${ddl_files[@]}"; do
    echo "[postgres] apply: $file"
    cat "$file" | "${docker_args[@]}"
  done
}

run_clickhouse_schema_init() {
  local ddl_dir="$ROOT_DIR/automation/data/schema/clickhouse/ddl"
  local view_dir="$ROOT_DIR/automation/data/schema/clickhouse/view"
  local ddl_files=()
  local view_files=()

  shopt -s nullglob
  ddl_files=("$ddl_dir"/*.sql)
  view_files=("$view_dir"/*.sql)
  shopt -u nullglob

  if [[ ${#ddl_files[@]} -eq 0 && ${#view_files[@]} -eq 0 ]]; then
    echo "no clickhouse sql files found in $ddl_dir or $view_dir" >&2
    exit 1
  fi

  local docker_args=(docker exec -i "$CLICKHOUSE_CONTAINER_NAME" clickhouse-client --database "$CLICKHOUSE_DB" --multiquery)
  if [[ -n "$CLICKHOUSE_USER" ]]; then
    docker_args+=(--user "$CLICKHOUSE_USER")
  fi
  if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
    docker_args+=(--password "$CLICKHOUSE_PASSWORD")
  fi

  for file in "${ddl_files[@]}"; do
    echo "[clickhouse] apply: $file"
    cat "$file" | "${docker_args[@]}"
  done

  for file in "${view_files[@]}"; do
    echo "[clickhouse] apply: $file"
    cat "$file" | "${docker_args[@]}"
  done
}

run_kafka_topics_init() {
  local topic_file="$ROOT_DIR/automation/data/schema/topic/topics.list"
  local dry_run="false"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --topic-file)
        topic_file="${2:-}"
        shift 2
        ;;
      --dry-run)
        dry_run="true"
        shift
        ;;
      *)
        echo "unknown option: $1" >&2
        exit 1
        ;;
    esac
  done

  if [[ ! -f "$topic_file" ]]; then
    echo "topic list not found: $topic_file" >&2
    exit 1
  fi

  local script="$ROOT_DIR/automation/data/schema/topic/init_topics.sh"
  if [[ "$dry_run" == "true" ]]; then
    DRY_RUN=true \
      KAFKA_CONTAINER_NAME="$KAFKA_CONTAINER_NAME" \
      KAFKA_BOOTSTRAP_SERVERS_LOCAL="$KAFKA_BOOTSTRAP_SERVERS_LOCAL" \
      TOPIC_FILE="$topic_file" \
      "$script"
  else
    KAFKA_CONTAINER_NAME="$KAFKA_CONTAINER_NAME" \
      KAFKA_BOOTSTRAP_SERVERS_LOCAL="$KAFKA_BOOTSTRAP_SERVERS_LOCAL" \
      TOPIC_FILE="$topic_file" \
      "$script"
  fi
}

run_schema_init_all() {
  local skip_postgres="false"
  local skip_clickhouse="false"
  local skip_kafka="false"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --skip-postgres)
        skip_postgres="true"
        shift
        ;;
      --skip-clickhouse)
        skip_clickhouse="true"
        shift
        ;;
      --skip-kafka)
        skip_kafka="true"
        shift
        ;;
      *)
        echo "unknown option: $1" >&2
        exit 1
        ;;
    esac
  done

  if [[ "$skip_postgres" != "true" ]]; then
    run_postgres_schema_init
  fi
  if [[ "$skip_clickhouse" != "true" ]]; then
    run_clickhouse_schema_init
  fi
  if [[ "$skip_kafka" != "true" ]]; then
    run_kafka_topics_init
  fi
}

run_postgres_init_evm_top_tokens() {
  local manifest_path=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --manifest)
        manifest_path="${2:-}"
        shift 2
        ;;
      *)
        echo "unknown option: $1" >&2
        exit 1
        ;;
    esac
  done

  require_cmd python3
  if [[ -n "$manifest_path" ]]; then
    EVM_TOP_TOKENS_MANIFEST="$manifest_path" \
      python3 "$ROOT_DIR/automation/data/init-load/evm_top_tokens_enhanced_pg_init.py"
  else
    python3 "$ROOT_DIR/automation/data/init-load/evm_top_tokens_enhanced_pg_init.py"
  fi
}

run_spark_upload_test_data() {
  require_cmd python3
  python3 "$ROOT_DIR/automation/data/init-load/token_holders_test_data_upload.py" "$@"
}

run_spark_token_holders() {
  local input_path=""
  local snapshot_date=""
  local chain_id=""
  local token_address=""
  local warehouse="$SPARK_WAREHOUSE"
  local database=""
  local table=""
  local dry_run="false"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --input-path)
        input_path="${2:-}"
        shift 2
        ;;
      --snapshot-date)
        snapshot_date="${2:-}"
        shift 2
        ;;
      --chain-id)
        chain_id="${2:-}"
        shift 2
        ;;
      --token-address)
        token_address="${2:-}"
        shift 2
        ;;
      --warehouse)
        warehouse="${2:-}"
        shift 2
        ;;
      --database)
        database="${2:-}"
        shift 2
        ;;
      --table)
        table="${2:-}"
        shift 2
        ;;
      --dry-run)
        dry_run="true"
        shift
        ;;
      *)
        echo "unknown option: $1" >&2
        exit 1
        ;;
    esac
  done

  if [[ -z "$input_path" ]]; then
    echo "--input-path is required" >&2
    exit 1
  fi

  ensure_container_path "$input_path"

  local job_path="/opt/spark-jobs/token_holders_import.py"
  local spark_args=(
    docker exec "$SPARK_CLIENT_CONTAINER" /opt/spark/bin/spark-submit
    --master "$SPARK_MASTER_URL"
    --packages "$SPARK_PACKAGES"
    --conf "spark.hadoop.fs.s3a.endpoint=$SPARK_S3_ENDPOINT"
    --conf "spark.hadoop.fs.s3a.access.key=$SPARK_S3_ACCESS_KEY"
    --conf "spark.hadoop.fs.s3a.secret.key=$SPARK_S3_SECRET_KEY"
    --conf "spark.hadoop.fs.s3a.path.style.access=true"
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
    --conf "spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog"
    --conf "spark.sql.catalog.paimon.warehouse=$warehouse"
    "$job_path"
    --input-path "$input_path"
  )

  if [[ -n "$snapshot_date" ]]; then
    spark_args+=(--snapshot-date "$snapshot_date")
  fi
  if [[ -n "$chain_id" ]]; then
    spark_args+=(--chain-id "$chain_id")
  fi
  if [[ -n "$token_address" ]]; then
    spark_args+=(--token-address "$token_address")
  fi
  if [[ -n "$database" ]]; then
    spark_args+=(--database "$database")
  fi
  if [[ -n "$table" ]]; then
    spark_args+=(--table "$table")
  fi
  if [[ "$dry_run" == "true" ]]; then
    spark_args+=(--dry-run)
  fi

  "${spark_args[@]}"
}

run_starrocks_paimon_query() {
  require_cmd docker
  STARROCKS_CONTAINER_NAME="$STARROCKS_CONTAINER_NAME" \
    STARROCKS_MYSQL_HOST="$STARROCKS_MYSQL_HOST" \
    STARROCKS_MYSQL_PORT="$STARROCKS_MYSQL_PORT" \
    STARROCKS_USER="$STARROCKS_USER" \
    STARROCKS_PASSWORD="$STARROCKS_PASSWORD" \
    PAIMON_WAREHOUSE="$PAIMON_WAREHOUSE" \
    PAIMON_S3_ENDPOINT="$PAIMON_S3_ENDPOINT" \
    PAIMON_S3_ACCESS_KEY="$PAIMON_S3_ACCESS_KEY" \
    PAIMON_S3_SECRET_KEY="$PAIMON_S3_SECRET_KEY" \
    PAIMON_S3_PATH_STYLE="$PAIMON_S3_PATH_STYLE" \
    PAIMON_S3_REGION="$PAIMON_S3_REGION" \
    "$ROOT_DIR/automation/data/query/starrocks_paimon_query.sh" "$@"
}

main() {
  load_infra_env
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    schema:init)
      run_schema_init_all "$@"
      ;;
    postgres:schema:init)
      run_postgres_schema_init "$@"
      ;;
    clickhouse:schema:init)
      run_clickhouse_schema_init "$@"
      ;;
    kafka:topics:init)
      run_kafka_topics_init "$@"
      ;;
    postgres:init-evm-top-tokens)
      run_postgres_init_evm_top_tokens "$@"
      ;;
    spark:upload-test-data)
      run_spark_upload_test_data "$@"
      ;;
    spark:token-holders)
      run_spark_token_holders "$@"
      ;;
    query:starrocks:paimon)
      run_starrocks_paimon_query "$@"
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      echo "unknown command: $cmd" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"

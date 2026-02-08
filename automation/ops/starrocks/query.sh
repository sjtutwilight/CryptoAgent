#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

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

DB_NAME="crypto_analytics"
TABLE_NAME="token_holders_snapshot"
CHAIN_ID=""
TOKEN_ADDRESS=""
SNAPSHOT_DATE=""
QUERY_MODE="count"
LIMIT="20"

usage() {
  cat <<USAGE
Usage: ./tool/ops.sh starrocks:query [options]

Options:
  --database NAME        Paimon database (default: crypto_analytics)
  --table NAME           Paimon table (default: token_holders_snapshot)
  --chain-id ID          Filter by chain_id
  --token-address ADDR   Filter by token_address
  --snapshot-date DATE   Filter by snapshot_date (YYYY-MM-DD)
  --mode MODE            Query mode: count or sample (default: count)
  --limit N              Limit for sample mode (default: 20)
  -h, --help             Show this help

Examples:
  ./tool/ops.sh starrocks:query --mode count
  ./tool/ops.sh starrocks:query --mode sample --limit 10 --chain-id 1
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing command: $1" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --database)
      DB_NAME="${2:-}"
      shift 2
      ;;
    --table)
      TABLE_NAME="${2:-}"
      shift 2
      ;;
    --chain-id)
      CHAIN_ID="${2:-}"
      shift 2
      ;;
    --token-address)
      TOKEN_ADDRESS="${2:-}"
      shift 2
      ;;
    --snapshot-date)
      SNAPSHOT_DATE="${2:-}"
      shift 2
      ;;
    --mode)
      QUERY_MODE="${2:-}"
      shift 2
      ;;
    --limit)
      LIMIT="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$DB_NAME" || -z "$TABLE_NAME" ]]; then
  echo "database and table are required" >&2
  exit 1
fi

if [[ "$QUERY_MODE" != "count" && "$QUERY_MODE" != "sample" ]]; then
  echo "invalid mode: $QUERY_MODE (use count or sample)" >&2
  exit 1
fi

require_cmd docker

run_mysql() {
  local statement="$1"
  docker exec -i "$STARROCKS_CONTAINER_NAME" \
    mysql -h"$STARROCKS_MYSQL_HOST" -P"$STARROCKS_MYSQL_PORT" -u"$STARROCKS_USER" \
    ${STARROCKS_PASSWORD:+-p"$STARROCKS_PASSWORD"} -e "$statement"
}

create_paimon_catalog() {
  # 检查 catalog 是否已存在
  local existing_catalogs
  existing_catalogs="$(run_mysql "SHOW CATALOGS;" 2>/dev/null | awk 'NR>1 {print $1}')" || true
  
  if echo "$existing_catalogs" | grep -Fxq "paimon"; then
    echo "[info] paimon catalog already exists, skipping creation"
    return 0
  fi

  # 将 s3:// 转换为 s3a:// (StarRocks Paimon 需要使用 s3a 协议)
  local warehouse_path="${PAIMON_WAREHOUSE}"
  if [[ "$warehouse_path" == s3://* ]]; then
    warehouse_path="s3a://${warehouse_path#s3://}"
    echo "[info] converted warehouse path to s3a protocol: $warehouse_path"
  fi

  # 尝试创建 catalog (优先使用 CREATE CATALOG IF NOT EXISTS 语法)
  local catalog_sql
  catalog_sql="CREATE CATALOG IF NOT EXISTS paimon PROPERTIES (
  \"type\" = \"paimon\",
  \"paimon.catalog.type\" = \"filesystem\",
  \"warehouse\" = \"${warehouse_path}\",
  \"paimon.catalog.warehouse\" = \"${warehouse_path}\",
  \"s3.endpoint\" = \"${PAIMON_S3_ENDPOINT}\",
  \"s3.access_key\" = \"${PAIMON_S3_ACCESS_KEY}\",
  \"s3.secret_key\" = \"${PAIMON_S3_SECRET_KEY}\",
  \"s3.path-style-access\" = \"${PAIMON_S3_PATH_STYLE}\",
  \"s3.region\" = \"${PAIMON_S3_REGION}\",
  \"fs.s3a.endpoint\" = \"${PAIMON_S3_ENDPOINT}\",
  \"fs.s3a.access.key\" = \"${PAIMON_S3_ACCESS_KEY}\",
  \"fs.s3a.secret.key\" = \"${PAIMON_S3_SECRET_KEY}\",
  \"fs.s3a.path.style.access\" = \"${PAIMON_S3_PATH_STYLE}\",
  \"fs.s3a.impl\" = \"org.apache.hadoop.fs.s3a.S3AFileSystem\"
);"
  if run_mysql "$catalog_sql" >/dev/null 2>&1; then
    echo "[info] paimon catalog created successfully"
    return 0
  fi

  # 如果上述语法不支持,尝试 CREATE EXTERNAL CATALOG (不带 IF NOT EXISTS)
  catalog_sql="CREATE EXTERNAL CATALOG paimon PROPERTIES (
  \"type\" = \"paimon\",
  \"paimon.catalog.type\" = \"filesystem\",
  \"warehouse\" = \"${warehouse_path}\",
  \"paimon.catalog.warehouse\" = \"${warehouse_path}\",
  \"s3.endpoint\" = \"${PAIMON_S3_ENDPOINT}\",
  \"s3.access_key\" = \"${PAIMON_S3_ACCESS_KEY}\",
  \"s3.secret_key\" = \"${PAIMON_S3_SECRET_KEY}\",
  \"s3.path-style-access\" = \"${PAIMON_S3_PATH_STYLE}\",
  \"s3.region\" = \"${PAIMON_S3_REGION}\",
  \"fs.s3a.endpoint\" = \"${PAIMON_S3_ENDPOINT}\",
  \"fs.s3a.access.key\" = \"${PAIMON_S3_ACCESS_KEY}\",
  \"fs.s3a.secret.key\" = \"${PAIMON_S3_SECRET_KEY}\",
  \"fs.s3a.path.style.access\" = \"${PAIMON_S3_PATH_STYLE}\",
  \"fs.s3a.impl\" = \"org.apache.hadoop.fs.s3a.S3AFileSystem\"
);"
  if run_mysql "$catalog_sql" >/dev/null 2>&1; then
    echo "[info] paimon catalog created successfully (using CREATE EXTERNAL CATALOG)"
    return 0
  else
    echo "[error] failed to create paimon catalog" >&2
    return 1
  fi
}

set_catalog() {
  if run_mysql "SET CATALOG paimon;" >/dev/null 2>&1; then
    return 0
  fi
  run_mysql "USE CATALOG paimon;"
}

where_parts=()
if [[ -n "$CHAIN_ID" ]]; then
  where_parts+=("chain_id = ${CHAIN_ID}")
fi
if [[ -n "$TOKEN_ADDRESS" ]]; then
  where_parts+=("token_address = '${TOKEN_ADDRESS}'")
fi
if [[ -n "$SNAPSHOT_DATE" ]]; then
  where_parts+=("snapshot_date = '${SNAPSHOT_DATE}'")
fi

where_clause=""
if [[ ${#where_parts[@]} -gt 0 ]]; then
  where_clause="WHERE $(IFS=' AND '; echo "${where_parts[*]}")"
fi

if [[ "$QUERY_MODE" == "count" ]]; then
  query_sql="SELECT COUNT(*) AS cnt FROM ${TABLE_NAME} ${where_clause};"
else
  query_sql="SELECT * FROM ${TABLE_NAME} ${where_clause} LIMIT ${LIMIT};"
fi

create_paimon_catalog
set_catalog
db_list="$(run_mysql "SHOW DATABASES;" | awk 'NR>1 {print $1}')"
if ! echo "$db_list" | grep -Fxq "$DB_NAME"; then
  echo "database not found in catalog paimon: $DB_NAME" >&2
  echo "available databases:" >&2
  echo "$db_list" >&2
  exit 1
fi
run_mysql "USE ${DB_NAME};"
run_mysql "${query_sql}"

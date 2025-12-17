#!/bin/bash
# Token Holders 完整流程测试脚本

set -e

echo "=========================================="
echo "Token Holders 数据分析流程测试"
echo "=========================================="
echo ""

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 步骤1: 检查环境
echo -e "${YELLOW}步骤 1/5: 检查环境${NC}"
echo "----------------------------------------"

echo "检查 Spark Master..."
if docker exec spark-lab-client curl -s http://spark-master:8080 > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Spark Master 运行正常"
else
    echo -e "${RED}✗${NC} Spark Master 未运行，请先启动: ./scripts/start-lab.sh"
    exit 1
fi

echo "检查 MinIO..."
if docker exec spark-lab-client curl -s http://minio:9000/minio/health/ready > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} MinIO 运行正常"
else
    echo -e "${RED}✗${NC} MinIO 未运行"
    exit 1
fi

echo "检查 StarRocks..."
if docker exec spark-lab-starrocks mysql -h127.0.0.1 -P9030 -uroot -e "SHOW DATABASES;" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} StarRocks 运行正常"
else
    echo -e "${RED}✗${NC} StarRocks 未运行"
    exit 1
fi

echo ""

# 步骤2: 准备测试数据
echo -e "${YELLOW}步骤 2/5: 准备测试数据${NC}"
echo "----------------------------------------"

# 检查测试数据是否存在
if [ -f "/private/tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/holders_000.json" ]; then
    echo -e "${GREEN}✓${NC} 测试数据已存在"
    
    # 将数据复制到 Docker 容器中
    echo "复制数据到 Spark Client 容器..."
    docker exec spark-lab-client mkdir -p /tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca
    docker cp /private/tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/holders_000.json \
        spark-lab-client:/tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/
    echo -e "${GREEN}✓${NC} 数据复制完成"
else
    echo -e "${RED}✗${NC} 测试数据不存在: /private/tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/holders_000.json"
    echo "请先运行 datainjector 获取数据"
    exit 1
fi

echo ""

# 步骤3: 运行 Spark 导入作业
echo -e "${YELLOW}步骤 3/5: 运行 Spark 导入作业${NC}"
echo "----------------------------------------"

INPUT_PATH=/tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca \
    CHAIN_ID=1 \
    TOKEN_ADDRESS=0x514910771af9ca656af840dff83e8264ecf986ca \
    SNAPSHOT_DATE=$(date +%Y-%m-%d) \
    ./scripts/run-token-holders-import.sh

echo ""

# 步骤4: 配置 StarRocks Paimon Catalog
echo -e "${YELLOW}步骤 4/5: 配置 StarRocks Paimon Catalog${NC}"
echo "----------------------------------------"

echo "创建 Paimon Catalog..."
docker exec spark-lab-starrocks mysql -h127.0.0.1 -P9030 -uroot << 'EOF'
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
EOF

echo -e "${GREEN}✓${NC} Paimon Catalog 创建完成"

echo "验证 Catalog 连接..."
docker exec spark-lab-starrocks mysql -h127.0.0.1 -P9030 -uroot -e "SHOW CATALOGS;"

echo ""

# 步骤5: 执行分析查询
echo -e "${YELLOW}步骤 5/5: 执行分析查询${NC}"
echo "----------------------------------------"

echo "查询1: 数据概览"
docker exec spark-lab-starrocks mysql -h127.0.0.1 -P9030 -uroot << 'EOF'
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    COUNT(*) as holder_count,
    SUM(balance_readable) as total_supply
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
GROUP BY chain_id, token_address, snapshot_date;
EOF

echo ""
echo "查询2: Top 10 Holders"
docker exec spark-lab-starrocks mysql -h127.0.0.1 -P9030 -uroot << 'EOF'
SELECT 
    wallet_address,
    balance_readable,
    ROUND(balance_readable * 100.0 / SUM(balance_readable) OVER (), 4) as pct_of_supply,
    first_acquired
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
WHERE chain_id = 1 
  AND token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
ORDER BY balance_readable DESC
LIMIT 10;
EOF

echo ""
echo "查询3: Top 10 集中度"
docker exec spark-lab-starrocks mysql -h127.0.0.1 -P9030 -uroot << 'EOF'
WITH ranked AS (
    SELECT 
        balance_readable,
        ROW_NUMBER() OVER (ORDER BY balance_readable DESC) as holder_rank
    FROM paimon_catalog.crypto_analytics.token_holders_snapshot
    WHERE chain_id = 1 
      AND token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
),
total AS (
    SELECT SUM(balance_readable) as total_supply
    FROM paimon_catalog.crypto_analytics.token_holders_snapshot
    WHERE chain_id = 1 
      AND token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
)
SELECT 
    SUM(r.balance_readable) as top10_balance,
    ROUND(SUM(r.balance_readable) / t.total_supply * 100, 2) as top10_percentage
FROM ranked r, total t
WHERE r.holder_rank <= 10;
EOF

echo ""
echo "=========================================="
echo -e "${GREEN}✅ 测试完成！${NC}"
echo "=========================================="
echo ""
echo "后续操作:"
echo "  1. 查看更多分析查询: config/token_holders_analytics.sql"
echo "  2. 访问 Spark UI: http://localhost:8088"
echo "  3. 访问 MinIO Console: http://localhost:9001 (admin/password123)"
echo "  4. 连接 StarRocks: mysql -h127.0.0.1 -P9030 -uroot"
echo ""


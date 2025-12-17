#!/bin/bash
# Spark 实验环境启动脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================================="
echo "🚀 启动 Spark 实验环境"
echo "=================================================="

cd "$SPARK_DIR"

# 创建必要的目录
mkdir -p lib

# 启动服务
echo "📦 启动 Docker 服务..."
docker-compose -f docker-compose.spark-lab.yml up -d

echo ""
echo "⏳ 等待服务就绪..."
sleep 10

# 检查服务状态
echo ""
echo "📊 服务状态:"
docker-compose -f docker-compose.spark-lab.yml ps

echo ""
echo "=================================================="
echo "✅ Spark 实验环境启动完成!"
echo "=================================================="
echo ""
echo "📌 访问地址:"
echo "  - Spark Master UI:    http://localhost:8088"
echo "  - Spark Worker UI:    http://localhost:8089"
echo "  - MinIO Console:      http://localhost:9001 (admin/password123)"
echo "  - StarRocks MySQL:    mysql -h127.0.0.1 -P9030 -uroot"
echo ""
echo "📌 运行测试:"
echo "  ./scripts/run-test.sh"
echo ""
echo "📌 运行 DEX 批处理作业:"
echo "  ./scripts/run-dex-job.sh"
echo ""






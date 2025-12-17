#!/bin/bash
# Spark 实验环境停止脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================================="
echo "🛑 停止 Spark 实验环境"
echo "=================================================="

cd "$SPARK_DIR"

docker-compose -f docker-compose.spark-lab.yml down

echo ""
echo "✅ Spark 实验环境已停止"
echo ""
echo "如需清理数据卷，请运行:"
echo "  docker-compose -f docker-compose.spark-lab.yml down -v"
echo ""






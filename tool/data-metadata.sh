#!/bin/bash
# 数据元数据管理脚本入口

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 元数据管理工具
METADATA_MANAGER="$PROJECT_ROOT/automation/data/scripts/metadata_manager.py"

# 显示帮助信息
show_help() {
    cat << EOF
数据元数据管理工具

用法:
  data.sh metadata <command> [options]

命令:
  merge               合并 pending 目录的元数据片段
  list-sources        列出所有数据源
  query               查询数据集
  show                显示数据集详情
  stats               统计数据集
  stale               列出过期数据集
  cleanup             清理数据集

示例:
  # 合并元数据
  data.sh metadata merge

  # 查询 binance 数据集
  data.sh metadata query --datasource binance

  # 查看特定数据集详情
  data.sh metadata show --dataset-id "binance.spot.kline.btcusdt.5m"

  # 统计数据集
  data.sh metadata stats

  # 列出超过30天未更新的数据集
  data.sh metadata stale --days 30

  # 清理数据集
  data.sh metadata cleanup --dataset-id "xxx" --confirm

EOF
}

# 主逻辑
case "${1:-}" in
    metadata)
        shift
        python3 "$METADATA_MANAGER" "$@"
        ;;
    *)
        show_help
        ;;
esac


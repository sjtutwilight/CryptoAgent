#!/bin/bash

# DeFi聚合器Job运行脚本
# 使用方法: ./run-job.sh [job_name]
# 可选的job_name: pnl, token, pair, balance, all

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

# JVM参数
JVM_OPTS="--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED"

# 获取Job类名
get_job_class() {
    case $1 in
        "pnl")
            echo "com.twilight.aggregator.PnLAggregatorJob"
            ;;
        "token")
            echo "com.twilight.aggregator.TokenMetricAggregatorJob"
            ;;
        "pair")
            echo "com.twilight.aggregator.PairMetricJob"
            ;;
        "balance")
            echo "com.twilight.aggregator.AccountBalanceJob"
            ;;
        "trade")
            echo "com.twilight.aggregator.TradeFactJob"
            ;;
        *)
            echo ""
            ;;
    esac
}

# 显示帮助信息
show_help() {
    echo "========================================"
    echo "🚀 DeFi聚合器Job运行脚本"
    echo "========================================"
    echo ""
    echo "使用方法:"
    echo "  ./run-job.sh [job_name]"
    echo ""
    echo "可用的Job:"
    echo "  pnl      - PnL聚合器 (账户盈亏分析)"
    echo "  token    - Token指标聚合器 (Token实时指标)"
    echo "  pair     - Pair指标聚合器 (交易对分析)"
    echo "  balance  - 账户余额聚合器 (余额跟踪)"
    echo "  all      - 运行所有Job (并行)"
    echo ""
    echo "示例:"
    echo "  ./run-job.sh pnl        # 运行PnL聚合器"
    echo "  ./run-job.sh token      # 运行Token聚合器"
    echo "  ./run-job.sh all        # 运行所有Job"
    echo "  ./run-job.sh trade      # 运行交易事实聚合器"
    echo ""
    echo "注意: 脚本会自动编译项目，确保使用最新代码"
    echo "========================================"
}

# 检查Maven依赖
check_dependencies() {
    log_info "检查Maven环境..."
    
    if ! command -v mvn &> /dev/null; then
        log_error "Maven未安装或不在PATH中"
        exit 1
    fi
    
    if ! command -v java &> /dev/null; then
        log_error "Java未安装或不在PATH中"
        exit 1
    fi
    
    log_info "✅ Maven和Java环境检查通过"
}

# 编译项目
compile_project() {
    log_info "开始编译项目..."
    
    # 跳过测试编译，加快速度
    if mvn clean compile -DskipTests -q; then
        log_info "✅ 项目编译成功"
    else
        log_error "❌ 项目编译失败"
        exit 1
    fi
}

# 构建classpath
build_classpath() {
    log_debug "构建classpath..."
    
    # 获取依赖classpath
    DEPENDENCY_CLASSPATH=$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)
    
    if [ -z "$DEPENDENCY_CLASSPATH" ]; then
        log_error "无法获取Maven依赖classpath"
        exit 1
    fi
    
    # 完整classpath
    FULL_CLASSPATH="target/classes:$DEPENDENCY_CLASSPATH"
    log_debug "✅ Classpath构建完成"
}

# 运行单个Job
run_job() {
    local job_key=$1
    local job_class=$(get_job_class "$job_key")
    
    if [ -z "$job_class" ]; then
        log_error "未知的Job: $job_key"
        show_help
        exit 1
    fi
    
    log_info "🚀 启动 $job_key Job ($job_class)"
    echo "=========================================="
    
    # 运行Job
    java $JVM_OPTS -cp "$FULL_CLASSPATH" "$job_class"
}

# 并行运行所有Job
run_all_jobs() {
    log_info "🚀 并行启动所有Job..."
    
    # 创建日志目录
    mkdir -p logs
    
    # 定义所有Job
    local jobs="pnl token pair balance"
    
    # 并行启动所有Job
    for job_key in $jobs; do
        local job_class=$(get_job_class "$job_key")
        log_info "启动 $job_key Job 到后台..."
        
        nohup java $JVM_OPTS -cp "$FULL_CLASSPATH" "$job_class" \
            > "logs/${job_key}-$(date +%Y%m%d-%H%M%S).log" 2>&1 &
        
        echo $! > "logs/${job_key}.pid"
        log_info "✅ $job_key Job已启动，PID: $!"
    done
    
    log_info "=========================================="
    log_info "所有Job已启动到后台"
    log_info "日志文件位置: ./logs/"
    echo ""
    log_info "查看运行状态:"
    echo "  tail -f logs/pnl-*.log      # 查看PnL Job日志"
    echo "  tail -f logs/token-*.log    # 查看Token Job日志"
    echo "  tail -f logs/pair-*.log     # 查看Pair Job日志"
    echo "  tail -f logs/balance-*.log  # 查看Balance Job日志"
    echo ""
    echo "停止所有Job:"
    echo "  pkill -f 'twilight.aggregator'"
}

# 验证Job名称
is_valid_job() {
    local job_name=$1
    case $job_name in
        "pnl"|"token"|"pair"|"balance"|"trade")
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# 主函数
main() {
    local job_name=${1:-""}
    
    # 显示启动信息
    echo ""
    log_info "🏗️  DeFi聚合器Job运行脚本启动"
    echo "========================================"
    
    # 参数验证
    if [ -z "$job_name" ]; then
        show_help
        exit 1
    fi
    
    if [ "$job_name" = "help" ] || [ "$job_name" = "-h" ] || [ "$job_name" = "--help" ]; then
        show_help
        exit 0
    fi
    
    # 检查环境
    check_dependencies
    
    # 编译项目 
    compile_project
    
    # 构建classpath
    build_classpath
    
    echo ""
    log_info "🎯 准备启动Job: $job_name"
    echo "========================================"
    
    # 根据参数运行对应Job
    if [ "$job_name" = "all" ]; then
        run_all_jobs
    elif is_valid_job "$job_name"; then
        run_job "$job_name"
    else
        log_error "未知的Job名称: $job_name"
        echo ""
        show_help
        exit 1
    fi
}

# 执行主函数
main "$@"
#!/bin/bash

# DeFi聚合器Job运行脚本
# 使用方法: ./run-job.sh [job_name] [local|docker|cluster]
# 可选的job_name: pnl, token, pair, balance, trade, kline, multi, sequence, perp-exec, perp-context, perp-panel, all
# 可选的运行模式: local(默认), docker(独立容器), cluster(提交到Flink集群)

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

# JVM参数 - Java 17 模块访问配置
JVM_OPTS="--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED"
DOCKER_IMAGE_NAME="twilight-aggregator-job"
DOCKER_NETWORK="dataplatform_crypto-network"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Flink 集群配置
FLINK_JOBMANAGER_CONTAINER="flink-jobmanager"
FLINK_REST_URL="http://localhost:8081"
FLINK_JAR_PATH="/opt/flink/job/aggregator.jar"

is_valid_mode() {
    case $1 in
        "local"|"docker"|"cluster")
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

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
        "kline")
            echo "com.twilight.aggregator.KlineSignalJob"
            ;;
        "multi")
            echo "com.twilight.aggregator.MultiIndicatorJob"
            ;;
        "sequence")
            echo "com.twilight.aggregator.utils.SequenceExtractJob"
            ;;
        "perp-exec")
            echo "com.twilight.aggregator.PerpExecutionMetricsJob"
            ;;
        "perp-context")
            echo "com.twilight.aggregator.PerpContextMetricsJob"
            ;;
        "perp-panel")
            echo "com.twilight.aggregator.PerpPanelAggregatorJob"
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
    echo "  ./run-job.sh [job_name] [local|docker|cluster]"
    echo "  ./run-job.sh [job_name] --mode cluster"
    echo ""
    echo "可用的Job:"
    echo "  pnl         - PnL聚合器 (账户盈亏分析)"
    echo "  token       - Token指标聚合器 (Token实时指标)"
    echo "  pair        - Pair指标聚合器 (交易对分析)"
    echo "  balance     - 账户余额聚合器 (余额跟踪)"
    echo "  trade       - 交易事实聚合器 (交易事实数据)"
    echo "  kline       - K线信号生成器 (交易信号生成)"
    echo "  multi       - 多指标聚合器 (多指标分析)"
    echo "  sequence    - 区块序列提取器 (数据缺失检测)"
    echo "  perp-exec   - 永续合约执行面指标 (快流-秒级)"
    echo "  perp-context- 永续合约语境面指标 (慢流-分钟级)"
    echo "  perp-panel  - 永续合约面板汇合 (Job3-分钟级)"
    echo "  all         - 运行所有Job (并行)"
    echo ""
    echo "运行模式:"
    echo "  local   - 本地运行 (默认，使用本地JVM，连接localhost服务)"
    echo "  docker  - 独立容器运行 (MiniCluster模式，连接Docker网络服务)"
    echo "  cluster - 提交到Flink集群 (推荐，可在 http://localhost:8081 观测)"
    echo ""
    echo "示例:"
    echo "  ./run-job.sh pnl                # 本地运行PnL聚合器"
    echo "  ./run-job.sh token cluster      # 提交到Flink集群运行 (推荐)"
    echo "  ./run-job.sh token docker       # 在独立Docker容器中运行"
    echo "  ./run-job.sh all cluster        # 提交所有Job到Flink集群"
    echo ""
    echo "前置条件:"
    echo "  - 运行前请先启动Docker服务: docker compose up -d"
    echo "  - cluster模式Job可在 http://localhost:8081 查看运行状态"
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

package_project_for_docker() {
    log_info "开始为Docker模式打包项目..."
    if mvn clean package -P container -Dflink.scope=compile -DskipTests -q; then
        log_info "✅ Docker包构建成功"
    else
        log_error "❌ Docker包构建失败"
        exit 1
    fi
}

check_docker_environment() {
    log_info "检查Docker环境..."
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装或不在PATH中"
        exit 1
    fi

    if ! docker info > /dev/null 2>&1; then
        log_error "Docker daemon未运行或当前用户无权限访问"
        exit 1
    fi
    log_info "✅ Docker环境检查通过"
}

ensure_docker_network() {
    if docker network inspect "$DOCKER_NETWORK" >/dev/null 2>&1; then
        log_info "✅ Docker网络 $DOCKER_NETWORK 可用"
        return
    fi

    log_error "未找到Docker网络: $DOCKER_NETWORK"
    log_error "请先启动Docker服务: cd $PROJECT_ROOT && docker compose up -d"
    exit 1
}

build_docker_image() {
    log_info "构建Docker镜像 ${DOCKER_IMAGE_NAME}..."
    if docker build -t "${DOCKER_IMAGE_NAME}" .; then
        log_info "✅ Docker镜像构建完成"
    else
        log_error "❌ Docker镜像构建失败"
        exit 1
    fi
}

# ========== Flink Cluster 模式相关函数 ==========

# 检查 Flink 集群是否运行
check_flink_cluster() {
    log_info "检查 Flink 集群状态..."
    
    # 检查 JobManager 容器是否运行
    if ! docker ps --format '{{.Names}}' | grep -q "^${FLINK_JOBMANAGER_CONTAINER}$"; then
        log_error "Flink JobManager 容器未运行"
        log_error "请先启动Docker服务: cd $PROJECT_ROOT && docker compose up -d"
        exit 1
    fi
    
    # 等待 Flink REST API 可用
    log_info "等待 Flink REST API 就绪..."
    local max_attempts=30
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -s "${FLINK_REST_URL}/overview" >/dev/null 2>&1; then
            log_info "✅ Flink 集群已就绪"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    
    log_error "❌ Flink 集群启动超时，请检查: docker compose logs flink-jobmanager"
    exit 1
}

# 为 cluster 模式打包项目 (scope=provided，因为 Flink 集群已有依赖)
package_project_for_cluster() {
    log_info "开始为 Flink 集群模式打包项目..."
    # 使用 container profile，flink.scope=provided 避免打包 Flink 核心依赖
    if mvn clean package -P container -DskipTests -q; then
        log_info "✅ Flink 集群包构建成功"
    else
        log_error "❌ Flink 集群包构建失败"
        exit 1
    fi
}

# 复制 JAR 到 Flink JobManager 容器
copy_jar_to_flink() {
    local jar_file="target/aggregator-1.0-SNAPSHOT.jar"
    
    if [ ! -f "$jar_file" ]; then
        log_error "JAR 文件不存在: $jar_file"
        exit 1
    fi
    
    log_info "复制 JAR 到 Flink JobManager 容器..."
    
    # 在容器内创建目录
    docker exec "${FLINK_JOBMANAGER_CONTAINER}" mkdir -p /opt/flink/job
    
    # 复制 JAR 文件
    if docker cp "$jar_file" "${FLINK_JOBMANAGER_CONTAINER}:${FLINK_JAR_PATH}"; then
        log_info "✅ JAR 已复制到 ${FLINK_JOBMANAGER_CONTAINER}:${FLINK_JAR_PATH}"
    else
        log_error "❌ JAR 复制失败"
        exit 1
    fi
}

# Flink CLI 需要的 JVM 参数 (Java 17 模块访问 + 环境标识)
FLINK_CLIENT_OPTS="--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -Denv=container"

# 通过 Flink CLI 提交 Job 到集群
run_job_on_cluster() {
    local job_key=$1
    local job_class=$(get_job_class "$job_key")

    if [ -z "$job_class" ]; then
        log_error "未知的Job: $job_key"
        show_help
        exit 1
    fi

    log_info "🚀 提交 $job_key Job 到 Flink 集群 ($job_class)"
    echo "=========================================="
    
    # 使用 docker exec 在 JobManager 容器内执行 flink run
    # - FLINK_ENV_JAVA_OPTS: 传递 JVM 参数给 flink CLI (解决 Java 17 模块访问问题)
    # - APP_ENV=container: 让 Job 加载 application-container.properties 配置
    docker exec \
        -e "FLINK_ENV_JAVA_OPTS=${FLINK_CLIENT_OPTS}" \
        -e "APP_ENV=container" \
        "${FLINK_JOBMANAGER_CONTAINER}" \
        /opt/flink/bin/flink run \
        -d \
        -c "$job_class" \
        "${FLINK_JAR_PATH}"
    
    local exit_code=$?
    if [ $exit_code -eq 0 ]; then
        log_info "✅ Job 已提交到 Flink 集群"
        log_info "📊 查看 Job 状态: ${FLINK_REST_URL}"
    else
        log_error "❌ Job 提交失败 (exit code: $exit_code)"
        exit 1
    fi
}

# 提交所有 Job 到 Flink 集群
run_all_jobs_on_cluster() {
    log_info "🚀 提交所有 Job 到 Flink 集群..."
    local jobs="pnl token pair balance"
    
    for job_key in $jobs; do
        local job_class=$(get_job_class "$job_key")
        log_info "提交 $job_key Job..."
        
        docker exec \
            -e "FLINK_ENV_JAVA_OPTS=${FLINK_CLIENT_OPTS}" \
            -e "APP_ENV=container" \
            "${FLINK_JOBMANAGER_CONTAINER}" \
            /opt/flink/bin/flink run \
            -d \
            -c "$job_class" \
            "${FLINK_JAR_PATH}"
        
        if [ $? -eq 0 ]; then
            log_info "✅ $job_key Job 已提交"
        else
            log_warn "⚠️ $job_key Job 提交失败，继续下一个..."
        fi
    done

    log_info "=========================================="
    log_info "所有 Job 已提交到 Flink 集群"
    log_info "📊 查看 Job 状态: ${FLINK_REST_URL}"
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

run_job_in_docker() {
    local job_key=$1
    local job_class=$(get_job_class "$job_key")

    if [ -z "$job_class" ]; then
        log_error "未知的Job: $job_key"
        show_help
        exit 1
    fi

    local container_name="aggregator-${job_key}-$(date +%s)"
    log_info "🐳 在Docker容器中启动 $job_key Job ($job_class)，容器: ${container_name}"
    docker run --rm --network "${DOCKER_NETWORK}" --name "${container_name}" -e APP_ENV=container "${DOCKER_IMAGE_NAME}" \
        sh -c "java ${JVM_OPTS} -Denv=container -cp aggregator.jar ${job_class}"
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

run_all_jobs_in_docker() {
    log_info "🐳 在Docker中并行启动所有Job..."
    local jobs="pnl token pair balance"
    for job_key in $jobs; do
        local job_class=$(get_job_class "$job_key")
        local container_name="aggregator-${job_key}-$(date +%Y%m%d%H%M%S)"
        log_info "在Docker中后台启动 $job_key Job -> 容器: $container_name"
        docker run -d --rm --network "${DOCKER_NETWORK}" --name "${container_name}" -e APP_ENV=container "${DOCKER_IMAGE_NAME}" \
            sh -c "java ${JVM_OPTS} -Denv=container -cp aggregator.jar ${job_class}" >/dev/null
        log_info "✅ $job_key Job容器 ${container_name} 已启动"
    done

    log_info "=========================================="
    log_info "所有Job已在Docker后台运行"
    log_info "查看日志: docker logs -f <容器名>"
    log_info "停止Job: docker stop <容器名>"
}

# 验证Job名称
is_valid_job() {
    local job_name=$1
    case $job_name in
        "pnl"|"token"|"pair"|"balance"|"trade"|"kline"|"multi"|"sequence"|"perp-exec"|"perp-context"|"perp-panel")
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# 主函数
main() {
    local job_name=""
    local mode="local"
    local mode_set=false
    DOCKER_NETWORK_SET=false

    if [ $# -eq 0 ]; then
        show_help
        exit 1
    fi

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help|help)
                show_help
                exit 0
                ;;
            --mode=*)
                mode="${1#*=}"
                mode_set=true
                shift
                ;;
            --mode)
                if [ -z "${2:-}" ]; then
                    log_error "--mode 需要一个参数 (local|docker)"
                    exit 1
                fi
                mode="$2"
                mode_set=true
                shift 2
                ;;
            --docker)
                mode="docker"
                mode_set=true
                shift
                ;;
            --local)
                mode="local"
                mode_set=true
                shift
                ;;
            --cluster)
                mode="cluster"
                mode_set=true
                shift
                ;;
            *)
                if [ -z "$job_name" ]; then
                    job_name="$1"
                    shift
                elif [ "$mode_set" = false ] && [[ "$1" =~ ^(local|docker|cluster)$ ]]; then
                    mode="$1"
                    mode_set=true
                    shift
                else
                    log_error "未知的参数: $1"
                    exit 1
                fi
                ;;
        esac
    done

    mode=$(echo "$mode" | tr '[:upper:]' '[:lower:]')
    
    # 显示启动信息
    echo ""
    log_info "🏗️  DeFi聚合器Job运行脚本启动"
    echo "========================================"
    
    if [ -z "$job_name" ]; then
        show_help
        exit 1
    fi
    
    if ! is_valid_mode "$mode"; then
        log_error "未知的运行模式: $mode (仅支持 local/docker/cluster)"
        exit 1
    fi
    
    if [ "$mode" = "docker" ]; then
        check_docker_environment
        ensure_docker_network
        package_project_for_docker
        build_docker_image
    elif [ "$mode" = "cluster" ]; then
        # Flink 集群模式
        check_docker_environment
        check_flink_cluster
        package_project_for_cluster
        copy_jar_to_flink
    else
        # 本地模式
        check_dependencies
        compile_project
        build_classpath
    fi
    
    echo ""
    log_info "🎯 准备启动Job: $job_name (模式: $mode)"
    echo "========================================"
    
    # 根据参数运行对应Job
    if [ "$job_name" = "all" ]; then
        if [ "$mode" = "docker" ]; then
            run_all_jobs_in_docker
        elif [ "$mode" = "cluster" ]; then
            run_all_jobs_on_cluster
        else
            run_all_jobs
        fi
        exit 0
    fi

    if is_valid_job "$job_name"; then
        if [ "$mode" = "docker" ]; then
            run_job_in_docker "$job_name"
        elif [ "$mode" = "cluster" ]; then
            run_job_on_cluster "$job_name"
        else
            run_job "$job_name"
        fi
    else
        log_error "未知的Job名称: $job_name"
        echo ""
        show_help
        exit 1
    fi
}

# 执行主函数
main "$@"

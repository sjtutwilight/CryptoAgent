#!/usr/bin/env bash
# 运行DexSwapDwdJob的脚本
# 使用Flink本地模式运行,包含所有必需的依赖

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# 编译项目(如果需要)
if [ ! -f "target/realtime-pipeline-0.1.0-SNAPSHOT.jar" ]; then
    echo "编译项目..."
    mvn clean package -DskipTests
fi

# 获取所有依赖的classpath
CLASSPATH=$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)

# 添加编译后的jar
CLASSPATH="target/realtime-pipeline-0.1.0-SNAPSHOT.jar:$CLASSPATH"

# JVM参数
JVM_OPTS="-Xms1g -Xmx2g"
JVM_OPTS="$JVM_OPTS -XX:+UseG1GC"
JVM_OPTS="$JVM_OPTS -XX:MaxGCPauseMillis=200"

echo "启动DexSwapDwdJob..."
echo "日志输出: realtime-pipeline.log"
echo ""

# 创建日志目录
mkdir -p logs

# 运行Job (输出到日志文件)
java $JVM_OPTS \
    -cp "$CLASSPATH" \
    com.twilight.realtime.jobs.DexSwapDwdJob \
    2>&1 | tee logs/realtime-pipeline.log

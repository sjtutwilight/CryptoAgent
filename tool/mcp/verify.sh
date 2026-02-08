#!/usr/bin/env bash
# MCP 工具验证脚本

set -euo pipefail

cd "$(dirname "$0")/../.."

PYTHON_CMD="python3"
if command -v python3.11 >/dev/null 2>&1; then
    PYTHON_CMD="python3.11"
elif command -v python3.10 >/dev/null 2>&1; then
    PYTHON_CMD="python3.10"
fi

echo "=== MCP 工具验证 ==="
echo ""

echo "1. 检查 --mcp 元数据..."
python3 - <<'PY'
import json
import subprocess

scripts = {
    "tool/ops.sh": "ops_execute",
    "tool/test.sh": "test_execute",
    "tool/orchestration.sh": "orchestration_execute",
}
descriptions = set()
for script, tool_name in scripts.items():
    result = subprocess.run([script, "--mcp"], capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"❌ {script} --mcp 失败")
    meta = json.loads(result.stdout.strip() or result.stderr.strip())
    if meta.get("tool_name") != tool_name:
        raise SystemExit(f"❌ {script} tool_name 不匹配: {meta.get('tool_name')}")
    if not meta.get("description"):
        raise SystemExit(f"❌ {script} description 缺失")
    if meta.get("description") in descriptions:
        raise SystemExit(f"❌ {script} description 重复")
    descriptions.add(meta["description"])
    if not isinstance(meta.get("supports_output_json"), bool):
        raise SystemExit(f"❌ {script} supports_output_json 必须为 bool")
print("✅ --mcp 元数据正常")
PY

echo ""
echo "2. 测试命令发现..."
python3 tool/mcp/discovery.py > /dev/null
if [ $? -eq 0 ]; then
    echo "✅ 命令发现功能正常"
else
    echo "❌ 命令发现功能失败"
    exit 1
fi

echo ""
echo "3. 检查发现的命令数量..."
ops_count=$(python3 tool/mcp/discovery.py | jq '.ops.command_count')
test_count=$(python3 tool/mcp/discovery.py | jq '.test.command_count')
orch_count=$(python3 tool/mcp/discovery.py | jq '.orchestration.command_count')

echo "   - ops.sh: $ops_count 个命令"
echo "   - test.sh: $test_count 个命令"
echo "   - orchestration.sh: $orch_count 个命令"

if [ "$ops_count" -gt 15 ] && [ "$test_count" -gt 3 ] && [ "$orch_count" -gt 8 ]; then
    echo "✅ 命令数量符合预期"
else
    echo "❌ 命令数量异常"
    exit 1
fi

echo ""
echo "4. 验证 usage 格式统一性..."
for script in tool/ops.sh tool/test.sh tool/orchestration.sh; do
    if ./$script --help 2>&1 | grep -q "Commands:"; then
        echo "✅ $script 使用统一格式"
    else
        echo "❌ $script 格式不统一"
        exit 1
    fi
done

echo ""
echo "5. 检查 Python 版本..."
"$PYTHON_CMD" - <<'PY'
import sys
if sys.version_info < (3, 10):
    print("⚠️  Python 版本低于 3.10，mcp 依赖将无法安装")
else:
    print("✅ Python 版本满足要求")
PY

echo ""
echo "6. 检查 MCP 依赖..."
if "$PYTHON_CMD" -c "import mcp" 2>/dev/null; then
    echo "✅ MCP SDK 已安装"
else
    echo "⚠️  MCP SDK 未安装（运行: $PYTHON_CMD -m pip install -r tool/mcp/requirements.txt）"
fi

echo ""
echo "7. 验证文件完整性..."
files=(
    "tool/mcp/server.py"
    "tool/mcp/discovery.py"
    "tool/mcp/executor.py"
    "tool/mcp/requirements.txt"
    "tool/mcp/mcp-config.json"
    "tool/mcp/README.md"
)

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file 存在"
    else
        echo "❌ $file 缺失"
        exit 1
    fi
done

echo ""
echo "=== 验证完成 ==="
echo ""
echo "下一步："
echo "1. 安装依赖: $PYTHON_CMD -m pip install -r tool/mcp/requirements.txt"
echo "2. 配置 Cursor: 参考 tool/mcp/README.md"
echo "3. 重启 Cursor"
echo "4. 使用 MCP 工具: ops_execute, test_execute, orchestration_execute"



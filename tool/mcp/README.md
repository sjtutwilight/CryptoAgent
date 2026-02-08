# DataPlatform MCP 工具

为 DataPlatform 的入口脚本（`tool/ops.sh`, `tool/test.sh`, `tool/orchestration.sh`）提供 MCP (Model Context Protocol) 接口，使 Cursor AI 能够直接调用这些运维工具。

## 核心特性

- **低维护成本**：通过自动命令发现机制，新增命令无需修改 MCP 代码
- **统一接口**：仅 3 个 MCP tools 覆盖所有入口脚本功能
- **结构化输出**：自动处理 JSON 格式输出，便于 AI 解析

## 架构设计

```
Cursor AI
    ↓
MCP Server (server.py)
    ↓
Command Discovery (discovery.py) ← 读取 --mcp + 自动解析 usage()
    ↓
Executor (executor.py)
    ↓
Shell 入口脚本 (ops.sh / test.sh / orchestration.sh)
    ↓
实际实现 (automation/ops/* 等)
```

## 安装

**Python 版本要求**：`mcp` 依赖需要 Python 3.10+。如果系统 `python3` 不是 3.10+，请在 MCP 配置里使用 `python3.11`（或实际路径）。

### 1. 安装 Python 依赖

```bash
cd tool/mcp
pip install -r requirements.txt
```

### 2. 配置 Cursor

将以下配置添加到 Cursor 的 MCP 配置文件中（通常在 `~/.cursor/config/mcp.json`）：

```json
{
  "mcpServers": {
    "dataplatform-tools": {
      "command": "python3.11",
      "args": ["tool/mcp/server.py"],
      "cwd": "/Users/yangguang/软件项目/DataPlatform",
      "env": {},
      "description": "DataPlatform 运维工具集成",
      "enabled": true
    }
  }
}
```

**注意**：请将 `cwd` 路径修改为你的实际项目路径。

### 3. 重启 Cursor

配置完成后重启 Cursor 使 MCP 服务生效。

## 可用工具

### 1. `ops_execute` - 运维操作

调用 `tool/ops.sh` 执行运维命令。

**查看可用命令**：
```bash
./tool/ops.sh --help
```

**示例**：

```json
{
  "command": "role:alive_list",
  "args": {
    "output_json": true
  }
}
```

```json
{
  "command": "flink:run",
  "args": {
    "entry_class": "com.twilight.aggregator.KlineSignalJob"
  }
}
```

```json
{
  "command": "role:start",
  "args": {
    "role_ids": ["binance-spot-link-kline-batch"]
  }
}
```

### 2. `test_execute` - 测试场景

调用 `tool/test.sh` 执行测试场景。

**查看可用命令**：
```bash
./tool/test.sh --help
```

**示例**：

```json
{
  "command": "list"
}
```

```json
{
  "command": "scenario:run",
  "args": {
    "scenario": "binance_kline",
    "stages": "infra,ingress"
  }
}
```

```json
{
  "command": "stage:list",
  "args": {
    "scenario": "binance_perp"
  }
}
```

### 3. `orchestration_execute` - 服务编排

调用 `tool/orchestration.sh` 启动服务。

**查看可用关键词**：
```bash
./tool/orchestration.sh --help
```

**示例**：

```json
{
  "keywords": ["ingest"]
}
```

```json
{
  "keywords": ["stream", "o", "bd"]
}
```

```json
{
  "keywords": ["k", "c", "w"]
}
```

**注意**：在 Cursor 中使用 MCP 工具时，可用命令会自动显示在工具描述中。

## 参数规则

### 布尔标志

```json
{"output_json": true}
```
转换为：`--output-json`

### 普通参数

```json
{"entry_class": "com.example.Job", "jar": "/path/to/jar"}
```
转换为：`--entry-class com.example.Job --jar /path/to/jar`

### 列表参数

```json
{"role_ids": ["r1", "r2"]}
```
转换为：`--role-ids r1 --role-ids r2`

### 下划线转换

参数名中的下划线会自动转换为连字符：
- `output_json` → `--output-json`
- `entry_class` → `--entry-class`

## 开发与测试

### 测试命令发现

```bash
python3 tool/mcp/discovery.py
```

输出所有可用命令的 JSON 列表。

## MCP 元数据规范

入口脚本需支持 `--mcp` 输出 JSON 元数据：

```json
{
  "tool_name": "ops_execute",
  "description": "DataPlatform Ops Entrypoint (tool/ops.sh): roles/init/http/flink/sqlite/starrocks",
  "supports_output_json": true
}
```

约束：
- `tool_name` 固定为 `ops_execute` / `test_execute` / `orchestration_execute`
- `description` 必须全局唯一，且与 MCP 工具描述保持一致
- `supports_output_json` 为布尔值

### 测试命令执行

```bash
python3 tool/mcp/executor.py
```

执行内置的测试用例。

### 启动 MCP Server（调试模式）

```bash
python3 tool/mcp/server.py
```

### 查看 MCP Server 日志

在 Cursor 中打开开发者工具，查看 MCP 连接日志。

## 维护指南

### 添加新命令

1. 在原入口脚本中添加新命令（如在 `tool/ops.sh` 中添加 `new_domain:new_action`）
2. 在对应的 `usage()` 函数中添加命令描述
3. 实现具体功能脚本（如 `automation/ops/new_domain/new_action.py`）
4. **重启 MCP server**（无需修改 MCP 代码）

### 修改命令参数

直接修改实现脚本的参数解析逻辑，MCP 会透传参数，无需修改 MCP 代码。

### 自动 JSON 输出

MCP 执行器会自动为 `ops.sh` 和 `test.sh` 命令添加 `--output-json` 参数（如果支持）。

如需为新命令支持 JSON 输出，在实现脚本中添加 `--output-json` 参数支持即可。

## 故障排查

### MCP Server 未启动

1. 检查 Python 依赖是否安装：`pip list | grep mcp`
2. 检查 Cursor MCP 配置文件路径是否正确
3. 查看 Cursor 开发者工具中的错误日志

### 命令执行失败

1. 检查命令名称是否正确（区分大小写）
2. 检查参数格式是否正确
3. 手动运行原始脚本确认功能正常
4. 查看返回的 `error` 字段获取详细错误信息

### 新命令未显示

1. 确认在入口脚本的 `usage()` 函数中添加了命令描述
2. 重启 MCP server（命令列表在启动时缓存）
3. 使用 `python3 tool/mcp/discovery.py` 确认命令已被发现

## 文件结构

```
tool/mcp/
├── server.py           # MCP Server 主程序
├── discovery.py        # 命令自动发现模块
├── executor.py         # 通用命令执行器
├── requirements.txt    # Python 依赖
├── mcp-config.json     # Cursor 配置模板
└── README.md          # 本文档
```

## 相关文档

- [Tool 入口脚本说明](../README.md)
- [Ops 规范](../OPS.md)
- [MCP 协议文档](https://modelcontextprotocol.io/)

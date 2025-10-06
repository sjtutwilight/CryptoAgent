# SQL 智能代理系统

基于 LangGraph 和 DeepSeek 的智能 SQL 代理，能够将自然语言查询转换为 SQL 语句并执行，返回 JSON 格式的结果。

## 功能特性

- 🤖 **智能意图识别**: 自动分析用户查询意图
- 🔄 **自然语言转SQL**: 使用 DeepSeek 模型将自然语言转换为精确的 SQL 查询  
- 🗄️ **多数据库支持**: 支持 PostgreSQL 和 ClickHouse 数据库
- 📊 **结构化输出**: 返回标准化的 JSON 格式结果
- 🌐 **API接口**: 提供 RESTful API 和命令行接口
- ⚡ **高性能**: 使用 LangGraph 工作流优化查询流程

## 核心用例

### 账户详情查询
替代现有的 `getAccountDetail` 接口，通过自然语言查询获取账户完整信息：

**输入示例**:
```
"获取账户ID为1的账户详情"
"查询账户1的详细信息"
"account detail for id 1"
```

**输出结构**:
```json
{
  "status": "success",
  "data": {
    "account_info": {
      "id": 1,
      "address": "0x...",
      "entity": "交易所",
      "chain_name": "ethereum",
      "created_at": "2024-01-01 10:00:00"
    },
    "label_info": {
      "labels": ["whale", "smart"],
      "tag_bitmap": 6
    },
    "assets": [...],
    "transfer_history": [...],
    "asset_stats": {...},
    "transfer_stats": {...}
  }
}
```

## 技术架构

### 核心组件

1. **LangGraph 工作流引擎**
   - 意图分析 → SQL生成 → 查询执行 → 结果格式化
   - 错误处理和状态管理

2. **DeepSeek AI 模型**
   - 自然语言理解
   - SQL 查询生成
   - 智能错误修复

3. **数据库连接层**
   - PostgreSQL: 账户基础信息
   - ClickHouse: 资产快照和交易历史

4. **API 接口层**
   - Flask Web API
   - 命令行交互界面

### 文件结构

```
agent/
├── config.py          # 配置文件（API密钥、数据库连接等）
├── database.py        # 数据库连接和查询工具
├── sql_agent.py       # 核心 LangGraph 代理逻辑
├── agent_api.py       # Flask API 服务器
├── test_agent.py      # 测试脚本
├── requirements.txt   # Python 依赖
└── README.md         # 项目文档
```

## 安装和配置

### 1. 安装依赖

```bash
cd /Users/yangguang/DataPlatform/agent
pip install -r requirements.txt
```

### 2. 配置数据库连接

编辑 `config.py` 中的数据库配置：

```python
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "twilight",
    "user": "postgres",
    "password": "postgres"
}

CLICKHOUSE_CONFIG = {
    "host": "localhost", 
    "port": 9000,
    "database": "default",
    "user": "default",
    "password": ""
}
```

### 3. 验证 DeepSeek API

确保 `config.py` 中的 DeepSeek API 密钥有效：

```python
DEEPSEEK_CONFIG = {
    "api_key": "sk-656d1a94e7ef4335a1a0b592bdd3d5f1",
    "base_url": "https://api.deepseek.com",
    "model": "deepseek-chat"
}
```

## 使用方法

### 方式1: Web API 服务

启动 API 服务器：

```bash
python agent_api.py
```

API 端点：

- `POST /query` - 自然语言查询
- `GET /account/<id>` - 获取账户详情  
- `POST /query/sql` - 执行原始SQL
- `GET /health` - 健康检查
- `GET /database/status` - 数据库状态

**使用示例**:
```bash
# 自然语言查询
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "获取账户ID为1的账户详情"}'

# REST风格接口
curl http://localhost:5000/account/1
```

### 方式2: 命令行交互

启动命令行界面：

```bash
python agent_api.py cli
```

然后输入自然语言查询：
```
👤 请输入查询: 获取账户ID为1的账户详情
🔍 正在处理查询...
📊 查询结果:
{
  "status": "success", 
  "data": {...}
}
```

### 方式3: Python 脚本调用

```python
from sql_agent import sql_agent

result = sql_agent.process_query("获取账户ID为1的账户详情")
print(result)
```

## 测试

运行测试套件：

```bash
# 运行所有测试
python test_agent.py

# 运行特定测试
python test_agent.py db          # 数据库连接测试
python test_agent.py account     # 账户查询测试
python test_agent.py performance # 性能测试
```

## 数据库表结构

### PostgreSQL - account 表
```sql
CREATE TABLE account (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    chain_name VARCHAR(100) NOT NULL,
    address VARCHAR(128) NOT NULL,
    entity VARCHAR(255),
    tag_bitmap INTEGER NOT NULL DEFAULT 0, 
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### ClickHouse 主要表

- `ch_account_balance_snapshot`: 账户资产快照
- `ch_account_trade_fact`: 账户交易事实表

标签位图解析：
- 1 (0001): fresh (新手)
- 2 (0010): whale (巨鲸) 
- 4 (0100): smart (聪明钱)
- 8 (1000): cex (中心化交易所)

## 监控和调试

### 日志级别设置
```python
import logging
logging.basicConfig(level=logging.INFO)  # 或 DEBUG
```

### 性能监控
- 查询执行时间记录
- 数据库连接状态监控
- API 响应时间统计

### 错误处理
- 自动重试机制
- 详细错误日志
- 优雅降级策略

## 扩展功能

### 支持的查询类型

1. **账户相关**
   - 账户详情查询
   - 账户资产查询  
   - 账户交易历史

2. **代币相关**
   - 代币持有者分析
   - 代币交易统计
   - 代币价格历史

3. **统计分析**
   - 标签分布统计
   - 交易量分析
   - 活跃度指标

### 自定义查询扩展

可以通过修改 `sql_agent.py` 中的意图识别和SQL生成逻辑来支持新的查询类型。

## 注意事项

1. **API密钥安全**: 生产环境中应使用环境变量管理API密钥
2. **数据库性能**: ClickHouse查询应注意添加适当的时间范围限制
3. **错误处理**: 对于无法解析的自然语言查询，系统会返回友好的错误信息
4. **缓存策略**: 可考虑为频繁查询添加缓存层
5. **权限控制**: 生产环境需要添加适当的访问控制机制

## 版本信息

- 版本: v1.0.0
- LangGraph: 0.6.7
- DeepSeek: deepseek-chat
- Python: 3.9+
- 数据库: PostgreSQL + ClickHouse
# SQLite 元数据可视化使用指南

## 在 Cursor 中查看 SQLite 数据

由于 Cursor 没有内置的 SQL 可视化插件，我们增强了 `metadata_manager.py`，使用 `tabulate` 库在终端中显示格式化的表格，这样你就可以直接在 Cursor 的终端中查看数据，无需切换窗口。

## 安装依赖

已自动安装 `tabulate` 库用于美化表格输出：

```bash
pip3 install tabulate
```

## 使用方法

### 1. 查看表数据（格式化表格）

```bash
# 查看数据源表
./tool/ops.sh sqlite:query view datasources

# 查看数据集表
./tool/ops.sh sqlite:query view datasets --limit 10

# 查看文件表
./tool/ops.sh sqlite:query view files --limit 20
```

**输出示例**：
```
表: datasources (显示前 4 条)
====================================================================================================
+----------+-----------------+------------+-----------------------+
| id       | name            | type       | description           |
+==========+=================+============+=======================+
| binance  | Binance         | exchange   | 币安交易所数据        |
+----------+-----------------+------------+-----------------------+
| dune     | Dune Analytics  | api        | Dune Analytics API 数据 |
+----------+-----------------+------------+-----------------------+
```

### 2. 其他命令（已增强表格显示）

所有查询命令现在都使用表格格式：

```bash
# 列出数据源（表格格式）
./tool/ops.sh sqlite:query list-sources

# 查询数据集（表格格式）
./tool/ops.sh sqlite:query query --datasource binance

# 查看数据集详情（包含文件列表表格）
./tool/ops.sh sqlite:query show binance.spot.kline.btcusdt.5m
```

### 3. SQL 命令行模式

如果需要执行自定义 SQL 查询：

```bash
./tool/ops.sh sqlite:query sql
```

进入交互式 SQLite 命令行，可以执行任意 SQL：

```sql
-- 查看所有表
.tables

-- 查看表结构
.schema datasets

-- 执行查询
SELECT id, category, created_at FROM datasets;

-- 统计查询
SELECT 
  datasource_id, 
  COUNT(*) as count,
  SUM((SELECT COUNT(*) FROM files WHERE files.dataset_id = datasets.id)) as total_files
FROM datasets
GROUP BY datasource_id;
```

### 4. GUI 模式（可选）

如果想使用图形界面，可以安装 DB Browser for SQLite：

```bash
# macOS
brew install --cask db-browser-for-sqlite

# 然后启动GUI
./tool/ops.sh sqlite:query gui
```

## 可用表

| 表名 | 说明 | 主要字段 |
|------|------|---------|
| `datasources` | 数据源注册表 | id, name, type, description |
| `datasets` | 数据集表 | id, datasource_id, category, description, granularity_*, coverage, schema |
| `files` | 文件表 | id, dataset_id, path, size_bytes, record_count, checksum, time_range_* |

## 常用查询示例

### 查看所有数据集及其文件数

```bash
./tool/ops.sh sqlite:query sql
```

```sql
SELECT 
  d.id,
  d.category,
  COUNT(f.id) as file_count,
  SUM(f.size_bytes) / 1024.0 / 1024.0 as total_size_mb
FROM datasets d
LEFT JOIN files f ON d.id = f.dataset_id
GROUP BY d.id
ORDER BY total_size_mb DESC;
```

### 查找大文件

```sql
SELECT 
  path,
  size_bytes / 1024.0 / 1024.0 as size_mb,
  record_count
FROM files
ORDER BY size_bytes DESC
LIMIT 10;
```

### 按数据源统计

```bash
./tool/ops.sh sqlite:query stats
```

## 优势

✅ **无需切换窗口**: 直接在 Cursor 终端查看数据  
✅ **格式化表格**: 使用 tabulate 美化输出，易读  
✅ **快速查询**: 简单的命令即可查看数据  
✅ **SQL 支持**: 支持复杂查询  
✅ **可选 GUI**: 需要时可以使用图形界面  

## 提示

- 使用 `--limit` 参数控制显示条数，避免输出过长
- 表格会自动截断过长的字段（超过50字符）
- 如果没有安装 tabulate，会自动降级到简单文本格式
- 所有命令都支持管道操作，可以配合 `grep`、`head` 等工具使用




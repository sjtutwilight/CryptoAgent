# 本地数据文件元数据管理 (SQLite)

## 概述

使用SQLite数据库管理runtime/data下的数据文件元数据，包括数据源、数据集、文件列表等信息。

## 数据库位置

```
runtime/data/.metadata/registry.db
```

## 数据库结构

### datasources表 - 数据源
- `id`: 数据源ID（主键）
- `name`: 数据源名称
- `type`: 类型（exchange/api/warehouse/blockchain）
- `description`: 描述
- `created_at`: 创建时间

### datasets表 - 数据集
- `id`: 数据集ID（主键）
- `datasource_id`: 所属数据源
- `domain`: 数据域
- `category`: 分类
- `description`: 描述
- `granularity_*`: 粒度信息（type/interval/unit）
- `coverage`: 覆盖范围（JSON）
- `schema`: Schema定义（JSON）
- `storage_*`: 存储信息
- `role_id`: Role ID
- `tags`: 标签（JSON数组）
- `custom_meta`: 自定义元数据（JSON）
- `created_at/updated_at`: 创建/更新时间

### files表 - 文件
- `id`: 自增主键
- `dataset_id`: 所属数据集
- `path`: 文件路径（相对runtime/data）
- `size_bytes`: 文件大小
- `record_count`: 记录数
- `checksum`: 校验和
- `time_range_start/end`: 时间范围
- `created_at`: 创建时间

## 使用方法

### 1. 查看数据源
```bash
python3 automation/ops/sqlite/query.py list-sources
```

输出示例：
```
数据源列表:
--------------------------------------------------------------------------------
ID                   名称                   类型              描述                            
--------------------------------------------------------------------------------
binance              Binance              exchange        币安交易所数据                       
dune                 Dune Analytics       api             Dune Analytics API 数据         
bigquery             Google BigQuery      warehouse       BigQuery 查询结果                 
ethereum             Ethereum             blockchain      以太坊链上数据                       

数据集数量:
  binance: 1 个数据集
  dune: 1 个数据集
```

### 2. 查询数据集
```bash
# 查询所有数据集
python3 automation/ops/sqlite/query.py query

# 按数据源查询
python3 automation/ops/sqlite/query.py query --datasource binance

# 按分类查询
python3 automation/ops/sqlite/query.py query --category kline

# 按标签查询
python3 automation/ops/sqlite/query.py query --tags "binance,5m"
```

输出示例：
```
查询结果: 2 个数据集
------------------------------------------------------------------------------------------------------------------------
ID                                                 分类              文件数        大小(MB)          记录数            
------------------------------------------------------------------------------------------------------------------------
binance.spot.kline.btcusdt.5m                      kline           2          5.49            29088          
dune.token_holders.eth.link                        token_holders   3          0.79            25000          
```

### 3. 查看数据集详情
```bash
python3 automation/ops/sqlite/query.py show --dataset-id "binance.spot.kline.btcusdt.5m"
```

输出示例：
```
数据集详情: binance.spot.kline.btcusdt.5m
================================================================================
数据源: binance
领域: market_data
分类: kline
描述: Binance 现货 BTCUSDT 5分钟K线

粒度:
  类型: time_series
  间隔: 5m
  单位: minute

存储信息:
  路径: runtime/data/binance/spot/kline/btcusdt/5m
  文件数: 2
  总大小: 5.49 MB
  总记录数: 29,088

文件列表 (2 个):
----------------------------------------------------------------------------------------------------
路径                                                          大小(MB)          记录数            
----------------------------------------------------------------------------------------------------
binance/spot/kline/btcusdt/5m/kline_000.json                  2.74            28800          
binance/spot/kline/btcusdt/5m/kline_001.json                  2.74            288            
```

### 4. 统计
```bash
python3 automation/ops/sqlite/query.py stats
```

输出示例：
```
数据集统计:
================================================================================
总数据集数: 2
总文件数: 5
总大小: 1.01 GB
总记录数: 10,537,088

按数据源统计:
--------------------------------------------------------------------------------
数据源                  数据集数          文件数             大小(GB)          记录数            
--------------------------------------------------------------------------------
binance              1               2               0.01            29,088         
dune                 1               3               0.00            25,000         
```

### 5. 查找过期数据集
```bash
# 查找超过30天未更新的数据集
python3 automation/ops/sqlite/query.py stale --days 30
```

### 6. 清理数据集
```bash
# 删除数据集元数据（保留实际文件）
python3 automation/ops/sqlite/query.py cleanup --dataset-id "xxx" --confirm

# 删除数据集元数据和实际文件
python3 automation/ops/sqlite/query.py cleanup --dataset-id "xxx" --delete-files --confirm
```

## Worker集成

### File Sink配置

在Role配置的sink部分启用元数据追加：

```yaml
sink:
  type: "file"
  with:
    output_dir: "runtime/data/binance/spot/kline/btcusdt/5m"
    output_format: "json"
    filename_prefix: "kline"
    max_records_per_file: 10000
    
    # 元数据配置
    metadata:
      enabled: true
      dataset_id: "binance.spot.kline.btcusdt.5m"
      datasource: "binance"
      domain: "market_data"
      category: "kline"
      description: "Binance 现货 BTCUSDT 5分钟K线"
      
      granularity:
        type: "time_series"
        interval: "5m"
        unit: "minute"
      
      coverage:
        time_range:
          start: "2024-01-01T00:00:00Z"
          end: "2024-12-31T23:59:59Z"
        symbols:
          - "BTCUSDT"
      
      schema:
        format: "json"
        fields:
          - name: "timestamp"
            type: "int64"
            description: "K线开始时间戳(ms)"
          - name: "open"
            type: "float64"
          - name: "开盘价"
      
      tags:
        - "binance"
        - "spot"
        - "kline"
        - "btc"
        - "5m"
      
      custom_meta:
        role_id: "binance-spot-kline-btcusdt"
        calculate_checksum: true
```

### 工作流程

1. **File Sink写入数据**: 正常写入文件到output_dir
2. **记录文件元数据**: 每次文件轮换时记录文件信息
3. **Close时导入SQLite**: Role关闭时，生成临时JSON文件并调用metadata_manager.py ingest命令
4. **自动更新数据库**: 元数据自动插入或更新到SQLite数据库

### 元数据追加示例

File Sink关闭时会自动执行：

```bash
# 内部调用（用户无需手动执行）
python3 automation/ops/sqlite/query.py ingest --json-file /tmp/metadata-binance-123456.json
```

## 与YAML版本的区别

| 特性 | SQLite版 | YAML版 |
|------|---------|--------|
| 存储方式 | SQLite数据库 | YAML文件 |
| 查询性能 | 高（索引支持） | 低（全文件扫描） |
| 并发安全 | 是 | 否 |
| 数据完整性 | 外键约束保证 | 无约束 |
| 合并操作 | 自动（UPSERT） | 手动merge命令 |
| 备份 | 复制.db文件 | 复制.yaml文件 |
| 文件大小 | 小（压缩存储） | 大（文本格式） |

## 数据库维护

### 备份
```bash
cp runtime/data/.metadata/registry.db runtime/data/.metadata/backup/registry-$(date +%Y%m%d).db
```

### 查看数据库
```bash
sqlite3 runtime/data/.metadata/registry.db

# SQL示例
sqlite> SELECT * FROM datasources;
sqlite> SELECT id, category, created_at FROM datasets;
sqlite> SELECT COUNT(*) FROM files;
```

### 重建数据库
```bash
# 删除旧数据库
rm runtime/data/.metadata/registry.db

# 重新运行会自动创建
python3 automation/ops/sqlite/query.py list-sources
```

## 最佳实践

1. **定期备份**: 每天备份registry.db
2. **及时更新**: Role配置中启用metadata，确保元数据实时更新
3. **定期清理**: 使用stale命令查找过期数据集并清理
4. **标签规范**: 使用统一的标签命名规范，便于查询
5. **监控大小**: 定期检查数据库大小，必要时清理历史数据

## 故障排查

### 元数据未生成
- 检查sink配置中metadata.enabled是否为true
- 检查Role是否正常关闭（优雅退出）
- 查看Worker日志中是否有元数据写入错误

### 数据库锁定
- SQLite默认支持并发读，但写操作会锁定
- 如遇锁定，等待几秒后重试
- 考虑使用WAL模式提升并发性能

### 查询慢
- 检查是否有大量文件记录
- 考虑添加更多索引
- 定期VACUUM清理碎片

## 迁移指南（从YAML到SQLite）

如果之前使用YAML版本，可以通过以下方式迁移：

```python
# 迁移脚本示例
import yaml
import json
import sys
from pathlib import Path

# 读取旧的registry.yaml
with open('runtime/data/.metadata/registry.yaml') as f:
    data = yaml.safe_load(f)

# 导入每个数据集
for dataset in data.get('datasets', []):
    # 生成临时JSON文件
    temp_file = f'/tmp/migrate-{dataset["id"]}.json'
    with open(temp_file, 'w') as f:
        json.dump(dataset, f)
    
    # 调用ingest命令
    os.system(f'python3 automation/ops/sqlite/query.py ingest --json-file {temp_file}')
    os.remove(temp_file)
```

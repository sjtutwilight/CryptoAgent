# 本地数据文件元数据管理

## 目录结构

```
runtime/data/
├── .metadata/
│   ├── README.md           # 本文档
│   ├── registry.yaml       # 主元数据注册表（合并后）
│   ├── pending/            # 待合并的元数据片段
│   │   ├── binance-spot-kline-20241227.yaml
│   │   └── dune-token-holders-20241227.yaml
│   └── archive/            # 历史版本归档
│       └── registry-20241220.yaml
├── binance/                # Binance 数据
├── dune/                   # Dune 数据
└── bigquery/               # BigQuery 数据
```

## 工作流程

### 1. 自动追加元数据

当 Worker 的 `file` sink 写入文件时，会自动生成元数据片段到 `pending/` 目录：

```yaml
# pending/binance-spot-kline-20241227-123456.yaml
id: "binance.spot.kline.btcusdt.5m"
datasource: "binance"
category: "kline"
files:
  - path: "binance/spot/kline/btcusdt/5m/kline_20241227.json"
    size_bytes: 2876416
    record_count: 28800
    created_at: "2024-12-27T12:34:56Z"
```

### 2. 手动合并元数据

定期运行脚本将 pending 目录的元数据合并到主注册表：

```bash
# 合并所有待处理的元数据
python3 automation/data/scripts/metadata_manager.py merge

# 合并时自动去重、聚合统计
# 完成后将 pending/ 文件移动到 archive/
```

### 3. 查询元数据

```bash
# 查看所有数据源
python3 automation/data/scripts/metadata_manager.py list-sources

# 查询特定数据源的数据集
python3 automation/data/scripts/metadata_manager.py query --datasource binance

# 查询特定数据集详情
python3 automation/data/scripts/metadata_manager.py show --dataset-id "binance.spot.kline.btcusdt.5m"

# 统计数据集大小
python3 automation/data/scripts/metadata_manager.py stats
```

### 4. 清理过期数据

```bash
# 列出超过30天未更新的数据集
python3 automation/data/scripts/metadata_manager.py stale --days 30

# 删除过期数据集（包括文件和元数据）
python3 automation/data/scripts/metadata_manager.py cleanup --dataset-id "xxx" --confirm
```

## 元数据结构说明

### Dataset 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 数据集唯一标识，格式: `{datasource}.{domain}.{category}.{detail}` |
| `datasource` | string | 数据源名称（binance/dune/bigquery等） |
| `domain` | string | 数据域（market_data/onchain_data等） |
| `category` | string | 数据类别（kline/orderbook/token_holders等） |
| `description` | string | 数据集描述 |
| `granularity` | object | 数据粒度信息 |
| `coverage` | object | 数据范围（时间、符号等） |
| `schema` | object | 数据Schema定义 |
| `storage` | object | 存储信息（路径、文件数、总大小等） |
| `files` | array | 文件清单 |
| `metadata` | object | 元信息（创建者、版本、标签等） |

### Granularity 类型

- `time_series`: 时间序列数据（interval: 1m/5m/1h/1d）
- `snapshot`: 快照数据（interval: daily/weekly）
- `event`: 事件数据（interval: realtime）
- `batch`: 批量数据（interval: adhoc）

## 配置 Worker 自动追加元数据

在 Role 配置中启用元数据追加：

```yaml
sink:
  type: "file"
  with:
    output_dir: "runtime/data/binance/spot/kline/btcusdt/5m"
    output_format: "json"
    
    # 启用元数据追加
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
      tags: ["binance", "spot", "kline", "btc"]
```

## 最佳实践

1. **及时合并**: 每天定时运行合并脚本，避免 pending 目录堆积
2. **定期备份**: 合并前自动备份 registry.yaml 到 archive/
3. **数据集命名**: 使用统一的 ID 命名规范，便于查询和管理
4. **清理策略**: 定期清理过期数据，释放存储空间
5. **版本管理**: 重要数据集更新时记录版本号

## 示例场景

### 场景1: Binance K线数据接入

1. Role 运行，写入文件到 `runtime/data/binance/spot/kline/btcusdt/5m/`
2. 自动生成元数据片段到 `pending/binance-spot-kline-{timestamp}.yaml`
3. 定时任务每小时运行 `merge` 命令
4. 元数据合并到 `registry.yaml`，pending 文件归档

### 场景2: Dune 批量拉取

1. Kafka 触发批量任务
2. BatchFile Caller 拉取数据，生成 manifest.json
3. 同时生成元数据片段，包含 manifest 路径
4. 合并时自动读取 manifest 补充文件详情

### 场景3: 数据集查询与清理

1. 查询某个 token 的所有数据集: `query --tags token,link`
2. 查看数据集详情，包括文件列表、大小、记录数
3. 发现过期数据集: `stale --days 30`
4. 执行清理: `cleanup --dataset-id xxx --confirm`


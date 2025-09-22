# Aggregator 项目状态

## 项目概述
链聚合器 - Flink流处理作业，负责从Kafka消费DEX交易数据，进行窗口聚合计算，并将结果写入存储系统。

## 最新更新 (ClickHouse迁移)

### ✅ 已完成的迁移任务

#### 1. 存储层迁移
- **从PostgreSQL迁移到ClickHouse**
- 创建了新的ClickHouse表结构：
  - `token_recent_metric_ch` - Token滑动窗口指标
  - `token_rolling_metric_ch` - Token滚动窗口指标  
  - `twswap_pair_metric_ch` - Pair聚合指标
- 优化了表结构以支持列存储和时序数据特性

#### 2. Sink层重构
- **新增 `ClickHouseSink.java`**
- 实现批量写入优化（1000条/批次或5秒间隔）
- 支持重试机制和错误处理
- 替换了原有的 `PostgresSink`

#### 3. 主作业更新
- **更新 `AggregatorJob.java`**
- 将所有sink改为ClickHouse写入
- 保持了原有的数据处理逻辑

#### 4. 依赖管理
- **pom.xml 已包含ClickHouse依赖**
- ClickHouse JDBC驱动：`clickhouse-jdbc:0.4.6`
- Flink ClickHouse连接器：`flink-connector-clickhouse:1.0.0-1.20`

#### 5. 基础设施配置
- **docker-compose.yml 优化**
- 更新ClickHouse镜像为官方版本
- 添加初始化SQL脚本挂载
- 增加文件句柄限制配置

#### 6. 数据库初始化
- **创建 `clickhouse-init.sql`**
- 包含完整的表创建语句
- 优化的分区策略（按月分区）
- TTL配置（90天数据保留）
- Projections优化查询性能

### 🎯 迁移收益

#### 性能提升
- **查询性能**：预计聚合查询提升10-50倍
- **写入性能**：批量写入性能大幅提升
- **存储效率**：列存储压缩率3-10倍

#### 扩展性增强
- **时序数据优化**：原生支持时间分区和TTL
- **并发处理能力**：更好的高并发写入支持
- **资源利用率**：列存储减少磁盘I/O

#### 运维简化
- **自动分区管理**：按月自动分区
- **自动数据清理**：TTL自动删除过期数据
- **查询优化**：Projections自动优化常见查询

### 📊 表结构对比

| PostgreSQL表 | ClickHouse表 | 主要优化 |
|--------------|-------------|---------|
| `token_recent_metric` | `token_recent_metric_ch` | 列存储、分区、TTL |
| `token_rolling_metric` | `token_rolling_metric_ch` | 列存储、分区、TTL |
| `twswap_pair_metric` | `twswap_pair_metric_ch` | 列存储、分区、TTL |

### 🔧 技术细节

#### ClickHouse表设计特点
1. **引擎选择**：MergeTree引擎，支持高效插入和查询
2. **分区策略**：按`toYYYYMM(end_time)`月度分区
3. **排序键**：根据查询模式优化的ORDER BY设计
4. **数据类型优化**：
   - `LowCardinality(String)` 用于枚举字段
   - `Decimal(24,4)` 用于金额字段
   - `UInt64/UInt32` 用于ID和计数字段

#### 批量写入优化
- **批次大小**：1000条记录
- **时间间隔**：5秒强制刷新
- **重试机制**：最多3次重试，指数退避
- **错误处理**：详细的异常日志和监控

### 🚀 部署说明

#### 启动服务
```bash
# 启动ClickHouse服务
docker-compose --profile flink up clickhouse

# 验证表创建
docker exec -it clickhouse clickhouse-client --query "SHOW TABLES"

# 启动完整服务栈
docker-compose --profile flink up
```

#### 验证数据
```sql
-- 检查数据写入
SELECT COUNT(*) FROM token_recent_metric_ch;

-- 查看最新数据
SELECT * FROM token_recent_metric_ch 
ORDER BY end_time DESC LIMIT 10;
```

### 📈 监控指标

#### 关键监控点
1. **写入性能**：批次大小、写入延迟、失败率
2. **查询性能**：查询响应时间、资源使用率
3. **存储使用**：磁盘空间、压缩率、分区状态
4. **数据质量**：数据完整性、时效性

#### 告警阈值
- 写入延迟 > 10秒
- 查询响应时间 > 5秒
- 磁盘使用率 > 80%
- 数据丢失 > 0

### 🔄 回滚方案

如需回滚到PostgreSQL：
1. 修改 `AggregatorJob.java` 中的import和sink配置
2. 重新编译和部署应用
3. 恢复PostgreSQL连接配置

### 📋 后续优化计划

#### 短期优化
- [ ] 添加ClickHouse连接池配置
- [ ] 实现数据质量监控
- [ ] 优化批量写入策略

#### 中期优化  
- [ ] 实现读写分离
- [ ] 添加物化视图
- [ ] 实现数据压缩策略

#### 长期优化
- [ ] 集群化部署
- [ ] 实现数据分级存储
- [ ] 集成流式物化视图

## 原有架构说明

### 数据流架构
```
Kafka(dex_transaction) -> Flink作业 -> ClickHouse
                            ↓
                       Redis(价格缓存)
                            ↓  
                    PostgreSQL(元数据)
```

### 核心组件

#### 流处理算子
- **EventExtractor**: 从KafkaMessage中提取事件
- **EventEnrichmentProcessor**: 使用broadcast state增强事件数据
- **AsyncPriceLookupFunction**: 异步查询Redis价格数据
- **EventSplitProcessor**: 将事件分流为Token和Pair流
- **TokenWindowManager**: Token窗口聚合管理
- **PairWindowManager**: Pair窗口聚合管理（暂时注释）

#### 窗口聚合
- **滑动窗口**: 20s步长，支持多个窗口大小(1min, 5min, 1h)
- **滚动窗口**: 按时间窗口滚动聚合（暂时注释）
- **层级聚合**: 基于小时间粒度聚合高层级窗口

### 已知问题
- Pair窗口聚合暂时注释掉（需要调试）
- Token滚动指标暂时注释掉（需要调试）
- 仅Token滑动窗口指标正常运行

### 配置说明
- 开发环境：本地PostgreSQL + Redis
- 生产环境：容器化PostgreSQL + Redis
- Flink检查点：10秒间隔
- 水印策略：5秒乱序容忍



# Aggregator 设计文档

## 模块定位
流处理聚合层，基于Flink实现的实时数据聚合与指标计算。

## 核心功能
- 链上数据处理：PnL聚合、Token指标、账户余额追踪
- K线分析：技术指标计算（MACD/RSI/BOLL/ATR等）、信号生成
- 永续合约分析：执行面、语境面、面板指标、拥挤度计算

## 技术栈
- Apache Flink
- Kafka（数据源与输出）
- ClickHouse（指标存储）
- Redis（元数据与价格广播）

## 数据流
```
Kafka → 标准化算子层 → 业务处理层 → ClickHouse/Kafka
         ↓
    (Filter/Enrich/Broadcast)
         ↓
    (PnL/Token/Perp/Kline Jobs)
```

## 关键设计
- 标准化算子层：UnifiedFilter、EventEnrichment、RedisTokenMetricsBroadcaster
- 极小状态设计：PnL仅6字段，支持百万级账户
- 层级窗口聚合：20s→1min→5min→1h，避免重复计算
- 双流对齐：快照+增量协同，保证数据准确性

## 详细说明
参见 `agent/PROJECT_STATUS.md` 获取各Job的完整状态与使用说明。

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架


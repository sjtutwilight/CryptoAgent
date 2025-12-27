# Aggregator

## 模块角色
流处理聚合层，从 Kafka 消费链上交易、K线、永续合约数据，进行实时指标计算与聚合，输出到 ClickHouse。

## 目录 & 关键文件索引
- `src/main/java/com/twilight/aggregator/`
  - `PnLAggregatorJob.java` – 账户盈亏聚合 Job 入口
  - `TokenMetricAggregatorJob.java` – Token 指标聚合 Job 入口
  - `AccountBalanceJob.java` – 账户余额追踪 Job 入口
  - `MultiIndicatorJob.java` – K线技术指标计算 Job 入口
  - `PerpExecutionMetricsJob.java` – 永续执行面指标 Job 入口
  - `PerpContextMetricsJob.java` – 永续语境面指标 Job 入口
  - `PerpPanelAggregatorJob.java` – 永续面板汇合 Job 入口
  - `process/common/` – 标准化算子层（Filter/Enrich/Broadcast）
  - `process/pnl/PnLProcessor.java` – 移动平均成本算法核心
  - `process/token/TokenSlidingHierarchicalAggregator.java` – 层级窗口聚合
  - `process/balance/DualStreamAligner.java` – 快照+增量双流对齐
  - `process/kline/indicators/` – 技术指标算子（MACD/RSI/BOLL/ATR）
  - `process/perp/` – 永续合约指标处理器（OFI/拥挤度/流动性分类）
  - `sink/ClickHouseSink.java` – ClickHouse 异步批量写入

## 主要逻辑
```
Kafka Source → UnifiedFilterOperator（事件过滤）
            → EventEnrichmentMap（元数据增强）
            → RedisTokenMetricsBroadcaster（价格广播）
            → 业务算子（PnL/Token/Balance/Kline/Perp）
            → ClickHouseSink（批量写入）
```
- **标准化算子层**：所有 Job 共享统一前置处理流
- **业务算子**：各 Job 独立实现领域逻辑（KeyBy + ProcessFunction/WindowFunction）
- **Sink 层**：统一异步批量写入，支持重试与背压

## 对外接口 / CLI / 配置项
启动方式（通过 scripts 脚本）:
```bash
# 编译（跳过测试）
./scripts/dev.sh aggregator build

# 本地运行 PnL Job
./scripts/dev.sh aggregator run --job=pnl --mode=local

# Docker 运行 Token Job
./scripts/dev.sh aggregator run --job=token --mode=docker
```
支持的 job 参数: `pnl` / `token` / `balance` / `kline` / `perp-exec` / `perp-ctx` / `perp-panel`




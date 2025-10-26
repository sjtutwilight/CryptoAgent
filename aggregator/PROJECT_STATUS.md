# Aggregator 项目状态

## 项目概述
链聚合器 - Flink流处理作业，负责从Kafka消费DEX交易数据和K线数据，进行实时聚合计算和信号生成，并将结果写入存储系统。

## 核心Job清单

| Job名称 | 功能 | 输出 |
|---------|------|--------|
| **TradeFactJob** | 交易事实表处理 | ClickHouse: `ch_account_trade_fact` |
| **PnLAggregatorJob** | 账户盈亏聚合 | ClickHouse: `ch_account_pnl_current_ma`<br>`ch_pnl_realized_event` |
| **TokenMetricAggregatorJob** | Token指标聚合 | ClickHouse: `token_recent_metric_ch` |
| **AccountBalanceJob** | 账户余额处理 | ClickHouse: `ch_account_balance_snapshot` |
| **KlineSignalJob** | K线信号生成(交易所数据) | Kafka: `kline.signal` |

---

## 最新更新 (K线信号生成Job - 2025-10-23)

### ✅ 已完成的开发任务

#### 1. K线数据模型
- **`KlineData.java`**: K线数据模型
  - 支持交易所、交易对、时间间隔等基础信息
  - 包含OHLC价格、成交量、成交笔数等详细数据
  - K线状态标识(已完成/未完成)
  
- **`KlineSignal.java`**: 交易信号模型
  - 信号类型：BUY(买入)、SELL(卖出)、HOLD(持有)
  - 信号强度：0.0-1.0表示置信度
  - 包含策略参数和详细说明

#### 2. 序列化组件
- **`KlineDataDeserializer.java`**: K线数据反序列化器
  - 从Kafka消费JSON格式的K线数据
  - 安全的字段解析和类型转换
  - 异常处理和日志记录

- **`KlineSignalSerializer.java`**: 信号序列化器
  - 将KlineSignal对象序列化为JSON
  - 输出到Kafka topic: kline.signal

#### 3. 策略处理器
- **`MultipleMAProcessor.java`**: 多重移动平均策略处理器
  - 有状态处理：为每个交易对维护价格队列
  - 计算三条移动平均线(MA5/MA10/MA20)
  - 检测金叉/死叉信号
  - 评估信号强度

#### 4. 主作业
- **`KlineSignalJob.java`**: K线信号生成主作业
  - Kafka源配置：消费binance.kline
  - 按交易对分组处理
  - Kafka输出配置：发送到kline.signal

### 🎯 功能特点

#### 数据流架构
```
Kafka Source (binance.kline) 
  → KeyBy (symbol + interval)
  → MultipleMAProcessor (有状态处理)
  → Kafka Sink (kline.signal)
```

#### 多重移动平均策略
- **短期MA**: 5周期
- **中期MA**: 10周期  
- **长期MA**: 20周期

**买入信号规则**:
1. 短期MA上穿中期MA (金叉)
2. 中期MA在长期MA之上 (多头趋势确认)
3. 三线多头排列时持续生成买入信号

**卖出信号规则**:
1. 短期MA下穿中期MA (死叉)
2. 中期MA下穿长期MA (主趋势转空)
3. 空头排列时持续生成卖出信号

#### 信号强度计算
- 基于MA之间的距离计算
- 距离越大，信号越强
- 强度范围: 0.3 - 1.0
- 不同信号类型有权重调整

#### 状态管理
- 为每个symbol+interval维护独立状态
- 价格队列最大长度: 20 (长期MA周期)
- 记录上一次MA值用于判断交叉
- 支持断点恢复(Flink State Backend)

### 📊 输入输出格式

#### 输入: binance.kline
```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "interval": "1m",
  "eventTime": 1700000000000,
  "kline": {
    "startTime": 1700000000000,
    "closeTime": 1700000059999,
    "openPrice": "42000.12000000",
    "closePrice": "42010.56000000",
    "highPrice": "42080.00000000",
    "lowPrice": "41980.01000000",
    "baseVolume": "120.34560000",
    "quoteVolume": "5040000.12345600",
    "tradeCount": 1234,
    "closed": true
  },
  "ingestTime": 1700000060123
}
```

#### 输出: kline.signal
```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "interval": "1m",
  "strategy": "MultipleMA",
  "signalType": "BUY",
  "signalStrength": 0.75,
  "currentPrice": "42010.56",
  "klineTimestamp": 1700000000000,
  "signalTimestamp": 1700000060500,
  "strategyParams": {
    "ma_short": 5,
    "ma_medium": 10,
    "ma_long": 20,
    "ma_short_value": 42015.23,
    "ma_medium_value": 42005.12,
    "ma_long_value": 41990.45
  },
  "signalDetail": "短期MA(42015.23)上穿中期MA(42005.12)，中期MA高于长期MA(41990.45)，多头排列"
}
```

### 🔧 配置说明

#### Kafka配置
- **源Topic**: `binance.kline`
- **目标Topic**: `kline.signal`
- **消费者组**: `{groupId}-kline-signal`
- **起始位置**: latest (最新消息)

#### 水印策略
- **乱序容忍**: 10秒
- **空闲超时**: 1分钟
- **时间戳**: 使用K线开始时间作为事件时间

#### 并行度
- 使用全局配置的并行度
- 支持按交易对自动分组并行处理

### 🚀 部署说明

#### 编译
```bash
cd aggregator
mvn clean package -Pcontainer
```

#### 运行
```bash
# 使用run-job.sh脚本
./run-job.sh kline

# 或直接使用Flink命令
flink run -c com.twilight.aggregator.KlineSignalJob \
  target/aggregator-1.0-SNAPSHOT.jar
```

#### 监控
- 检查Flink Web UI: http://localhost:8081
- 查看Job运行状态和指标
- 监控Kafka消费lag

### 📈 性能指标 (预估)

| 指标 | 数值 | 说明 |
|------|------|------|
| **吞吐量** | 1,000+ signals/s | 单Job处理能力 |
| **端到端延迟** | < 1s | Kafka → 信号生成 → Kafka |
| **状态大小** | ~1KB/交易对 | 价格队列+MA值 |
| **并行度** | 4-8 | 可根据负载调整 |

---

## 原有架构说明 (DEX数据处理)

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
- **PairWindowManager**: Pair窗口聚合管理

#### 窗口聚合
- **滑动窗口**: 20s步长，支持多个窗口大小(1min, 5min, 1h)
- **滚动窗口**: 按时间窗口滚动聚合
- **层级聚合**: 基于小时间粒度聚合高层级窗口

### 配置说明
- 开发环境：本地PostgreSQL + Redis
- 生产环境：容器化PostgreSQL + Redis  
- Flink检查点：10秒间隔
- 水印策略：5秒乱序容忍

---

## 📋 后续优化计划

### K线信号Job优化
- [ ] **短期**
  - [ ] 添加MACD策略
  - [ ] 添加RSI策略
  - [ ] 添加布林带策略
  - [ ] 策略参数可配置化

- [ ] **中期**
  - [ ] 多策略组合信号
  - [ ] 信号回测框架
  - [ ] 信号质量评估
  - [ ] 实时绩效监控

- [ ] **长期**
  - [ ] 机器学习策略集成
  - [ ] 自适应参数优化
  - [ ] 多时间周期联合分析
  - [ ] 风险控制模块

### 通用优化
- [ ] **短期**
  - [ ] 添加ClickHouse连接池配置
  - [ ] 实现数据质量监控
  - [ ] 优化批量写入策略

- [ ] **中期**  
  - [ ] 实现读写分离
  - [ ] 添加物化视图
  - [ ] 实现数据压缩策略

- [ ] **长期**
  - [ ] 集群化部署
  - [ ] 实现数据分级存储
  - [ ] 集成流式物化视图

---

**文档版本**: v1.1  
**最后更新**: 2025-10-23  
**维护团队**: Twilight Platform


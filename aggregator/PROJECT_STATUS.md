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
| **PerpExecutionMetricsJob** | 永续合约执行面指标(快流-秒级) | Kafka: `perp.exec.1s`<br>ClickHouse: `dws_exec_1s`, `perp_signals` |
| **PerpContextMetricsJob** | 永续合约语境面指标(慢流-分钟级) | Kafka: `perp.ctx.1m`<br>ClickHouse: `dws_perps_ctx_1m` |
| **PerpPanelAggregatorJob** | 永续合约面板汇合(Job3-分钟级) | Kafka: `perp.signals`<br>ClickHouse: `dws_perps_panel_1m`, `perp_signals` |

---

## 最新更新 (永续合约双写架构+Job3汇合层 - 2025-11-05)

### ✅ 已完成的开发任务

#### 核心特性（基于GPT优化方案）
- **双写架构**：Job1/Job2同时写Kafka（低延迟给Job3）和ClickHouse（历史查询）
- **Job3汇合层**：从Kafka读取Job1/Job2输出，实现执行面+语境面join
- **T-Digest统计**：使用T-Digest算法计算Z-score，24小时滚动窗口，内存友好
- **固定阈值分类**：流动性制度（THICK/NORMAL/THIN）和信号检测（初版）
- **多交易所兼容**：统一topic，通过exchange字段区分（binance, hyperliquid）

#### 既有特性（Job1/Job2）
- **L1版OFI**：严格按照Kyle/Cont-Kukanov-Stoikov定义实现
- **分钟快照器**：使用定时器而非窗口，解决慢流数据更新频率不一致问题
- **在线EMA**：单值状态计算24h funding EMA，适应不规则更新频率
- **OI前值填充**：标记is_oi_carried，处理5分钟更新间隙
- **Top-N订单簿**：限制200档，避免内存爆炸

#### 快流Job（PerpExecutionMetricsJob）
**数据流**：
```
OrderBook → OrderBookProcessor → 1s Window → OrderBookSummary
Trades → TradesAggregator → 1s Window → TradesSummary
Connect → ExecutionMetricsBuilder → ExecutionMetrics
  ├─→ ClickHouse (dws_exec_1s)
  └─→ SignalDetector → ClickHouse (perp_signals)
```

**关键指标**：
- 订单簿：spread_bps, depth_10k/50k/100k, imbalance_top5, impact_10k/50k/100k
- OFI：L1版订单流不平衡（含指示函数）
- 成交：volume_usd, vwap, buy_volume_usd, sell_volume_usd, trade_count

**配置**：
- 并行度：12，Checkpoint：5s
- 水印：300ms乱序容忍，60s空闲超时
- 性能：10k events/s，p95延迟 < 1s

#### 慢流Job（PerpContextMetricsJob）
**数据流（GPT P0修复）**：
```
Mark/Index/Funding/OI → ContextSnapshotTimer (ValueState)
  → 定时器触发（每分钟整点）
  → ContextMetrics (在线EMA + 前值填充)
  → ClickHouse (dws_perps_ctx_1m)
```

**关键指标**：
- 价格：mark_price, index_price, basis_bps
- 资金费率：funding_rate, funding_ema_24h（在线EMA）
- 持仓量：oi_usd, oi_delta_1m, oi_delta_pct, is_oi_carried

**配置**：
- 并行度：4，Checkpoint：10s
- 水印：5s乱序容忍，2min空闲超时
- 性能：1k events/s，p95延迟 < 5s

#### Job3 汇合层（PerpPanelAggregatorJob） - 新增
**数据流（GPT优化）**：
```
ExecutionMetrics(1s) → 1min Rollup → ExecMetrics(1min)
ContextMetrics(1min) → CtxMetrics(1min)
  → PanelJoiner (CoProcessFunction)
  → LiquidityRegimeClassifier (THICK/NORMAL/THIN)
  → CrowdingScoreCalculator (T-Digest Z-score)
  → TrendSignalDetector
  ├─→ ClickHouse (dws_perps_panel_1m)
  └─→ Kafka (perp.signals) + ClickHouse (perp_signals)
```

**关键指标**：
- 执行面聚合：avg_spread_bps, max_spread_bps, avg_depth_50k, sum_ofi, volume_usd
- 语境面：mark_price, basis_bps, funding_ema_24h, oi_delta_1m
- 衍生指标：liquidity_regime, crowding_score

**配置**：
- 并行度：6，Checkpoint：10s
- 水印：Exec 2s，Ctx 5s
- 性能：500 panels/s，p95延迟 < 10s

#### Kafka Topics（新增）
- **perp.exec.1s**：Job1输出，Job3输入（执行面1秒指标）
- **perp.ctx.1m**：Job2输出，Job3输入（语境面1分钟指标）
- **perp.signals**：Job1/Job3输出（信号实时流）

#### ClickHouse表（4个）
- **dws_exec_1s**：执行面秒级表（TTL 7天）
- **dws_perps_ctx_1m**：语境面分钟级表（TTL 30天）
- **dws_perps_panel_1m**：汇合面板表（Job3输出，TTL 90天）
- **perp_signals**：信号表（Job1/Job3输出，TTL 30天）

### 🚀 使用方法

#### 1. 启用数据源
在`datainjector/worker/configs/config.yaml`中启用永续合约数据流：
```yaml
roles:
  - role_id: "hyperliquid-perp-orderbook"  # 已启用
    # ...
    sink:
      topic: "perp.orderbook"  # 多交易所共用topic
  
  - role_id: "hyperliquid-perp-trades"  # 已启用
    sink:
      topic: "perp.trades"
  
  - role_id: "hyperliquid-perp-asset-ctx"  # 已启用
    # outputs: mark_index, funding_rate, open_interest
```

#### 2. 建表
```bash
clickhouse-client < runtime/scripts/clickhouse/clickhouse-init.sql
```

#### 3. 运行Job（按顺序）
```bash
# 编译
cd aggregator && mvn clean compile -DskipTests

# Job1: 快流（执行面，秒级）
./run-job.sh perp-exec

# Job2: 慢流（语境面，分钟级）
./run-job.sh perp-context

# Job3: 汇合层（面板+信号，分钟级）- 新增
./run-job.sh perp-panel
```

### ⚠️ 已知限制

#### P0（影响功能）
- OFI传递问题：OFI在OrderBookProcessor中计算但未传递到ExecutionMetrics（需要重构）

#### P1（影响质量）
- 硬编码阈值：信号检测应改为动态分位数
- CoGroup健壮性：需处理Trades空窗口情况

#### P2（可选增强）
- Job 3缺失：PerpPanelAggregatorJob（汇合面板，含liquidity_regime/crowding_score）
- Redis热点快照：为agent/API提供实时数据

### 📊 输出示例

#### dws_exec_1s（执行面）
```
symbol='BTCUSDT', mid_price=64225.5, spread_bps=2.3, 
depth_50k=125000, impact_50k_bps=3.2, ofi=0.0, 
volume_usd=2.5M, vwap=64226.8
```

#### dws_perps_ctx_1m（语境面）
```
symbol='BTCUSDT', mark=64226.1, basis_bps=0.8, 
funding_rate=0.0001, funding_ema_24h=0.00012, 
oi_usd=634M, oi_delta_1m=+5.2M (+0.82%)
```

#### perp_signals（信号）
```
type=EXEC_HEALTH, level=WARNING, metric=spread_anomaly, 
value=12.5 bps, threshold=10 bps, 
detail="点差偏高：12.50 bps，超过阈值 10.00 bps"
```

---

## 历史更新 (K线信号生成Job - 2025-10-23)

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

## 最新更新 (多指标技术分析框架 - 2025-11-02)

### ✅ 已完成的开发任务

#### 1. 可扩展指标框架
- **`BaseIndicatorProcessor<T>`**: 指标处理器抽象基类
  - 统一的状态管理（价格队列、指标历史值）
  - 模板方法定义处理流程
  - 通用工具方法（SMA、EMA、标准差计算等）
  - 支持自定义指标值类型
  
- **`IndicatorConfig`**: 指标配置类
  - Builder模式构建配置
  - 预定义各指标默认配置
  - 支持灵活参数化

#### 2. 趋势类指标 (`process/kline/indicators/trend/`)

**MACD处理器（MACDProcessor）**：
- 快线周期：12，慢线周期：26，信号线周期：9
- 计算DIF、DEA、MACD柱
- 信号规则：
  - 金叉买入（DIF上穿DEA）
  - 死叉卖出（DIF下穿DEA）
  - 零轴上方/下方判断趋势强度
- 信号强度基于MACD柱绝对值和DIF-DEA距离

**EMA处理器（EMAProcessor）**：
- 支持任意周期配置（默认20/50/200）
- 信号规则：
  - 价格上穿EMA：买入信号
  - 价格下穿EMA：卖出信号
  - 价格在上升EMA之上：持续多头
  - 价格在下降EMA之下：持续空头
- 信号强度基于价格与EMA距离和EMA斜率

#### 3. 震荡类指标 (`process/kline/indicators/oscillator/`)

**RSI处理器（RSIProcessor）**：
- 周期：14，超买：70，超卖：30
- 计算方法：基于价格变化的平均涨跌幅
- 信号规则：
  - RSI从超卖区回升：买入信号
  - RSI上穿50线：多头确认
  - RSI从超买区回落：卖出信号
  - RSI下穿50线：空头确认
- 支持背离检测（预留扩展）

**KDJ处理器（KDJProcessor）**：
- K周期：9，D周期：3，J周期：3
- 计算RSV、K、D、J值
- 信号规则：
  - K上穿D（金叉）：买入信号
  - K下穿D（死叉）：卖出信号
  - 超卖区金叉：强买入
  - 超买区死叉：强卖出
  - J值极端（<0 或 >100）：反转信号

#### 4. 波动率类指标 (`process/kline/indicators/volatility/`)

**布林带处理器（BollingerBandsProcessor）**：
- 周期：20，标准差倍数：2.0
- 计算上轨、中轨、下轨、带宽、%B指标
- 信号规则：
  - 价格从下轨反弹：买入信号
  - 价格从上轨回落：卖出信号
  - 带宽收窄后扩张：突破信号
- 信号强度基于%B极端程度和带宽变化

**ATR处理器（ATRProcessor）**：
- 周期：14
- 计算真实波幅TR和ATR
- 应用场景：
  - 价格上涨 + ATR快速上升：强势突破
  - 价格下跌 + ATR快速上升：恐慌下跌
  - ATR持续下降：低波动，酝酿突破
  - ATR从低位回升：趋势启动信号

#### 5. 扩展KlineData模型
新增便捷方法：
- `getAmplitude()`: 计算振幅（百分比）
- `getChangePercent()`: 计算涨跌幅（百分比）
- `isBullish() / isBearish()`: 判断阴阳线
- `getBodySize()`: 计算K线实体大小
- `getUpperShadow() / getLowerShadow()`: 计算上下影线长度

#### 6. 多指标并行作业（MultiIndicatorJob）
- 同时运行8个指标处理器：
  - 趋势类：MACD、EMA20、EMA50、EMA200
  - 震荡类：RSI、KDJ
  - 波动率类：Bollinger Bands、ATR
- 所有信号统一输出到`kline.signal` topic
- 通过`strategy`字段区分不同指标
- 支持独立并行计算，互不干扰

### 🎯 技术架构特点

#### 指标框架设计
```
BaseIndicatorProcessor<T> (抽象基类)
├─ 状态管理：PriceQueue (OHLCV数据)
├─ 模板方法：processElement()
├─ 抽象方法：
│  ├─ calculateIndicator(): 计算指标值
│  └─ generateSignal(): 生成交易信号
└─ 工具方法：SMA、EMA、标准差、交叉判断等

指标实现层
├─ trend/
│  ├─ MACDProcessor
│  └─ EMAProcessor
├─ oscillator/
│  ├─ RSIProcessor
│  └─ KDJProcessor
└─ volatility/
   ├─ BollingerBandsProcessor
   └─ ATRProcessor
```

#### 数据流架构
```
Kafka Source (binance.kline)
  → KeyBy (symbol + interval)
  ├─ MACD Processor → KlineSignal
  ├─ EMA20/50/200 Processors → KlineSignal
  ├─ RSI Processor → KlineSignal
  ├─ KDJ Processor → KlineSignal
  ├─ Bollinger Bands Processor → KlineSignal
  └─ ATR Processor → KlineSignal
  → Union All Signals
  → Kafka Sink (kline.signal)
```

### 📊 输出格式

#### kline.signal Topic示例（MACD）
```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "interval": "1m",
  "strategy": "MACD",
  "signalType": "BUY",
  "signalStrength": 0.75,
  "currentPrice": "42010.56",
  "klineTimestamp": 1700000000000,
  "signalTimestamp": 1700000060500,
  "strategyParams": {
    "fast_period": 12,
    "slow_period": 26,
    "signal_period": 9,
    "dif": 15.234567,
    "dea": 10.123456,
    "macd": 10.222222
  },
  "signalDetail": "MACD金叉：DIF(15.234567)上穿DEA(10.123456)，MACD柱转正(10.222222)，强烈买入信号"
}
```

#### kline.signal Topic示例（RSI）
```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "interval": "1m",
  "strategy": "RSI14",
  "signalType": "BUY",
  "signalStrength": 0.82,
  "currentPrice": "42010.56",
  "klineTimestamp": 1700000000000,
  "signalTimestamp": 1700000060500,
  "strategyParams": {
    "period": 14,
    "rsi_value": 32.45,
    "overbought_threshold": 70,
    "oversold_threshold": 30
  },
  "signalDetail": "RSI从超卖区(28.34)回升至32.45，买入信号"
}
```

### 🚀 部署说明

#### 编译
```bash
cd aggregator
mvn clean package -Pcontainer
```

#### 运行多指标作业
```bash
# 使用run-job.sh脚本
./run-job.sh multi-indicator

# 或直接使用Flink命令
flink run -c com.twilight.aggregator.MultiIndicatorJob \
  target/aggregator-1.0-SNAPSHOT.jar
```

### 📈 性能指标（预估）

| 指标 | 数值 | 说明 |
|------|------|------|
| **吞吐量** | 5,000+ signals/s | 8个指标并行处理能力 |
| **端到端延迟** | < 2s | Kafka → 指标计算 → Kafka |
| **状态大小（每个指标）** | ~2-5KB/交易对 | 价格队列 + 指标状态 |
| **总状态大小** | ~16-40KB/交易对 | 8个指标 * 单指标状态 |
| **并行度** | 4-8 | 可根据负载调整 |

### 🔧 配置说明

#### 指标参数配置
所有指标默认参数可通过`IndicatorConfig`类调整：

```java
// MACD自定义参数
IndicatorConfig macdConfig = new IndicatorConfig.Builder()
    .intParam("fast_period", 12)
    .intParam("slow_period", 26)
    .intParam("signal_period", 9)
    .build();

// RSI自定义参数
IndicatorConfig rsiConfig = new IndicatorConfig.Builder()
    .intParam("period", 14)
    .intParam("overbought", 70)
    .intParam("oversold", 30)
    .build();
```

## 📋 后续优化计划

### K线信号Job优化
- [x] **短期**（已完成）
  - [x] 添加MACD策略
  - [x] 添加RSI策略
  - [x] 添加布林带策略
  - [x] 添加KDJ、ATR、EMA策略
  - [x] 策略参数可配置化
  - [x] 建立可扩展指标框架

- [ ] **中期**
  - [ ] 多策略组合信号（信号投票机制）
  - [ ] 添加更多经典指标（BOLL、OBV、VWAP等）
  - [ ] 信号回测框架
  - [ ] 信号质量评估
  - [ ] 实时绩效监控
  - [ ] 背离检测（顶背离、底背离）

- [ ] **长期**
  - [ ] 机器学习策略集成
  - [ ] 自适应参数优化
  - [ ] 多时间周期联合分析
  - [ ] 风险控制模块
  - [ ] 动态止损/止盈策略

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



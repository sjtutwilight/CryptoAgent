# 数据质量引擎 (quality-engine) 项目状态

## 项目概述

独立的Java模块，基于Spring Boot + Kafka实现实时+微批双模式数据质量检测，覆盖DEX/CEX数据管道，通过Kafka事件和Webhook实现实时告警。

## 技术栈

- Java 17
- Spring Boot 2.7.18
- Spring Kafka
- PostgreSQL (规则配置/告警记录)
- ClickHouse (时序指标)
- WebFlux (Webhook)

## 模块结构

```
quality-engine/
├── config/           # 配置类
├── domain/           # 领域模型
│   ├── enums/        # 枚举（QualityDimension, AlertLevel, DataDomain）
│   ├── entity/       # JPA实体（AlertRecord, RuleConfig）
│   ├── rule/         # 规则相关（RuleResult, RuleContext）
│   ├── metric/       # 指标模型（QualityMetric）
│   └── alert/        # 告警模型（QualityAlert）
├── rule/             # 规则引擎
│   ├── base/         # 基类（QualityRule, BaseRule, AggregateRule）
│   ├── realtime/     # 实时规则（CompletenessRule, RangeCheckRule, SchemaValidationRule）
│   └── aggregate/    # 聚合规则（FreshnessRule, ThroughputRule, ConsistencyRule）
├── consumer/         # Kafka消费者
├── aggregator/       # 窗口聚合
├── alert/            # 告警分发
│   └── channel/      # 告警通道（Kafka, Webhook, Persistence）
├── sink/             # 指标落库
├── repository/       # 数据访问层
└── api/              # REST API
```

## 已实现功能

### 1. 实时规则（per-message）

| 规则名称 | 维度 | 适用域 | 状态 |
|---------|------|--------|------|
| `dex.completeness.required_fields` | 完整性 | DEX | ✅ |
| `kline.completeness.required_fields` | 完整性 | KLINE | ✅ |
| `perp.orderbook.completeness` | 完整性 | PERP | ✅ |
| `perp.trades.completeness` | 完整性 | PERP | ✅ |
| `dex.accuracy.amount_range` | 准确性 | DEX | ✅ |
| `kline.accuracy.price_range` | 准确性 | KLINE | ✅ |
| `perp.orderbook.price_range` | 准确性 | PERP | ✅ |
| `dex.schema.validation` | 模式 | DEX | ✅ |
| `kline.schema.validation` | 模式 | KLINE | ✅ |
| `perp.orderbook.schema.validation` | 模式 | PERP | ✅ |

### 2. 聚合规则（窗口）

| 规则名称 | 维度 | 窗口 | 状态 |
|---------|------|------|------|
| `dex.timeliness.freshness` | 时效性 | 1min | ✅ |
| `kline.timeliness.freshness` | 时效性 | 1min | ✅ |
| `perp.orderbook.freshness` | 时效性 | 1min | ✅ |
| `dex.timeliness.throughput` | 吞吐量 | 1min | ✅ |
| `kline.timeliness.throughput` | 吞吐量 | 1min | ✅ |
| `perp.orderbook.throughput` | 吞吐量 | 1min | ✅ |

### 3. 告警通道

- [x] Kafka告警通道
- [x] Webhook告警通道
- [x] PostgreSQL持久化通道
- [x] 告警限流

### 4. 存储

- [x] PostgreSQL: 告警记录、规则配置
- [x] ClickHouse: 时序质量指标

### 5. API

- [x] `GET /api/quality/status` - 系统状态
- [x] `GET /api/quality/rules` - 规则列表
- [x] `GET /api/quality/alerts` - 告警查询
- [x] `GET /api/quality/alerts/stats` - 告警统计
- [x] `GET /api/quality/alerts/{id}` - 告警详情
- [x] `GET /api/quality/health` - 健康检查
- [x] `POST /api/quality/rules/configs` - 规则配置

## 配置说明

### 主要配置项（application.yml）

```yaml
quality:
  rule:
    config-path: classpath:rules/default-rules.yml
  window:
    default-size-ms: 60000
    flush-interval-ms: 10000
  alert:
    channels:
      kafka:
        enabled: true
        topic: quality.alerts
        min-level: WARNING
      webhook:
        enabled: false
        url: ${WEBHOOK_URL:}
        min-level: CRITICAL
    rate-limit:
      window-seconds: 60
      max-alerts-per-rule: 5
  metric:
    batch-size: 100
    flush-interval-ms: 5000
```

## 启动方式

```bash
cd quality-engine
mvn spring-boot:run
```

## 依赖服务

- Kafka: localhost:9092
- PostgreSQL: localhost:5432/twilight
- ClickHouse: localhost:8123

## 待完成

- [ ] 跨源价格一致性检测
- [ ] 规则配置热加载
- [ ] 指标Dashboard集成
- [ ] 告警聚合（相似告警合并）

## 更新日志

### v0.1.0 (MVP)
- 初始版本
- 实时规则：完整性、准确性、模式校验
- 聚合规则：时效性、吞吐量
- 告警分发：Kafka、Webhook、持久化
- REST API


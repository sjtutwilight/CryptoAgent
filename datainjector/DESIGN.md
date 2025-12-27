# DataInjector 设计文档

## 模块定位
数据接入层，负责从多源异构数据源采集数据并保障完整性。

## 核心功能
- 控制面与数据面分离：任务调度、状态管理、全局限流
- 配置驱动的统一Worker架构：支持WebSocket/HTTP/SDK多协议
- 数据完整性保障：顺序性、完整性、幂等性三维保障
- 智能补数：支持快照补数与范围补数

## 技术栈
- Go（Worker）
- Java Spring Boot（Control Plane）
- PostgreSQL（任务持久化）
- Kafka（任务下发与数据输出）
- Redis（限流令牌桶）

## 数据流
```
数据源 → Worker → Kafka
         ↑
    Control Plane（任务调度/限流/状态管理）
```

## 关键设计
- 四大亮点机制：数据完整性保障、延时调度、状态管理与重试、多级限流
- Integrity模块：SequenceEngine、ReorderBuffer、BackfillScheduler、Gate、Deduper
- 仿真数据源：MockDataProvider（故障注入）、LocalNode（DEX仿真）

## 详细说明
- Worker设计：参见 `worker/DESIGN.md`
- 控制面设计：参见 `control-plane-service/DESIGN.md`
- Mock Provider：参见 `mockDataProvider/DESIGN.md`
- LocalNode：参见 `localnode/DESIGN.md`

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架











# Realtime Pipeline 设计文档

## 模块定位
实时流处理管道，负责DEX交易数据的实时清洗与转换（DWD层）。

## 核心功能
- DexSwapDwdJob：DEX交易明细层处理
- 数据清洗：字段标准化、类型转换
- 实时转发：Kafka → Flink → Kafka

## 技术栈
- Apache Flink
- Kafka

## 数据流
```
Kafka (dex_transaction) → DexSwapDwdJob → Kafka (dwd_dex_swap)
```

## 关键设计
- 轻量级转换：仅做必要的清洗与标准化
- 低延迟：毫秒级处理
- 幂等性：支持重复消费

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架







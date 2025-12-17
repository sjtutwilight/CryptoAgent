# Mock Data Provider 设计文档

## 模块定位
仿真数据源，用于验证数据接入层的非功能性处理能力（故障注入、数据缺失、乱序等）。

## 核心功能
- 数据生成器：模拟区块链、交易所等数据源
- 故障注入器：HTTP失败、WebSocket断连、数据缺失、心跳异常、链重组
- 多协议支持：WebSocket、HTTP、JSON-RPC

## 技术栈
- Go
- WebSocket Server
- HTTP Server

## 数据流
```
配置文件 → DataGenerator → FaultInjector → Protocol Server
                                              ↓
                                          Worker订阅/拉取
```

## 关键设计
- 可配置故障场景（故障类型、发生概率、持续时间）
- 支持链重组模拟（回滚+分叉）
- 心跳与重连机制验证

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架


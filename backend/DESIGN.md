# Backend 设计文档

## 模块定位
数据应用层后端服务，提供RESTful API供前端与Agent调用。

## 核心功能
- Token指标API：价格、市值、交易量、持仓分布
- K线分析API：技术指标（MACD/RSI/BOLL等）、信号查询
- 永续合约API：执行面、语境面、面板指标、信号查询
- PnL分析API：账户盈亏、宏观指标（NUPL/MVRV/SOPR）

## 技术栈
- Spring Boot
- ClickHouse（主查询）
- Redis（热点缓存）
- PostgreSQL（元数据）

## 数据流
```
前端/Agent → Controller → Service → ClickHouse/Redis
                                        ↓
                                  数据聚合与转换
                                        ↓
                                  ApiResponse<T>
```

## 关键设计
- 统一响应格式：ApiResponse<T>（code/message/data/timestamp）
- 分页与限制：最大pageSize=500，序列limit=5000
- 查询优化：利用ClickHouse Projection加速
- 缓存策略：热点数据Redis缓存

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架


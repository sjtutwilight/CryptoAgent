# Control Plane Service 设计文档

## 模块定位
控制面服务，负责任务调度、状态管理、限流控制与Worker协调。

## 核心功能
- 任务生命周期管理（创建、调度、重试、状态追踪）
- 全局限流与配额管理
- 基于持久化时间戳的延时调度
- 任务状态上报与监控

## 技术栈
- Spring Boot
- PostgreSQL（任务持久化）
- Kafka（任务下发与状态上报）
- Redis（限流令牌桶）

## 数据流
```
REST API → TaskScheduler → PostgreSQL
          ↓
TimerProducer → Kafka (http.tasks)
          ↓
Worker → StatusListener → PostgreSQL (状态更新)
```

## 关键设计
- 双重限流：控制面全局限流 + Worker局部限流
- 定时扫描器：每1秒扫描到期任务并下发
- 指数退避重试：支持可重试/不可重试错误分类

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架


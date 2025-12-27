# Observability 设计文档

## 模块定位
可观测性栈，提供监控、日志、追踪能力。

## 核心功能
- 指标监控：Prometheus采集 + Grafana可视化
- 日志聚合：Promtail收集 + Loki存储
- 告警管理：AlertManager规则配置与通知
- 链路追踪：预留Jaeger集成

## 技术栈
- Prometheus
- Grafana
- Loki
- Promtail

## 数据流
```
应用指标 → Prometheus → Grafana Dashboard
应用日志 → Promtail → Loki → Grafana查询
告警规则 → AlertManager → 通知渠道
```

## 关键设计
- 统一采集：所有服务暴露/metrics端点
- 日志标准化：结构化日志格式
- 告警分级：P0/P1/P2告警策略

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架
- 2025-12-19: 修复 Grafana 无法搜索容器日志问题
  - **问题**: Grafana Loki Explore 显示 404 错误，无法查看容器日志
  - **根因**: Loki 2.9.2 不支持 Volume API (`/loki/api/v1/index/volume`)，而 Grafana 新版 Loki Explore 插件依赖此 API
  - **修复**: 
    - 升级 Loki: 2.9.2 → 3.3.1
    - 升级 Promtail: 2.9.2 → 3.3.1
    - 更新 Loki 配置: schema v11 (boltdb-shipper) → v13 (tsdb)
    - 启用 Volume API 支持 (`volume_enabled: true`)
    - 添加日志保留策略 (7天)
  - **新增工具**: `automation/test/tools/probe_cli.py infra stack` - 日志链路/观测栈诊断（probe）
  - **验证**: Promtail 正常采集容器日志，Loki 存储正常，Grafana 连接正常






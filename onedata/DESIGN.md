# OneData 设计文档

## 模块定位
数据治理平台，提供元数据管理与数据质量保障能力。

## 核心功能
- 元数据管理：数据资产目录、血缘关系、标签体系
- 数据质量检测：完整性、准确性、一致性、及时性检测
- 质量报告：质量评分、趋势分析、告警通知

## 技术栈
- Spring Boot（后端服务）
- React（前端界面）
- PostgreSQL（元数据存储）
- Flink（实时质量检测）
- ClickHouse（质量指标存储）

## 数据流
```
数据源 → 元数据采集 → 元数据仓库 → 前端展示
         ↓
    质量检测引擎 → 质量指标 → 告警通知
```

## 关键设计
- 统一元数据模型：表、字段、血缘、标签
- 规则引擎：可配置质量规则
- 血缘图谱：可视化数据流向与依赖

## 详细说明
- Metadata Core：参见 `metadata-core/DESIGN.md`
- Metadata UI：参见 `metadata-ui/DESIGN.md`
- Quality Engine：参见 `quality-engine/DESIGN.md`

## 变更记录
- 2025-12-17: 初始化DESIGN.md框架











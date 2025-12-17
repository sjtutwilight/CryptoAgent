# 数据治理平台前端 (metadata-ui) 项目状态

## 项目概述

数据治理平台统一前端，集成元数据管理和数据质量监控两大模块。

## 技术栈

- React 18
- Material-UI 5
- React Router 6
- Axios
- D3.js (可视化)
- date-fns (日期处理)

## 模块结构

```
metadata-ui/
├── src/
│   ├── components/
│   │   ├── quality/              # 质量监控组件
│   │   │   ├── AlertLevelBadge.js    # 告警级别徽章
│   │   │   ├── DimensionBadge.js     # 质量维度徽章
│   │   │   ├── QualityStatsCard.js   # 统计卡片
│   │   │   ├── AlertCard.js          # 告警卡片
│   │   │   ├── RuleCard.js           # 规则卡片
│   │   │   ├── AlertStatsChart.js    # 告警统计图表
│   │   │   └── index.js              # 组件导出
│   │   ├── Layout.js             # 应用布局（含导航）
│   │   ├── EntityCard.js         # 实体卡片
│   │   ├── EntityList.js         # 实体列表
│   │   ├── QualityGauge.js       # 质量仪表
│   │   └── ...
│   ├── pages/
│   │   ├── DiscoveryPage.js      # 元数据发现页
│   │   ├── EntityDetailPage.js   # 实体详情页
│   │   ├── QualityDashboardPage.js   # 质量监控仪表盘
│   │   ├── AlertListPage.js      # 告警列表页
│   │   └── RuleListPage.js       # 规则管理页
│   ├── services/
│   │   ├── api.js                # 元数据API服务
│   │   └── qualityApi.js         # 质量引擎API服务
│   ├── hooks/
│   │   ├── useMetadataSearch.js  # 元数据搜索Hook
│   │   └── useSSE.js             # SSE实时更新Hook
│   ├── App.js                    # 路由配置
│   ├── theme.js                  # 主题配置
│   └── setupProxy.js             # 开发代理配置
└── package.json
```

## 已实现功能

### 1. 元数据管理模块

| 功能 | 状态 |
|------|------|
| 元数据发现页面 | ✅ |
| 实体搜索和筛选 | ✅ |
| 实体详情页面 | ✅ |
| 血缘关系图 | ✅ |
| SSE实时更新 | ✅ |

### 2. 数据质量监控模块

| 功能 | 状态 |
|------|------|
| 质量监控仪表盘 | ✅ |
| 系统状态展示 | ✅ |
| 告警统计图表 | ✅ |
| 最近告警列表 | ✅ |
| 告警列表页面 | ✅ |
| 告警筛选（级别/域/规则） | ✅ |
| 告警分页 | ✅ |
| 规则管理页面 | ✅ |
| 规则搜索和筛选 | ✅ |
| 规则状态显示 | ✅ |

### 3. 公共组件

| 组件 | 功能 |
|------|------|
| AlertLevelBadge | 告警级别徽章（INFO/WARNING/CRITICAL） |
| DimensionBadge | 质量维度徽章（完整性/时效性/准确性/一致性/模式） |
| QualityStatsCard | 统计数据卡片 |
| AlertCard | 告警信息卡片（支持紧凑模式） |
| RuleCard | 规则信息卡片 |
| AlertStatsChart | 告警分布图表 |

## 路由配置

| 路径 | 页面 | 说明 |
|------|------|------|
| `/` | DiscoveryPage | 元数据发现 |
| `/entity/:id` | EntityDetailPage | 实体详情 |
| `/quality` | QualityDashboardPage | 质量监控仪表盘 |
| `/quality/alerts` | AlertListPage | 告警列表 |
| `/quality/rules` | RuleListPage | 规则管理 |

## API对接

### 元数据服务 (metadata-core: 8095)
- `GET /v1/metadata/entities` - 实体搜索
- `GET /v1/metadata/entities/:id` - 实体详情
- `GET /v1/metadata/entities/:id/lineage` - 血缘关系
- `GET /v1/metadata/updates/stream` - SSE实时更新

### 质量引擎 (quality-engine: 8096)
- `GET /api/quality/status` - 系统状态
- `GET /api/quality/health` - 健康检查
- `GET /api/quality/rules` - 规则列表
- `GET /api/quality/alerts` - 告警查询
- `GET /api/quality/alerts/stats` - 告警统计
- `GET /api/quality/alerts/:id` - 告警详情

## 启动方式

```bash
cd metadata-ui

# 安装依赖
npm install

# 开发模式启动
npm start
```

访问 http://localhost:3000

## 依赖服务

- metadata-core: http://localhost:8095 (元数据服务)
- quality-engine: http://localhost:8096 (质量引擎)

## 待完成

- [ ] 告警详情弹窗/页面
- [ ] 规则配置编辑
- [ ] 指标趋势图表
- [ ] 告警通知配置
- [ ] 规则执行历史

## 更新日志

### v0.2.0 (数据质量模块)
- 新增质量监控仪表盘
- 新增告警列表页面
- 新增规则管理页面
- 新增质量相关组件
- 平台更名为"数据治理平台"
- 配置多服务代理

### v0.1.0 (MVP)
- 初始版本
- 元数据发现和搜索
- 实体详情和血缘
- SSE实时更新


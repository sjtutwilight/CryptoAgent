# AI 数据分析 Agent

基于 LangGraph + DeepSeek 的多模态数据助手，可通过统一的工具集访问代币、K 线与永续合约指标。本指南聚焦永续合约（Perp）场景，帮助产品/研究同学提出更高质量的问题，并理解代理的输出。

## 功能概览

- 🤖 **智能路由**：基于用户问题自动选择 `perps/markets`、`perps/{symbol}/execution` 等后端接口。
- 🧠 **短期记忆**：前端通过 `sessionId` 维持 6 轮对话，代理会在提示词中注入摘要 & 最近原文，保证上下文连续。
- 📈 **多视角分析**：执行面（盘口/成交）、语境面（资金费率/持仓）、面板融合、异常信号均可一站式查询。
- 🔍 **调试友好**：若 `AGENT_DEBUG=true`，控制台会打印 LangGraph 步骤、工具调用与 Observation，便于排查推理链。

## 系统组成

```
agent/
├── new_agent_api.py        # Flask API（/chat、/tools、/examples 等）
├── simple_agent.py         # LangGraph ReAct Agent + 系统提示词
├── conversation_manager.py # 基于内存的短期会话管理
├── tools/                  # 各领域工具封装
│   ├── base.py             # API 客户端、to_csv 等公用逻辑
│   ├── perp_tools.py       # 永续合约相关工具
│   └── ...                 # Token / Kline / 系统工具
└── README.md               # 当前文档
```

后端依赖 `backend/src/main/java/com/twilight/backend/controller/PerpAnalyticsController.java` 暴露的 `/v1/perps` 系列接口；前端 `ChatWidget` 会在请求中附带 `sessionId` 并持久化到 `localStorage`。

## 永续合约分析指南

### 1. 关键指标面板

| 场景                 | 对应工具/接口                          | 关注字段示例 |
|----------------------|-----------------------------------------|--------------|
| 大盘排序 / 换仓筛选  | `get_perp_market_snapshots` → `/perps/markets`，可配合 `get_kline_market_snapshots` | `avgSpreadBps`, `avgDepth50k`, `crowdingScore`, `liquidityRegime`, `fundingRate`, 现货涨跌幅 |
| 流动性健康           | `get_perp_execution_series` → `/perps/{symbol}/execution` | `spreadBps`, `impact_50k`, `depth_*`, `volumeUsd`, `ofi`, `illiqLambda` |
| 杠杆 & 仓位风险      | `get_perp_context_series` → `/perps/{symbol}/context` | `fundingRate`, `fundingEma24h`, `oiUsd`, `oiDeltaPct`, `isOiCarried` |
| 综合得分 / 拥挤度    | `get_perp_panel_series` → `/perps/{symbol}/panel` | `crowdingScore`, `avgImpact50kBps`, `avgImbalance`, `liquidityRegime` |
| 异常告警             | `get_perp_signals` → `/perps/signals`    | `signalLevel`, `signalType`, `metricName`, `metricValue`, `contextJson` |

### 2. 提问要点（写给用户）

1. **指定交易维度**：请明确 `symbol`、`exchange`，必要时附带 `algoVersion`（如 `prod`、`exp_v2`）。
2. **时间窗口**：说明观察范围（如 “近 30 分钟” → `limit=1800`，或提供 `startTime/endTime`），并指出需要秒级还是分钟级。
3. **指标关注点**：告知需要对 `spread` / `funding` / `crowding` / `signals` 做什么判断（对比、过滤、监控等）。
4. **多场景组合**：如果问题涉及执行 + 语境 + 信号，请分步描述；若要联合现货表现，请注明现货交易所/周期/时间窗，方便代理先调用 K 线接口再调用永续接口，并给出综合结论。

#### 优秀提问模板

- “比较 OKX 与 Hyperliquid 上 BTCUSDT 近 30 分钟（现货 1m K 线 + 永续执行面 1s 数据）的点差、冲击成本，判断哪个更适合下单。”
- “分析 ETHUSDT 在 Binance 现货的涨幅与永续资金费率/拥挤度过去 12 小时的变化，评估是否存在杠杆过度。”
- “列出最近 50 条 signalLevel≥WARNING 的永续异常信号，并结合现货 K 线判断是否出现价格共振。”
- “我想寻找现货成交量放大但永续 crowdingScore < 0.4 的标的，给出前 5 名并附上 spread/funding。”

### 3. 回答结构（代理遵循）

1. **结论摘要**：一句话说明状态，如 “BTCUSDT 在 Hyperliquid 上流动性走弱且资金费率转负”。
2. **执行面**：列出时间窗口 + `spread/impact/depth/volume/OFI` 的统计与趋势。
3. **语境面**：说明 `fundingRate`, `fundingEma24h`, `oiUsd`, `oiDeltaPct` 的方向及其含义。
4. **面板/信号**：若使用 `panel` 或 `signals`，给出 `crowdingScore`, `liquidityRegime`, `signalLevel` 等，并引用 `contextJson` 的关键理由。
5. **操作建议**：结合前述指标说明可行动作（调仓/等待/扩大观察窗口），若数据不足要指出限制和推荐的下一步查询。

## 会话与调试

- **Session 管理**：前端在请求体和 `X-Session-Id` 中发送 `sessionId`。`POST /chat` 支持 `reset=true` 清空记忆；CLI 输入 `reset` 亦可。
- **短期记忆**：最多保留近 6 轮原文，超过阈值会压缩为摘要并插入系统提示 (`# 历史摘要`)。
- **调试输出**：设置 `AGENT_DEBUG=true`，`simple_agent` 会在控制台打印 LangGraph 步骤、工具调用及 Observation 摘要，同时 API 响应包含 `debugSteps` 方便前端展示或日志采集。

## 快速启动

1. 安装依赖：
   ```bash
   cd agent
   pip install -r requirements.txt
   ```
2. 配置环境变量（示例）：
   ```bash
   export BACKEND_API_URL=http://localhost:8088/api/v1
   export AGENT_DEBUG=true
   ```
3. 启动服务：
   ```bash
   python new_agent_api.py
   ```
4. 前端 `ChatWidget` 将自动携带 `sessionId`，可通过建议模板开始询问永续行情。

如需进一步定制（例如新增永续指标或扩展长记忆），请同步更新 `perp_tools` 的描述与本 README，保持用户与代理的心智一致。***

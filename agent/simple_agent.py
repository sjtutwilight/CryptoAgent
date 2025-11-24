"""
简化版API代理
使用LangGraph的预构建ReAct Agent
"""
import json
import logging
from typing import Dict, Any, List, Optional
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, BaseMessage
from langgraph.prebuilt import create_react_agent

from config import DEEPSEEK_CONFIG, AGENT_RUNTIME_CONFIG
from api_tools import AVAILABLE_TOOLS

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleAPIAgent:
    """简化版API代理"""
    
    def __init__(self):
        # 初始化 DeepSeek 客户端
        self.llm = ChatOpenAI(
            api_key=DEEPSEEK_CONFIG["api_key"],
            base_url=DEEPSEEK_CONFIG["base_url"],
            model=DEEPSEEK_CONFIG["model"],
            temperature=1.0
        )
        self.debug_enabled = AGENT_RUNTIME_CONFIG["debug_enabled"]
        
        # 系统提示词
        self.system_prompt = self._build_system_prompt()
        
        # 创建ReAct代理
        self.agent = create_react_agent(
            self.llm,
            AVAILABLE_TOOLS,
            prompt=self.system_prompt
        )
    
    def _build_system_prompt(self) -> str:
        """构建系统提示词"""
        
        tools_description = "\n".join([
            f"- {tool.name}: {tool.description}" 
            for tool in AVAILABLE_TOOLS
        ])
        
        return f"""你是一个专业的区块链数据分析助手，可以通过API工具访问代币、K线与永续合约的实时指标。

# 可用工具
{tools_description}

# 用户提问预期
- 鼓励用户给出「场景 + 对象 + 维度/时间范围」的完整描述，例如：「分析 BTCUSDT 永续近30分钟的点差和成交量」。 
- 当缺少关键参数（如交易对、交易所、时间范围、排序字段）时，先向用户确认所需信息再调用工具。
- 若用户的问题跨多个板块（如同时需要K线和永续数据），可以按「先确认需求 → 分步调用工具 → 汇总结论」的顺序处理。

# 工具使用指南
## 通用操作
1. 根据用户意图列出需要的指标，并选择最匹配的工具。
2. 在调用工具前明确参数（symbols/exchange/interval/timeRange/limit等），必要时进行单位换算或默认值说明。
3. 工具返回后检查 `status` / `code` 字段；若失败，给出错误信息及可行的补救建议。
4. 对原始数据做总结，提取关键数值并转化为易懂的洞察。

## 代币与账户（现有能力）
- 代币列表/排行 → 使用 `get_token_list`
- 代币详细信息/概览 → 使用 `get_token_overview`
- 代币持有者分布 → 使用 `get_token_distribution`
- 代币PnL分析 → 使用 `get_token_pnl`
- 账户详情/资产/历史 → 使用 `get_account_detail`
- 系统健康检查 → 使用 `health_check`

## K线决策分析
- 大盘快照/排行 → 使用 `get_kline_market_snapshots`
- 指定交易对的历史K线 → 使用 `get_kline_series`
- RSI/MACD等指标输出 → 使用 `get_kline_indicators`
- 目前仅支持 1m / 5m 周期；如用户请求其他周期，需说明限制并建议使用可用周期
- 对比不同交易所或周期时，可多次调用并在结论中总结差异

## K线决策流程
1. 明确查询目标：行情概览（多标的）或单一标的深度分析，并确认 symbol / exchange / interval / 时间窗口。
2. 大盘或候选筛选 → 调用 `get_kline_market_snapshots`，提取涨跌幅、成交量、振幅等排序依据。
3. 深度分析 → 调用 `get_kline_series`，结合最近价格、涨跌、成交量、信号字段；如需动量/超买判断，再调用 `get_kline_indicators`。
4. 若需与永续对比，请复用同一 symbol/exchange，保持时间尺度一致，便于说明现货 vs 永续差异。
5. 若要覆盖两个时间尺度（如近15分钟 vs 2小时），请说明数据来自 1m/5m，并解释为何该尺度能代表用户目标。
6. 汇总时突出趋势、动量、波动、信号一致性；若数据不足（例如 limit 较短或返回为空），要说明限制并给出下一步提议。

## K线回答模板
1. **结论摘要**：给出明确的多/空或中性判断，并标注所依据的时间窗口。
2. **关键指标**：列表展示（价格、涨跌%、成交量、振幅、信号/指标值），注明数据来源接口和参数。
3. **趋势&风险**：描述价格/指标的演变（上升/放缓/背离），指出可能的驱动因素或需关注的风险。
4. **操作建议**（若用户有决策诉求）：可提出关注点，例如「若成交量继续走低需警惕假突破」；若无足够信息则建议补充查询。

## 永续合约决策分析
- 市场快照/排序 → 使用 `get_perp_market_snapshots`
- 执行面（秒级盘口&成交） → 使用 `get_perp_execution_series`
- 语境面（资金费率/持仓/OI） → 使用 `get_perp_context_series`
- 面板指标（执行+语境融合） → 使用 `get_perp_panel_series`
- 异常信号告警 → 使用 `get_perp_signals`
- 说明 exchange/algo/timeRange/limit 参数，必要时引用 perp docs 中的限制（执行面 limit<=10000, 语境/面板 limit<=1440）
- 若用户关注风险，请综合执行面（流动性）、语境面（杠杆/持仓）与异常信号，得出一致/冲突结论

## 永续决策流程
1. 明确场景：是挑选低点差标的、监控资金费率异常、还是验证拥挤度；确认 symbol/exchange/algo/timeRange。
2. 先用 `get_perp_market_snapshots` 获取候选列表或验证整体 regime（spread/crowding/liquidityRegime）。
3. 根据问题拆分：
   - 交易执行健康 → `get_perp_execution_series` 查看 spreadBps、impact、depth、ofi、volume 的短期变化；
   - 杠杆/仓位风险 → `get_perp_context_series` 分析 fundingRate、oiUsd、oiDeltaPct；
   - 需要多维得分/策略共识 → `get_perp_panel_series` 结合 crowdingScore、avgImpact50kBps；
   - 异常/告警 → `get_perp_signals` 聚焦高等级信号并读取 contextJson 原因。
4. 如果问题跨时间尺度，分别说明每次调用的 limit/时间区间，并比较其差异。
5. 在需要全盘分析时，明确与现货 K 线对应的数据段，并指出执行/语境与现货走势的背离或共振，必要时给出下一步建议。

## 现货 + 永续综合分析提示
1. 使用 `get_kline_market_snapshots` / `get_kline_series` 获取现货/指数价趋势，再使用 `get_perp_*` 工具获取永续指标，确保时间区间一致。
2. 比较以下维度：
   - **价格差异**：现货涨跌 vs 永续 mid/fair 价、basis。
   - **成交量/流动性**：K线成交量与永续 volume/depth/impact 是否同步。
   - **情绪指标**：K线信号与永续 funding/crowding 是否共振。
3. 输出时先给出现货结论，再补充永续指标与差异点；若存在显著背离，应解释可能原因（套利、杠杆需求等）。

## 永续回答模板
1. **结论摘要**：给出明确判断（例如“BTCUSDT 在 Hyperliquid 上流动性趋弱”），并注明数据时间窗。
2. **执行面**：列举关键指标（spread、impact、depth、ofi、volume）及变化方向。
3. **语境面**：说明资金费率、OI、杠杆/拥挤度变化，是否与执行面一致。
4. **异常/信号**：如有信号，描述类型、等级、触发指标与 contextJson 中的原因。
5. **风险与建议**：指出潜在风险（例如资金费率失衡、流动性 regime 变化）和可行动作（调低杠杆、关注下一次 Funding 等）。 

# 结果输出规范
- 先给出核心结论，再展示关键数据点（建议使用条目或表格文字）。
- 明确时间范围、交易所、数据来源（如「数据源：/v1/perps/panel，limit=1440」）。
- 对趋势类问题说明变化方向与可能原因；若数据不足，需要说明假设或限制。
- 如果进行了多个工具调用，按调用顺序概述过程，并在最后汇总整体判断。

# 可靠性提醒
- 始终以中文回复。
- 对数值设置合理精度：价格保留4位小数，百分比保留2位小数；大量级差异较大时给出单位（如USD / 百万美元）。
- 若工具返回空数据或错误，请解释可能原因，并指导用户尝试调整参数或时间范围。
- 不编造没有的数据；必要时明确指出需要进一步查询或等待数据更新。
"""
    
    def process_query(
            self,
            user_query: str,
            history: Optional[List[Dict[str, str]]] = None,
            summary: str = ""
    ) -> Dict[str, Any]:
        """处理用户查询"""
        logger.info(f"处理用户查询: {user_query}")
        
        try:
            compiled_history = self._build_history_messages(history or [], summary or "")
            invoke_messages: List[BaseMessage] = compiled_history + [HumanMessage(content=user_query)]

            # 调用代理处理查询
            result = self.agent.invoke({
                "messages": invoke_messages
            })
            
            # 提取最终回复
            messages = result.get("messages", [])
            if messages:
                last_message = messages[-1]
                if hasattr(last_message, 'content'):
                    final_answer = last_message.content
                else:
                    final_answer = str(last_message)
            else:
                final_answer = "抱歉，无法生成回复。"

            debug_steps = self._extract_debug_steps(messages)
            if self.debug_enabled:
                self._log_debug_trace(debug_steps)
            
            return {
                "status": "success",
                "answer": final_answer,
                "conversation": [
                    {
                        "role": "user" if isinstance(msg, HumanMessage) else "assistant",
                        "content": msg.content if hasattr(msg, 'content') else str(msg)
                    }
                    for msg in messages 
                    if hasattr(msg, 'content') and msg.content
                ],
                "debugSteps": debug_steps if debug_steps else None
            }
            
        except Exception as e:
            logger.error(f"查询处理失败: {e}")
            import traceback
            traceback.print_exc()
            
            return {
                "status": "error",
                "error": str(e),
                "answer": f"处理查询时发生错误: {str(e)}"
            }

    def _build_history_messages(
            self,
            history: List[Dict[str, str]],
            summary: str
    ) -> List[BaseMessage]:
        messages: List[BaseMessage] = []
        if summary:
            messages.append(SystemMessage(content=f"# 历史摘要\n{summary}"))

        for turn in history:
            content = turn.get("content")
            if not content:
                continue
            role = turn.get("role", "user")
            if role == "assistant":
                messages.append(AIMessage(content=content))
            else:
                messages.append(HumanMessage(content=content))
        return messages

    def _extract_debug_steps(self, messages: List[BaseMessage]) -> List[Dict[str, Any]]:
        debug_steps = []
        for idx, msg in enumerate(messages):
            content = getattr(msg, "content", "")
            role = getattr(msg, "type", msg.__class__.__name__)
            step = {
                "index": idx + 1,
                "role": role,
                "content": content if isinstance(content, str) else str(content)
            }
            tool_calls = getattr(msg, "tool_calls", None)
            if tool_calls:
                step["tool_calls"] = tool_calls
            debug_steps.append(step)
        return debug_steps

    def _log_debug_trace(self, steps: List[Dict[str, Any]]) -> None:
        if not steps:
            return
        logger.info("===== Agent Trace (%d steps) =====", len(steps))
        for step in steps:
            content = step.get("content", "")
            if len(content) > 500:
                content = content[:500] + "..."
            logger.info("Step %s [%s]: %s", step.get("index"), step.get("role"), content)

# 全局代理实例
simple_agent = SimpleAPIAgent()

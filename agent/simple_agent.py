"""
简化版API代理
使用LangGraph的预构建ReAct Agent
"""
import json
import logging
from typing import Dict, Any
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from config import DEEPSEEK_CONFIG
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
            temperature=0.1
        )
        
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

## 永续合约决策分析
- 市场快照/排序 → 使用 `get_perp_market_snapshots`
- 执行面（秒级盘口&成交） → 使用 `get_perp_execution_series`
- 语境面（资金费率/持仓/OI） → 使用 `get_perp_context_series`
- 面板指标（执行+语境融合） → 使用 `get_perp_panel_series`
- 异常信号告警 → 使用 `get_perp_signals`

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
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """处理用户查询"""
        logger.info(f"处理用户查询: {user_query}")
        
        try:
            # 调用代理处理查询
            result = self.agent.invoke({
                "messages": [HumanMessage(content=user_query)]
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
                ]
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

# 全局代理实例
simple_agent = SimpleAPIAgent()

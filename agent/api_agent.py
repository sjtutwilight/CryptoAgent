"""
基于API工具的智能代理系统
使用LangGraph和DeepSeek，通过API工具调用后端服务
"""
import json
import logging
from typing import Dict, Any, List
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode, create_react_agent
from pydantic import BaseModel, Field

from config import DEEPSEEK_CONFIG
from api_tools import AVAILABLE_TOOLS

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AgentState(dict):
    """代理状态管理 - 使用dict子类避免TypedDict的限制"""
    
    def __init__(self, **kwargs):
        super().__init__()
        self.update({
            "messages": [],
            "user_query": "",
            "intent": "",
            "selected_tools": [],
            "tool_results": [],
            "final_answer": "",
            "error_message": "",
            "step": "start"
        })
        self.update(kwargs)

class APIAgent:
    """基于API工具的代理主类"""
    
    def __init__(self):
        # 初始化 DeepSeek 客户端
        self.llm = ChatOpenAI(
            api_key=DEEPSEEK_CONFIG["api_key"],
            base_url=DEEPSEEK_CONFIG["base_url"],
            model=DEEPSEEK_CONFIG["model"],
            temperature=0.1
        ).bind_tools(AVAILABLE_TOOLS)
        
        # 工具节点
        self.tool_node = ToolNode(AVAILABLE_TOOLS)
        
        # 构建工作流图
        self.workflow = self._build_workflow()
        
        # 系统提示词
        self.system_prompt = self._build_system_prompt()
    
    def _build_system_prompt(self) -> str:
        """构建系统提示词"""
        
        tools_description = "\n".join([
            f"- {tool.name}: {tool.description}" 
            for tool in AVAILABLE_TOOLS
        ])
        
        return f"""你是一个专业的区块链数据分析助手，可以通过API工具访问实时的链上数据。

# 可用工具
{tools_description}

# 工具使用指南

## 1. 代币相关查询
- 代币列表/排行 → 使用 get_token_list
- 代币详细信息/概览 → 使用 get_token_overview  
- 代币持有者分布 → 使用 get_token_distribution
- 代币PnL分析 → 使用 get_token_pnl

## 2. 账户相关查询
- 账户详情/资产/历史 → 使用 get_account_detail

## 3. 系统状态查询
- API健康检查 → 使用 health_check

# 参数说明
- token_id: 代币的唯一标识ID
- account_id: 账户的唯一标识ID  
- time_range: 时间范围，可选值 20s/1min/5min/1h
- sort_by: 排序字段，可选值 mcap/volume/holders/price
- order: 排序方向，asc(升序)/desc(降序)

# 响应处理
1. 根据用户查询选择合适的工具
2. 解析工具返回的JSON数据
3. 提取关键信息并整理成易读的格式
4. 如果数据不足，主动说明并建议其他查询方式

# 注意事项
- 优先使用工具获取实时数据
- 对于复杂查询，可能需要调用多个工具
- 始终以中文回复用户
- 数值要保持适当的精度（价格保留4位小数，百分比保留2位小数）
"""

    def _build_workflow(self) -> StateGraph:
        """构建 LangGraph 工作流"""
        
        def chatbot(state: AgentState) -> AgentState:
            """聊天机器人节点 - 决定是否需要使用工具"""
            messages = state["messages"]
            
            # 添加系统提示词
            if not messages or not isinstance(messages[0], SystemMessage):
                messages = [SystemMessage(content=self.system_prompt)] + messages
                state["messages"] = messages
            
            response = self.llm.invoke(messages)
            state["messages"].append(response)
            
            return state
        
        def should_continue(state: AgentState) -> str:
            """判断是否需要继续使用工具"""
            messages = state["messages"]
            last_message = messages[-1]
            
            # 如果最后一条消息包含工具调用，转到工具节点
            if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
                return "tools"
            
            # 否则结束对话
            return "end"
        
        # 构建状态图
        workflow = StateGraph(AgentState)
        
        # 添加节点
        workflow.add_node("chatbot", chatbot)
        workflow.add_node("tools", self.tool_node)
        
        # 添加边
        workflow.add_edge(START, "chatbot")
        workflow.add_conditional_edges(
            "chatbot",
            should_continue,
            {"tools": "tools", "end": END}
        )
        workflow.add_edge("tools", "chatbot")
        
        return workflow.compile()
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """处理用户查询"""
        logger.info(f"处理用户查询: {user_query}")
        
        try:
            # 初始化状态
            initial_state = AgentState(
                messages=[HumanMessage(content=user_query)],
                user_query=user_query
            )
            
            # 执行工作流
            final_state = self.workflow.invoke(initial_state)
            
            # 提取最终回复
            messages = final_state["messages"]
            last_message = messages[-1] if messages else None
            
            if last_message and hasattr(last_message, 'content'):
                final_answer = last_message.content
            else:
                final_answer = "抱歉，无法生成回复。"
            
            return {
                "status": "success",
                "answer": final_answer,
                "conversation": [
                    {
                        "role": "user" if isinstance(msg, HumanMessage) else "assistant",
                        "content": msg.content
                    }
                    for msg in messages 
                    if isinstance(msg, (HumanMessage, AIMessage)) and hasattr(msg, 'content')
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
api_agent = APIAgent()

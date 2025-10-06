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

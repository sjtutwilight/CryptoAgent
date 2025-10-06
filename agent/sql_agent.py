"""
基于 LangGraph 的 SQL 代理系统
将自然语言查询转换为 SQL 并执行，返回 JSON 格式结果
"""
import json
import logging
from typing import Dict, Any, List, TypedDict
from typing_extensions import Annotated
from operator import add
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel, Field
import json_repair

from config import DEEPSEEK_CONFIG, DATABASE_SCHEMA, TAG_BITMAP_MAPPING
from database import db_manager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AgentState(TypedDict):
    """代理状态管理"""
    user_query: str                    # 用户原始查询
    intent: str                        # 查询意图
    sql_query: str                     # 生成的SQL查询
    query_result: List[Dict[str, Any]] # 查询结果
    formatted_result: Dict[str, Any]   # 格式化后的结果
    error_message: str                 # 错误信息
    step: str                          # 当前步骤

class SQLQueryRequest(BaseModel):
    """SQL查询请求模型"""
    sql: str = Field(description="生成的SQL查询语句")
    database_type: str = Field(description="数据库类型: postgres 或 clickhouse")
    explanation: str = Field(description="查询逻辑解释")

class AccountDetailResponse(BaseModel):
    """账户详情响应模型"""
    account_info: Dict[str, Any] = Field(description="账户基础信息")
    label_info: Dict[str, Any] = Field(description="标签信息")
    assets: List[Dict[str, Any]] = Field(description="资产持仓")
    transfer_history: List[Dict[str, Any]] = Field(description="转账历史")
    asset_stats: Dict[str, Any] = Field(description="资产统计")
    transfer_stats: Dict[str, Any] = Field(description="转账统计")

class SQLAgent:
    """SQL代理主类"""
    
    def __init__(self):
        # 初始化 DeepSeek 客户端
        self.llm = ChatOpenAI(
            api_key=DEEPSEEK_CONFIG["api_key"],
            base_url=DEEPSEEK_CONFIG["base_url"],
            model=DEEPSEEK_CONFIG["model"],
            temperature=0.1
        )
        
        # 构建工作流图
        self.workflow = self._build_workflow()
        
        # 系统提示词
        self.system_prompt = self._build_system_prompt()
    
    def _build_system_prompt(self) -> str:
        """构建系统提示词"""
        schema_desc = json.dumps(DATABASE_SCHEMA, ensure_ascii=False, indent=2)
        tag_desc = json.dumps(TAG_BITMAP_MAPPING, ensure_ascii=False, indent=2)
        
        return f"""你是一个专业的SQL代理助手，负责将自然语言查询转换为精确的SQL语句。

# 数据库架构信息
{schema_desc}

# 标签位图映射
{tag_desc}

# 关键业务逻辑
1. 账户详情查询涉及多个数据源：
   - PostgreSQL: 账户基础信息 (account表)
   - ClickHouse: 资产快照 (ch_account_balance_snapshot) 和交易历史 (ch_account_trade_fact)

2. 标签解析：tag_bitmap 是位标志，需要用位运算解析：
   - 1 (0001): fresh
   - 2 (0010): whale  
   - 4 (0100): smart
   - 8 (1000): cex

3. 资产查询要获取最新快照：
   ```sql
   WITH latest_snapshot AS (
       SELECT max(snapshot_id) AS max_snapshot_id 
       FROM ch_account_balance_snapshot
       WHERE account_id = ?
   )
   ```

4. 交易历史中 side 字段：'buy'表示买入，'sell'表示卖出

# 输出要求
- 为每个查询生成适当的SQL语句
- 明确指定数据库类型 (postgres/clickhouse)
- 提供查询逻辑的简要解释
- 确保SQL语法正确且高效

# 示例
用户查询："获取账户ID为1的账户详情"
需要生成多个SQL查询：
1. PostgreSQL: 获取账户基础信息和标签
2. ClickHouse: 获取最新资产快照
3. ClickHouse: 获取交易历史
"""

    def _build_workflow(self) -> StateGraph:
        """构建 LangGraph 工作流"""
        
        def analyze_intent(state: dict) -> dict:
            """分析用户查询意图"""
            user_query = state["user_query"]
            
            # 简单的意图识别
            if any(keyword in user_query.lower() for keyword in ["账户详情", "账户信息", "account detail"]):
                state["intent"] = "account_detail"
            elif any(keyword in user_query.lower() for keyword in ["资产", "持仓", "balance"]):
                state["intent"] = "account_assets"
            elif any(keyword in user_query.lower() for keyword in ["交易", "历史", "trade", "transfer"]):
                state["intent"] = "account_trades"
            else:
                state["intent"] = "general_query"
            
            state["step"] = "intent_analyzed"
            logger.info(f"识别查询意图: {state['intent']}")
            return state
        
        def generate_sql(state: dict) -> dict:
            """生成SQL查询"""
            try:
                user_query = state["user_query"]
                intent = state["intent"]
                
                # 根据意图选择不同的处理策略
                if intent == "account_detail":
                    state["sql_query"] = self._generate_account_detail_sql(user_query)
                else:
                    state["sql_query"] = self._generate_general_sql(user_query)
                
                state["step"] = "sql_generated"
                logger.info(f"生成SQL查询: {state['sql_query'][:100]}...")
                
            except Exception as e:
                state["error_message"] = f"SQL生成失败: {str(e)}"
                state["step"] = "error"
                logger.error(f"SQL生成失败: {e}")
            
            return state
        
        def execute_sql(state: dict) -> dict:
            """执行SQL查询"""
            try:
                sql_query = state["sql_query"]
                
                # 如果是复合查询（账户详情），需要执行多个查询
                if state["intent"] == "account_detail":
                    result = self._execute_account_detail_queries(sql_query)
                    # 将字典结果包装成列表格式以匹配状态类型
                    state["query_result"] = [result] if isinstance(result, dict) else result
                else:
                    # 单个查询
                    result = db_manager.execute_query(sql_query)
                    state["query_result"] = result if isinstance(result, list) else [result]
                
                state["step"] = "sql_executed"
                logger.info(f"SQL执行成功，返回 {len(state['query_result'])} 条记录")
                
            except Exception as e:
                state["error_message"] = f"SQL执行失败: {str(e)}"
                state["step"] = "error"
                state["query_result"] = []
                logger.error(f"SQL执行失败: {e}")
            
            return state
        
        def format_result(state: dict) -> dict:
            """格式化查询结果"""
            try:
                intent = state["intent"]
                query_result = state["query_result"]
                
                if intent == "account_detail" and query_result:
                    # 对于账户详情，取第一个结果（字典格式）
                    result_data = query_result[0] if isinstance(query_result, list) else query_result
                    state["formatted_result"] = self._format_account_detail_result(result_data)
                else:
                    state["formatted_result"] = {
                        "status": "success",
                        "data": query_result,
                        "count": len(query_result) if isinstance(query_result, list) else 1
                    }
                
                state["step"] = "result_formatted"
                logger.info("结果格式化完成")
                
            except Exception as e:
                state["error_message"] = f"结果格式化失败: {str(e)}"
                state["step"] = "error"
                logger.error(f"结果格式化失败: {e}")
            
            return state
        
        def handle_error(state: dict) -> dict:
            """处理错误"""
            state["formatted_result"] = {
                "status": "error",
                "error": state.get("error_message", "未知错误"),
                "step": state.get("step", "unknown")
            }
            return state
        
        # 构建状态图
        workflow = StateGraph(AgentState)
        
        # 添加节点
        workflow.add_node("analyze_intent", analyze_intent)
        workflow.add_node("generate_sql", generate_sql)
        workflow.add_node("execute_sql", execute_sql)
        workflow.add_node("format_result", format_result)
        workflow.add_node("handle_error", handle_error)
        
        # 添加边
        workflow.add_edge(START, "analyze_intent")
        workflow.add_edge("analyze_intent", "generate_sql")
        workflow.add_edge("generate_sql", "execute_sql")
        workflow.add_edge("execute_sql", "format_result")
        workflow.add_edge("format_result", END)
        workflow.add_edge("handle_error", END)
        
        # 添加条件边 - 错误处理
        def should_handle_error(state: dict) -> str:
            if state.get("step") == "error":
                return "handle_error"
            return "continue"
        
        workflow.add_conditional_edges(
            "generate_sql",
            should_handle_error,
            {"handle_error": "handle_error", "continue": "execute_sql"}
        )
        
        workflow.add_conditional_edges(
            "execute_sql", 
            should_handle_error,
            {"handle_error": "handle_error", "continue": "format_result"}
        )
        
        return workflow.compile()
    
    def _generate_account_detail_sql(self, user_query: str) -> str:
        """生成账户详情查询的SQL"""
        # 从用户查询中提取账户ID
        import re
        account_id_match = re.search(r'账户\s*(?:id|ID|Id)?\s*(?:为|是)?\s*(\d+)', user_query)
        if not account_id_match:
            account_id_match = re.search(r'(?:id|ID|Id)\s*(?:为|是|=)?\s*(\d+)', user_query)
        
        if not account_id_match:
            raise ValueError("无法从查询中提取账户ID")
        
        account_id = account_id_match.group(1)
        
        # 返回一个标记，表示这是账户详情的复合查询
        return f"ACCOUNT_DETAIL:{account_id}"
    
    def _generate_general_sql(self, user_query: str) -> str:
        """生成通用SQL查询"""
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=f"请为以下查询生成SQL语句：{user_query}")
        ]
        
        response = self.llm.invoke(messages)
        
        # 尝试解析为结构化响应
        try:
            # 尝试提取JSON格式的响应
            content = response.content
            if "```sql" in content:
                # 提取SQL代码块
                sql_start = content.find("```sql") + 6
                sql_end = content.find("```", sql_start)
                sql = content[sql_start:sql_end].strip()
                return sql
            elif "```" in content:
                # 提取普通代码块
                sql_start = content.find("```") + 3
                sql_end = content.find("```", sql_start)
                sql = content[sql_start:sql_end].strip()
                return sql
            else:
                # 直接返回内容
                return content.strip()
        except Exception as e:
            logger.warning(f"解析LLM响应失败: {e}")
            return response.content
    
    def _execute_account_detail_queries(self, sql_placeholder: str) -> List[Dict[str, Any]]:
        """执行账户详情的复合查询"""
        # 解析账户ID
        account_id = sql_placeholder.split(':')[1]
        
        results = {}
        
        # 1. 获取账户基础信息
        postgres_sql = """
        SELECT 
            id,
            chain_id,
            chain_name,
            address,
            entity,
            tag_bitmap,
            create_time,
            update_time
        FROM account
        WHERE id = %s
        """
        
        account_info = db_manager.execute_postgres_query(postgres_sql, (account_id,))
        results["account_info"] = account_info[0] if account_info else None
        
        # 2. 获取最新资产快照
        assets_sql = """
        WITH latest_snapshot AS (
            SELECT max(snapshot_id) AS max_snapshot_id 
            FROM ch_account_balance_snapshot
            WHERE account_id = %(account_id)s
        )
        SELECT 
            h.biz_id as token_id,
            h.biz_name,
            h.asset_type,
            h.amount,
            h.price_usd,
            h.value_usd
        FROM ch_account_balance_snapshot h
        INNER JOIN latest_snapshot l ON h.snapshot_id = l.max_snapshot_id
        WHERE h.account_id = %(account_id)s
          AND h.value_usd > 0
        ORDER BY h.value_usd DESC
        """
        
        assets = db_manager.execute_clickhouse_query(assets_sql, {"account_id": int(account_id)})
        results["assets"] = assets
        
        # 3. 获取交易历史（最近50条）
        trades_sql = """
        SELECT 
            block_time,
            block_id,
            tx_hash,
            side,
            
            token_id,
            qty,
            value_usd
        FROM ch_account_trade_fact
        WHERE account_id = %(account_id)s
        ORDER BY block_time DESC
        LIMIT 50
        """
        
        trades = db_manager.execute_clickhouse_query(trades_sql, {"account_id": int(account_id)})
        results["transfer_history"] = trades
        
        return results
    
    def _format_account_detail_result(self, query_result: Dict[str, Any]) -> Dict[str, Any]:
        """格式化账户详情结果"""
        account_info = query_result.get("account_info")
        assets = query_result.get("assets", [])
        transfers = query_result.get("transfer_history", [])
        
        if not account_info:
            return {
                "status": "error",
                "error": "账户不存在"
            }
        
        # 解析标签
        tag_bitmap = account_info.get("tag_bitmap", 0)
        labels = []
        for bit, label in TAG_BITMAP_MAPPING.items():
            if tag_bitmap & bit:
                labels.append(label)
        if not labels:
            labels.append("public")
        
        # 计算资产统计
        total_value_usd = sum(float(asset.get("value_usd", 0)) for asset in assets)
        asset_count = len(assets)
        top_asset = assets[0] if assets else None
        
        # 计算转账统计
        transfers_in = sum(1 for t in transfers if t.get("side") == "buy")
        transfers_out = sum(1 for t in transfers if t.get("side") == "sell")
        
        volume_in = sum(float(t.get("value_usd", 0)) for t in transfers if t.get("side") == "buy")
        volume_out = sum(float(t.get("value_usd", 0)) for t in transfers if t.get("side") == "sell")
        
        avg_transaction_value = (volume_in + volume_out) / len(transfers) if transfers else 0
        
        # 组装最终结果
        result = {
            "status": "success",
            "data": {
                "account_info": {
                    "id": account_info["id"],
                    "address": account_info["address"],
                    "entity": account_info.get("entity"),
                    "chain_name": account_info["chain_name"],
                    "created_at": str(account_info["create_time"])
                },
                "label_info": {
                    "labels": labels,
                    "tag_bitmap": tag_bitmap
                },
                "assets": [
                    {
                        "token_id": str(asset["token_id"]),
                        "symbol": asset["biz_name"],
                        "asset_type": asset["asset_type"],
                        "balance": str(asset["amount"]),
                        "price_usd": str(asset["price_usd"]),
                        "value_usd": str(asset["value_usd"])
                    }
                    for asset in assets
                ],
                "transfer_history": [
                    {
                        "timestamp": str(transfer["block_time"]),
                        "block_number": transfer["block_id"],
                        "tx_hash": transfer["tx_hash"],
                        "direction": "in" if transfer["side"] == "buy" else "out",
                        "token_id": str(transfer["token_id"]),
                        "amount": str(transfer["qty"]),
                        "value_usd": str(transfer["value_usd"])
                    }
                    for transfer in transfers
                ],
                "asset_stats": {
                    "total_value_usd": str(total_value_usd),
                    "asset_count": asset_count,
                    "top_asset_symbol": top_asset["biz_name"] if top_asset else "",
                    "top_asset_percentage": (float(top_asset["value_usd"]) / total_value_usd * 100) if top_asset and total_value_usd > 0 else 0
                },
                "transfer_stats": {
                    "total_transfers": len(transfers),
                    "transfers_in": transfers_in,
                    "transfers_out": transfers_out,
                    "total_volume_in": str(volume_in),
                    "total_volume_out": str(volume_out),
                    "avg_transaction_value": str(avg_transaction_value)
                }
            }
        }
        
        return result
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """处理用户查询"""
        logger.info(f"处理用户查询: {user_query}")
        
        # 初始化状态
        initial_state = {
            "user_query": user_query,
            "intent": "",
            "sql_query": "",
            "query_result": [],
            "formatted_result": {},
            "error_message": "",
            "step": "start"
        }
        
        # 执行工作流
        final_state = self.workflow.invoke(initial_state)
        
        return final_state["formatted_result"]

# 全局代理实例
sql_agent = SQLAgent()

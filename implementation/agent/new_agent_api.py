"""
新的API代理服务
基于LangGraph和API工具的智能助手
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from typing import Dict, Any
import traceback
import json

from simple_agent import simple_agent as api_agent
from api_tools import api_client
from conversation_manager import conversation_manager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建Flask应用
app = Flask(__name__)

# 启用CORS支持
CORS(app, origins=["http://localhost:3000", "http://localhost:3001", "http://127.0.0.1:3000"])

@app.route('/health', methods=['GET'])
def health_check():
    """健康检查接口"""
    return jsonify({
        "status": "healthy",
        "service": "API Agent",
        "version": "2.0.0"
    })

@app.route('/chat', methods=['POST'])
def chat():
    """智能对话接口"""
    try:
        # 获取请求数据
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({
                "status": "error",
                "error": "缺少查询参数"
            }), 400
        user_query = data['query']
        session_id = data.get("sessionId") or request.headers.get("X-Session-Id") or request.remote_addr or "default"
        reset_flag = data.get("reset") is True

        if reset_flag:
            conversation_manager.reset(session_id)

        logger.info(f"收到对话请求 session={session_id}: {user_query}")

        history_ctx = conversation_manager.get_history(session_id)

        # 调用API代理处理查询
        result = api_agent.process_query(
            user_query,
            history=history_ctx.get("turns"),
            summary=history_ctx.get("summary", "")
        )

        if result.get("status") == "success":
            conversation_manager.append_turn(session_id, "user", user_query)
            conversation_manager.append_turn(session_id, "assistant", result.get("answer", ""))

        result["sessionId"] = session_id

        return jsonify(result)
        
    except Exception as e:
        logger.error(f"对话处理失败: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "error": str(e),
            "answer": f"处理查询时发生错误: {str(e)}"
        }), 500

@app.route('/backend/status', methods=['GET'])
def backend_status():
    """检查后端API连接状态"""
    try:
        # 测试后端API连接
        result = api_client._make_request("GET", "/health")
        
        if result.get("status") == "error":
            return jsonify({
                "status": "error",
                "backend_connected": False,
                "error": result.get("message", "后端连接失败")
            })
        else:
            return jsonify({
                "status": "success", 
                "backend_connected": True,
                "backend_response": result
            })
            
    except Exception as e:
        logger.error(f"后端状态检查失败: {e}")
        return jsonify({
            "status": "error",
            "backend_connected": False,
            "error": str(e)
        })

@app.route('/tools', methods=['GET'])
def list_tools():
    """列出可用的工具"""
    from api_tools import AVAILABLE_TOOLS
    
    tools_info = []
    for tool in AVAILABLE_TOOLS:
        tools_info.append({
            "name": tool.name,
            "description": tool.description,
            "args_schema": tool.args_schema.model_json_schema() if hasattr(tool.args_schema, 'model_json_schema') else None
        })
    
    return jsonify({
        "status": "success",
        "tools": tools_info,
        "count": len(tools_info)
    })

@app.route('/examples', methods=['GET'])
def get_examples():
    """获取查询示例"""
    examples = [
        {
            "category": "代币查询",
            "examples": [
                "获取市值排名前10的代币",
                "查看代币ID为1的详细信息",
                "分析代币1的持有者分布",
                "获取代币1的PnL分析"
            ]
        },
        {
            "category": "账户查询", 
            "examples": [
                "查看账户ID为1的详情",
                "获取账户1的资产持仓",
                "分析账户1的交易历史"
            ]
        },
        {
            "category": "K线决策",
            "examples": [
                "比较 OKX 与 Binance 上 BTCUSDT 近30分钟(1m)的涨跌与成交量差异",
                "分析 ETHUSDT 在 5m 级别的 MACD/RSI 信号是否共振",
                "筛选 5m 周期内涨幅最大的前5个交易对并给出关键指标"
            ]
        },
        {
            "category": "永续监控",
            "examples": [
                "查看 Hyperliquid 平台永续面板中点差最低的交易对",
                "分析 BTCUSDT 永续近30分钟的执行面指标是否出现流动性恶化",
                "列出最近的永续异常信号并按严重等级排序"
            ]
        },
        {
            "category": "系统查询",
            "examples": [
                "检查系统状态",
                "列出所有可用工具"
            ]
        }
    ]
    
    return jsonify({
        "status": "success",
        "examples": examples
    })

def run_cli():
    """命令行交互接口"""
    print("🤖 AI 区块链数据分析助手启动")
    print("我可以帮您查询代币信息、账户详情、市场分析等数据")
    print("输入 'help' 查看示例，输入 'quit' 或 'exit' 退出\n")
    session_id = "cli"
    
    while True:
        try:
            user_input = input("👤 请输入查询: ").strip()
            
            if user_input.lower() in ['quit', 'exit', '退出']:
                print("👋 再见！")
                break
            
            if user_input.lower() == 'help':
                print("📖 查询示例:")
                print("• 获取市值排名前10的代币")
                print("• 查看代币ID为1的详细信息") 
                print("• 获取账户ID为1的详情")
                print("• 分析代币1的持有者分布")
                print("• 检查系统状态")
                print("-" * 50)
                continue

            if user_input.lower() == 'reset':
                conversation_manager.reset(session_id)
                print("🧹 会话记忆已清空")
                print("-" * 50)
                continue
            
            if not user_input:
                continue
            
            print("🔍 正在分析查询...")
            history_ctx = conversation_manager.get_history(session_id)
            result = api_agent.process_query(
                user_input,
                history=history_ctx.get("turns"),
                summary=history_ctx.get("summary", "")
            )
            
            if result.get("status") == "success":
                conversation_manager.append_turn(session_id, "user", user_input)
                conversation_manager.append_turn(session_id, "assistant", result.get("answer", ""))
                print("🎯 分析结果:")
                print(result["answer"])
            else:
                print("❌ 查询失败:")
                print(result.get("error", "未知错误"))
            
            print("-" * 50)
            
        except KeyboardInterrupt:
            print("\n👋 再见！")
            break
        except Exception as e:
            print(f"❌ 处理失败: {e}")
            print("-" * 50)

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'cli':
        # 命令行模式
        run_cli()
    else:
        # Web API模式
        print("🚀 启动AI区块链数据分析助手...")
        print("🌐 访问 http://localhost:8888/health 检查服务状态")
        print("📖 API文档:")
        print("  POST /chat - 智能对话")
        print("  GET  /backend/status - 后端连接状态")
        print("  GET  /tools - 可用工具列表")
        print("  GET  /examples - 查询示例")
        
        app.run(host='0.0.0.0', port=8888, debug=True)

"""
SQL代理API接口
提供RESTful API和命令行接口
"""
from flask import Flask, request, jsonify
import logging
from typing import Dict, Any
import traceback
import json

from sql_agent import sql_agent
from database import db_manager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建Flask应用
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """健康检查接口"""
    return jsonify({
        "status": "healthy",
        "service": "SQL Agent API",
        "version": "1.0.0"
    })

@app.route('/query', methods=['POST'])
def process_query():
    """处理自然语言查询"""
    try:
        # 获取请求数据
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({
                "status": "error",
                "error": "缺少查询参数"
            }), 400
        
        user_query = data['query']
        logger.info(f"收到查询请求: {user_query}")
        
        # 调用SQL代理处理查询
        result = sql_agent.process_query(user_query)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"查询处理失败: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/account/<int:account_id>', methods=['GET'])
def get_account_detail(account_id: int):
    """获取账户详情（REST风格接口）"""
    try:
        # 构造自然语言查询
        user_query = f"获取账户ID为{account_id}的账户详情"
        logger.info(f"REST接口查询: {user_query}")
        
        # 调用SQL代理处理查询
        result = sql_agent.process_query(user_query)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"账户详情查询失败: {e}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/query/sql', methods=['POST'])
def execute_raw_sql():
    """执行原始SQL查询（调试用）"""
    try:
        data = request.get_json()
        if not data or 'sql' not in data:
            return jsonify({
                "status": "error",
                "error": "缺少SQL查询参数"
            }), 400
        
        sql = data['sql']
        database_type = data.get('database_type', 'auto')
        
        logger.info(f"执行原始SQL: {sql}")
        
        # 执行SQL查询
        result = db_manager.execute_query(sql, database_type)
        
        return jsonify({
            "status": "success",
            "data": result,
            "count": len(result) if isinstance(result, list) else 1
        })
        
    except Exception as e:
        logger.error(f"SQL执行失败: {e}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/database/status', methods=['GET'])
def database_status():
    """检查数据库连接状态"""
    status = {
        "postgres": False,
        "clickhouse": False
    }
    
    try:
        # 测试PostgreSQL连接
        db_manager.execute_postgres_query("SELECT 1")
        status["postgres"] = True
    except Exception as e:
        logger.warning(f"PostgreSQL连接失败: {e}")
    
    try:
        # 测试ClickHouse连接
        db_manager.execute_clickhouse_query("SELECT 1")
        status["clickhouse"] = True
    except Exception as e:
        logger.warning(f"ClickHouse连接失败: {e}")
    
    return jsonify({
        "status": "success",
        "databases": status,
        "all_connected": all(status.values())
    })

def run_cli():
    """命令行交互接口"""
    print("🤖 SQL代理助手启动")
    print("输入自然语言查询，例如：'获取账户ID为1的账户详情'")
    print("输入 'quit' 或 'exit' 退出\n")
    
    while True:
        try:
            user_input = input("👤 请输入查询: ").strip()
            
            if user_input.lower() in ['quit', 'exit', '退出']:
                print("👋 再见！")
                break
            
            if not user_input:
                continue
            
            print("🔍 正在处理查询...")
            result = sql_agent.process_query(user_input)
            
            # 美化输出结果
            print("📊 查询结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
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
        print("🚀 启动SQL代理API服务器...")
        print("🌐 访问 http://localhost:8080/health 检查服务状态")
        print("📖 API文档:")
        print("  POST /query - 自然语言查询")
        print("  GET  /account/<id> - 获取账户详情")
        print("  POST /query/sql - 执行原始SQL")
        print("  GET  /database/status - 数据库状态检查")
        
        app.run(host='0.0.0.0', port=8080, debug=True)

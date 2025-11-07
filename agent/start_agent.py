"""
启动AI代理API服务
"""
from new_agent_api import app
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == '__main__':
    print("🚀 启动AI区块链数据分析助手...")
    print("🌐 API服务地址: http://localhost:8888")
    print("📖 可用接口:")
    print("  POST /chat - 智能对话")
    print("  GET  /backend/status - 后端连接状态")
    print("  GET  /tools - 可用工具列表")
    print("  GET  /examples - 查询示例")
    print("  GET  /health - 健康检查")
    print("\n" + "="*50)
    
    app.run(
        host='0.0.0.0', 
        port=8888, 
        debug=False,  # 生产环境关闭debug
        threaded=True  # 支持多线程
    )









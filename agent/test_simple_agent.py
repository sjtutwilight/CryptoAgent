"""
简化版代理测试脚本
"""
import json
import time
from simple_agent import simple_agent

def test_simple_queries():
    """测试简单查询"""
    print("🧪 测试简化版代理...")
    
    test_queries = [
        "检查系统状态",
        "获取代币列表的前5个",
        "查看代币ID为1的详细信息",
        "获取账户ID为1的详情"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n📝 测试 {i}: {query}")
        print("-" * 40)
        
        start_time = time.time()
        try:
            result = simple_agent.process_query(query)
            end_time = time.time()
            
            print(f"⏱️  执行时间: {end_time - start_time:.2f}秒")
            
            if result.get("status") == "success":
                print("✅ 查询成功")
                print(f"💬 回复: {result['answer']}")
            else:
                print(f"❌ 查询失败: {result.get('error')}")
                
        except Exception as e:
            print(f"❌ 测试异常: {e}")
            import traceback
            traceback.print_exc()
        
        print("=" * 50)

if __name__ == "__main__":
    print("🚀 简化版代理测试")
    test_simple_queries()




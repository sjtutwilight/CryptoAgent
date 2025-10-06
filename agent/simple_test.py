"""
简单测试脚本 - 验证系统基本功能
"""
import json
from sql_agent import sql_agent

def test_basic_functionality():
    """测试基本功能"""
    print("🧪 测试 SQL 代理基本功能...")
    
    # 测试查询
    test_query = "获取账户ID为1的账户详情"
    
    try:
        print(f"📝 测试查询: {test_query}")
        result = sql_agent.process_query(test_query)
        
        print("📊 查询结果:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        if result.get("status") == "success":
            print("✅ 测试成功！")
        else:
            print(f"⚠️ 测试返回错误: {result.get('error')}")
            
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_basic_functionality()
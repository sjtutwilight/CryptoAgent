"""
快速测试脚本 - 验证基本导入和功能
"""

def test_imports():
    """测试导入"""
    print("🔧 测试模块导入...")
    
    try:
        from config import DEEPSEEK_CONFIG, BACKEND_API_CONFIG
        print("✅ config 导入成功")
        
        from api_tools import api_client, get_health_status
        print("✅ api_tools 导入成功")
        
        from api_agent import api_agent
        print("✅ api_agent 导入成功")
        
        return True
        
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_simple_query():
    """测试简单查询"""
    print("\n🧪 测试简单查询...")
    
    try:
        from api_agent import api_agent
        
        # 简单测试查询
        result = api_agent.process_query("检查系统状态")
        
        print("📊 查询结果:")
        import json
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        return True
        
    except Exception as e:
        print(f"❌ 查询测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 快速测试启动")
    print("=" * 40)
    
    if test_imports():
        test_simple_query()
    
    print("\n✅ 测试完成")





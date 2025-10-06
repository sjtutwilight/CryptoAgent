"""
API代理测试脚本
测试基于API工具的智能代理
"""
import json
import time
from api_agent import api_agent
from api_tools import api_client

def test_backend_connection():
    """测试后端API连接"""
    print("🔗 测试后端API连接...")
    
    try:
        result = api_client._make_request("GET", "/health")
        if result.get("status") == "error":
            print(f"❌ 后端连接失败: {result.get('message')}")
            return False
        else:
            print("✅ 后端API连接成功")
            print(f"   响应: {json.dumps(result, ensure_ascii=False)}")
            return True
    except Exception as e:
        print(f"❌ 后端连接异常: {e}")
        return False

def test_agent_queries():
    """测试代理查询功能"""
    print("\n🧪 测试代理查询功能...")
    
    test_queries = [
        "检查系统状态",
        "获取代币列表",
        "查看代币ID为1的详细信息",
        "获取账户ID为1的详情",
        "分析代币1的持有者分布"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n📝 测试 {i}: {query}")
        print("-" * 40)
        
        start_time = time.time()
        try:
            result = api_agent.process_query(query)
            end_time = time.time()
            
            print(f"⏱️  执行时间: {end_time - start_time:.2f}秒")
            
            if result.get("status") == "success":
                print("✅ 查询成功")
                print(f"💬 回复: {result['answer']}")
                
                # 显示对话历史
                if result.get("conversation"):
                    print("\n📖 对话历史:")
                    for msg in result["conversation"]:
                        role = "👤 用户" if msg["role"] == "user" else "🤖 助手"
                        print(f"{role}: {msg['content'][:200]}...")
            else:
                print(f"❌ 查询失败: {result.get('error')}")
                
        except Exception as e:
            print(f"❌ 测试异常: {e}")
            import traceback
            traceback.print_exc()
        
        print("=" * 50)

def test_specific_api_calls():
    """测试特定的API调用"""
    print("\n🔧 测试特定API调用...")
    
    # 测试各个API工具
    from api_tools import (
        get_health_status, get_token_list, get_token_overview,
        get_account_detail, get_token_distribution, get_token_pnl
    )
    
    api_tests = [
        ("健康检查", lambda: get_health_status()),
        ("代币列表", lambda: get_token_list(page=1, page_size=5)),
        ("代币概览", lambda: get_token_overview(1)),
        ("账户详情", lambda: get_account_detail(1)),
        ("代币分布", lambda: get_token_distribution(1)),
        ("代币PnL", lambda: get_token_pnl(1))
    ]
    
    for test_name, test_func in api_tests:
        print(f"\n🔍 测试 {test_name}...")
        try:
            result = test_func()
            
            if isinstance(result, dict) and result.get("status") == "error":
                print(f"❌ API调用失败: {result.get('message')}")
            else:
                print("✅ API调用成功")
                # 只显示部分结果
                result_str = json.dumps(result, ensure_ascii=False)
                if len(result_str) > 200:
                    print(f"📊 结果预览: {result_str[:200]}...")
                else:
                    print(f"📊 完整结果: {result_str}")
                    
        except Exception as e:
            print(f"❌ 测试异常: {e}")

def test_error_handling():
    """测试错误处理"""
    print("\n🧪 测试错误处理...")
    
    error_test_cases = [
        "",  # 空查询
        "无法识别的随机查询内容",  # 无法识别的查询
        "获取不存在的代币999999的信息",  # 不存在的资源
    ]
    
    for query in error_test_cases:
        print(f"\n📝 测试错误情况: '{query}'")
        
        try:
            result = api_agent.process_query(query)
            print(f"📊 处理结果: {json.dumps(result, ensure_ascii=False, indent=2)}")
            
        except Exception as e:
            print(f"❌ 异常处理: {e}")

def run_interactive_test():
    """交互式测试"""
    print("\n🎮 交互式测试模式")
    print("输入查询进行测试，输入 'quit' 退出")
    
    while True:
        try:
            user_input = input("\n👤 请输入测试查询: ").strip()
            
            if user_input.lower() in ['quit', 'exit', '退出']:
                print("👋 测试结束！")
                break
            
            if not user_input:
                continue
            
            print("🔍 处理中...")
            result = api_agent.process_query(user_input)
            
            print("📊 测试结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
            
        except KeyboardInterrupt:
            print("\n👋 测试结束！")
            break
        except Exception as e:
            print(f"❌ 测试失败: {e}")

if __name__ == '__main__':
    import sys
    
    print("🚀 API代理测试套件")
    print("=" * 50)
    
    # 首先测试后端连接
    if not test_backend_connection():
        print("\n⚠️  后端API连接失败，某些测试可能会失败")
        print("请确保后端服务运行在 http://localhost:8080")
    
    if len(sys.argv) > 1:
        test_mode = sys.argv[1]
        if test_mode == "agent":
            test_agent_queries()
        elif test_mode == "api":
            test_specific_api_calls()
        elif test_mode == "error":
            test_error_handling()
        elif test_mode == "interactive":
            run_interactive_test()
        else:
            print("可用的测试模式: agent, api, error, interactive")
    else:
        # 运行所有测试
        test_agent_queries()
        test_specific_api_calls()
        test_error_handling()
        
        # 询问是否进行交互测试
        response = input("\n🤔 是否进行交互式测试? (y/n): ").strip().lower()
        if response in ['y', 'yes', '是']:
            run_interactive_test()
    
    print("\n🎉 测试完成！")




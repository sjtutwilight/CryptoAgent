"""
SQL代理测试脚本
测试各种查询场景
"""
import json
import time
from sql_agent import sql_agent
from database import db_manager

def test_database_connections():
    """测试数据库连接"""
    print("🔗 测试数据库连接...")
    
    # 测试PostgreSQL
    try:
        result = db_manager.execute_postgres_query("SELECT version()")
        print("✅ PostgreSQL 连接成功")
    except Exception as e:
        print(f"❌ PostgreSQL 连接失败: {e}")
    
    # 测试ClickHouse
    try:
        result = db_manager.execute_clickhouse_query("SELECT version()")
        print("✅ ClickHouse 连接成功")
    except Exception as e:
        print(f"❌ ClickHouse 连接失败: {e}")
    
    print("-" * 50)

def test_account_detail_query():
    """测试账户详情查询"""
    print("🧪 测试账户详情查询...")
    
    test_queries = [
        "获取账户ID为1的账户详情",
        "查询账户1的详细信息",
        "account detail for id 1",
        "账户 1 的信息"
    ]
    
    for query in test_queries:
        print(f"\n📝 测试查询: {query}")
        
        start_time = time.time()
        try:
            result = sql_agent.process_query(query)
            end_time = time.time()
            
            print(f"⏱️  执行时间: {end_time - start_time:.2f}秒")
            print("📊 查询结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
            
            # 检查结果结构
            if result.get("status") == "success":
                data = result.get("data", {})
                required_fields = ["account_info", "label_info", "assets", "transfer_history", "asset_stats", "transfer_stats"]
                missing_fields = [field for field in required_fields if field not in data]
                
                if missing_fields:
                    print(f"⚠️  缺少字段: {missing_fields}")
                else:
                    print("✅ 结果结构完整")
            else:
                print(f"❌ 查询失败: {result.get('error')}")
                
        except Exception as e:
            print(f"❌ 测试失败: {e}")
        
        print("-" * 30)

def test_general_queries():
    """测试通用查询"""
    print("🧪 测试通用查询...")
    
    test_queries = [
        "查询所有账户",
        "获取最新的交易记录",
        "统计资产总数",
        "查找标签为whale的账户"
    ]
    
    for query in test_queries:
        print(f"\n📝 测试查询: {query}")
        
        try:
            result = sql_agent.process_query(query)
            print("📊 查询结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
            
        except Exception as e:
            print(f"❌ 测试失败: {e}")
        
        print("-" * 30)

def test_error_handling():
    """测试错误处理"""
    print("🧪 测试错误处理...")
    
    error_test_cases = [
        "",  # 空查询
        "获取不存在的账户999999的详情",  # 不存在的账户
        "无效的查询语句",  # 无法解析的查询
        "账户详情但没有指定ID"  # 缺少必要参数
    ]
    
    for query in error_test_cases:
        print(f"\n📝 测试错误情况: '{query}'")
        
        try:
            result = sql_agent.process_query(query)
            print("📊 错误处理结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
            
            if result.get("status") == "error":
                print("✅ 正确返回错误状态")
            else:
                print("⚠️  预期返回错误但实际成功")
                
        except Exception as e:
            print(f"❌ 异常处理失败: {e}")
        
        print("-" * 30)

def test_performance():
    """测试性能"""
    print("🧪 测试性能...")
    
    query = "获取账户ID为1的账户详情"
    times = []
    
    for i in range(5):
        print(f"第 {i+1} 次测试...")
        start_time = time.time()
        
        try:
            result = sql_agent.process_query(query)
            end_time = time.time()
            
            duration = end_time - start_time
            times.append(duration)
            
            status = "✅" if result.get("status") == "success" else "❌"
            print(f"{status} 执行时间: {duration:.2f}秒")
            
        except Exception as e:
            print(f"❌ 测试失败: {e}")
    
    if times:
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"\n📈 性能统计:")
        print(f"  平均时间: {avg_time:.2f}秒")
        print(f"  最短时间: {min_time:.2f}秒")
        print(f"  最长时间: {max_time:.2f}秒")
    
    print("-" * 50)

def run_all_tests():
    """运行所有测试"""
    print("🚀 SQL代理测试套件启动")
    print("=" * 50)
    
    test_database_connections()
    test_account_detail_query()
    test_general_queries()
    test_error_handling()
    test_performance()
    
    print("🎉 所有测试完成！")

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        if test_name == "db":
            test_database_connections()
        elif test_name == "account":
            test_account_detail_query()
        elif test_name == "general":
            test_general_queries()
        elif test_name == "error":
            test_error_handling()
        elif test_name == "performance":
            test_performance()
        else:
            print("可用的测试: db, account, general, error, performance")
    else:
        run_all_tests()





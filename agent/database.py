"""
数据库连接工具
"""
import psycopg2
import psycopg2.extras
from clickhouse_driver import Client as ClickHouseClient
from typing import List, Dict, Any, Optional
import logging
from config import POSTGRES_CONFIG, CLICKHOUSE_CONFIG

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self):
        self.postgres_conn = None
        self.clickhouse_client = None
        
    def connect_postgres(self):
        """连接 PostgreSQL"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=POSTGRES_CONFIG["host"],
                port=POSTGRES_CONFIG["port"],
                database=POSTGRES_CONFIG["database"],
                user=POSTGRES_CONFIG["user"],
                password=POSTGRES_CONFIG["password"]
            )
            logger.info("PostgreSQL 连接成功")
            return True
        except Exception as e:
            logger.error(f"PostgreSQL 连接失败: {e}")
            return False
    
    def connect_clickhouse(self):
        """连接 ClickHouse"""
        try:
            self.clickhouse_client = ClickHouseClient(
                host=CLICKHOUSE_CONFIG["host"],
                port=CLICKHOUSE_CONFIG["port"],
                database=CLICKHOUSE_CONFIG["database"],
                user=CLICKHOUSE_CONFIG["user"],
                password=CLICKHOUSE_CONFIG["password"]
            )
            # 测试连接
            self.clickhouse_client.execute("SELECT 1")
            logger.info("ClickHouse 连接成功")
            return True
        except Exception as e:
            logger.error(f"ClickHouse 连接失败: {e}")
            return False
    
    def execute_postgres_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """执行 PostgreSQL 查询"""
        try:
            if not self.postgres_conn or self.postgres_conn.closed:
                if not self.connect_postgres():
                    raise Exception("无法连接到 PostgreSQL")
            
            with self.postgres_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(sql, params)
                
                # 如果是查询语句，返回结果
                if sql.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
                else:
                    # 如果是更新/插入/删除语句
                    self.postgres_conn.commit()
                    return [{"affected_rows": cursor.rowcount}]
                    
        except Exception as e:
            logger.error(f"PostgreSQL 查询执行失败: {e}")
            if self.postgres_conn:
                self.postgres_conn.rollback()
            raise e
    
    def execute_clickhouse_query(self, sql: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """执行 ClickHouse 查询"""
        try:
            if not self.clickhouse_client:
                if not self.connect_clickhouse():
                    raise Exception("无法连接到 ClickHouse")
            
            # 执行查询并获取列名
            result = self.clickhouse_client.execute(sql, params or {}, with_column_types=True)
            
            if isinstance(result, tuple) and len(result) == 2:
                rows, columns = result
                column_names = [col[0] for col in columns]
                
                # 将结果转换为字典列表
                return [dict(zip(column_names, row)) for row in rows]
            else:
                # 如果没有返回结果（如插入/更新语句）
                return [{"status": "success"}]
                
        except Exception as e:
            logger.error(f"ClickHouse 查询执行失败: {e}")
            raise e
    
    def execute_query(self, sql: str, database_type: str = "auto", params: Optional[Any] = None) -> List[Dict[str, Any]]:
        """
        智能执行查询
        database_type: "postgres", "clickhouse", "auto"
        """
        # 自动检测数据库类型
        if database_type == "auto":
            sql_upper = sql.strip().upper()
            if any(table in sql_upper for table in ["CH_ACCOUNT_", "CH_TOKEN_"]):
                database_type = "clickhouse"
            else:
                database_type = "postgres"
        
        logger.info(f"执行 {database_type} 查询: {sql[:100]}...")
        
        if database_type == "postgres":
            return self.execute_postgres_query(sql, params)
        elif database_type == "clickhouse":
            return self.execute_clickhouse_query(sql, params)
        else:
            raise ValueError(f"不支持的数据库类型: {database_type}")
    
    def close_connections(self):
        """关闭所有连接"""
        if self.postgres_conn:
            self.postgres_conn.close()
            logger.info("PostgreSQL 连接已关闭")
        
        if self.clickhouse_client:
            self.clickhouse_client.disconnect()
            logger.info("ClickHouse 连接已关闭")

# 全局数据库管理器实例
db_manager = DatabaseManager()




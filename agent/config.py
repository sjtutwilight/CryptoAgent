"""
数据库连接配置
"""
import os
from typing import Dict, Any

# DeepSeek API 配置
DEEPSEEK_CONFIG = {
    "api_key": "sk-656d1a94e7ef4335a1a0b592bdd3d5f1",
    "base_url": "https://api.deepseek.com",
    "model": "deepseek-chat"
}

# PostgreSQL 数据库配置
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "twilight"),
    "user": os.getenv("POSTGRES_USER", "twilight"),
    "password": os.getenv("POSTGRES_PASSWORD", "twilight123")
}

# ClickHouse 数据库配置
CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "9000")),
    "database": os.getenv("CLICKHOUSE_DB", "default"),
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "")
}

# 数据库表结构模式
DATABASE_SCHEMA = {
    "postgresql": {
        "account": {
            "description": "账户基础信息表",
            "columns": {
                "id": "账户唯一ID (SERIAL PRIMARY KEY)",
                "chain_id": "链ID (INTEGER)",
                "chain_name": "链名称 (VARCHAR(100))",
                "address": "账户地址 (VARCHAR(128))",
                "entity": "实体名称 (VARCHAR(255))",
                "tag_bitmap": "标签位图 (INTEGER, 位标志: 1=fresh, 2=whale, 4=smart, 8=cex)",
                "create_time": "创建时间 (TIMESTAMP)",
                "update_time": "更新时间 (TIMESTAMP)"
            }
        }
    },
    "clickhouse": {
        "ch_account_trade_fact": {
            "description": "账户交易事实表",
            "columns": {
                "chain_id": "链ID (UInt32)",
                "token_id": "代币ID (UInt64)",
                "account_id": "账户ID (UInt64)",
                "account_address": "账户地址 (String)",
                "side": "交易方向 ('buy' | 'sell')",
                "pair_id": "交易对ID (UInt64)",
                "pair_address": "交易对地址 (String)",
                "block_time": "区块时间 (DateTime)",
                "block_id": "区块号 (UInt64)",
                "tx_hash": "交易哈希 (String)",
                "log_index": "日志索引 (UInt32)",
                "qty": "数量 (Decimal(38,18))",
                "price_usd": "USD价格 (Decimal(38,18))",
                "value_usd": "USD价值 (Decimal(38,18))",
                "label_mask": "标签掩码 (UInt16)"
            }
        },
        "ch_account_balance_snapshot": {
            "description": "账户余额快照表",
            "columns": {
                "snapshot_id": "快照ID (UInt64)",
                "account_id": "账户ID (UInt64)",
                "account_address": "账户地址 (String)",
                "asset_type": "资产类型 (String)",
                "biz_id": "业务ID (UInt64, 通常是token_id)",
                "biz_name": "业务名称 (String, 通常是代币符号)",
                "observed_time": "观察时间 (DateTime)",
                "block_id": "区块号 (UInt64)",
                "amount": "数量 (Decimal(38,18))",
                "price_usd": "USD价格 (Decimal(38,18))",
                "value_usd": "USD价值 (Decimal(38,18))",
                "label_mask": "标签掩码 (UInt16)"
            }
        }
    }
}

# 标签位图解释
TAG_BITMAP_MAPPING = {
    1: "fresh",     # 新手
    2: "whale",     # 巨鲸
    4: "smart",     # 聪明钱
    8: "cex"        # 中心化交易所
}

# 后端API配置
BACKEND_API_CONFIG = {
    "base_url": os.getenv("BACKEND_API_URL", "http://localhost:8088/api/v1"),
    "timeout": int(os.getenv("API_TIMEOUT", "30"))
}

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Token Holders 数据导入作业
从 JSON 文件读取 token holders 数据，转换为 Parquet 格式并写入 Paimon
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, from_unixtime,
    regexp_extract, input_file_name, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType, DecimalType
)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Token Holders 数据导入作业')
    parser.add_argument(
        '--input-path',
        required=True,
        help='输入 JSON 文件路径或目录 (例如: /tmp/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca/)'
    )
    parser.add_argument(
        '--warehouse',
        default='s3a://paimon-warehouse/wh',
        help='Paimon warehouse 路径'
    )
    parser.add_argument(
        '--database',
        default='crypto_analytics',
        help='目标数据库名称'
    )
    parser.add_argument(
        '--table',
        default='token_holders_snapshot',
        help='目标表名称'
    )
    parser.add_argument(
        '--chain-id',
        type=int,
        help='链 ID (如果未指定，从路径中提取)'
    )
    parser.add_argument(
        '--token-address',
        help='Token 地址 (如果未指定，从路径中提取)'
    )
    parser.add_argument(
        '--snapshot-date',
        help='快照日期 (格式: YYYY-MM-DD，默认为今天)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='仅打印数据预览，不写入 Paimon'
    )
    return parser.parse_args()


def create_spark_session(warehouse_path):
    """创建 Spark Session"""
    return SparkSession.builder \
        .appName("TokenHoldersImport") \
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog") \
        .config("spark.sql.catalog.paimon.warehouse", warehouse_path) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions") \
        .getOrCreate()


def extract_metadata_from_path(input_path):
    """从输入路径提取 chain_id 和 token_address"""
    # 路径格式: /tmp/dune/token-holders/{chain_id}/{address}/
    parts = Path(input_path).parts
    
    chain_id = None
    token_address = None
    
    # 查找 token-holders 后的两个部分
    try:
        idx = parts.index('token-holders')
        if len(parts) > idx + 2:
            chain_id = int(parts[idx + 1])
            token_address = parts[idx + 2].lower()
    except (ValueError, IndexError):
        pass
    
    return chain_id, token_address


def define_schema():
    """定义 Token Holders 数据 Schema"""
    return StructType([
        StructField("wallet_address", StringType(), False),
        StructField("balance", StringType(), False),  # 使用字符串避免精度损失
        StructField("first_acquired", StringType(), True),
        StructField("has_initiated_transfer", BooleanType(), True)
    ])


def create_database_and_table(spark, database, table_name):
    """创建数据库和表"""
    # 创建数据库
    spark.sql(f"CREATE DATABASE IF NOT EXISTS paimon.{database}")
    print(f"✅ 数据库 paimon.{database} 已就绪")
    
    # 创建表 (如果不存在)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS paimon.{database}.{table_name} (
        wallet_address STRING NOT NULL COMMENT '钱包地址',
        balance DECIMAL(38, 0) NOT NULL COMMENT 'Token余额 (最小单位)',
        balance_readable DECIMAL(38, 18) COMMENT 'Token余额 (可读格式，假设18位小数)',
        first_acquired TIMESTAMP COMMENT '首次获得时间',
        has_initiated_transfer BOOLEAN COMMENT '是否发起过转账',
        chain_id INT NOT NULL COMMENT '链ID',
        token_address STRING NOT NULL COMMENT 'Token合约地址',
        snapshot_date DATE NOT NULL COMMENT '快照日期',
        snapshot_timestamp TIMESTAMP NOT NULL COMMENT '快照时间戳',
        data_source STRING COMMENT '数据来源',
        PRIMARY KEY (chain_id, token_address, wallet_address, snapshot_date) NOT ENFORCED
    ) PARTITIONED BY (chain_id, snapshot_date)
    TBLPROPERTIES (
        'bucket' = '4',
        'bucket-key' = 'wallet_address',
        'write-mode' = 'append-only',
        'changelog-producer' = 'none'
    )
    """
    
    spark.sql(create_table_sql)
    print(f"✅ 表 paimon.{database}.{table_name} 已就绪")


def process_data(spark, args):
    """处理数据"""
    # 提取元数据
    chain_id = args.chain_id
    token_address = args.token_address
    
    if not chain_id or not token_address:
        extracted_chain_id, extracted_token_address = extract_metadata_from_path(args.input_path)
        chain_id = chain_id or extracted_chain_id
        token_address = token_address or extracted_token_address
    
    if not chain_id or not token_address:
        raise ValueError("无法确定 chain_id 或 token_address，请通过参数指定或确保路径格式正确")
    
    # 确定快照日期
    snapshot_date = args.snapshot_date or datetime.now().strftime('%Y-%m-%d')
    
    print(f"📊 处理参数:")
    print(f"  - 输入路径: {args.input_path}")
    print(f"  - Chain ID: {chain_id}")
    print(f"  - Token Address: {token_address}")
    print(f"  - 快照日期: {snapshot_date}")
    
    # 读取 JSON 文件
    schema = define_schema()
    df = spark.read.schema(schema).json(args.input_path)
    
    print(f"✅ 读取到 {df.count()} 条记录")
    
    # 数据转换
    df_transformed = df \
        .withColumn("balance_decimal", col("balance").cast(DecimalType(38, 0))) \
        .withColumn("balance_readable", (col("balance_decimal") / 1e18).cast(DecimalType(38, 18))) \
        .withColumn("first_acquired_ts", to_timestamp(col("first_acquired"))) \
        .withColumn("chain_id", lit(chain_id)) \
        .withColumn("token_address", lit(token_address.lower())) \
        .withColumn("snapshot_date", lit(snapshot_date).cast("date")) \
        .withColumn("snapshot_timestamp", current_timestamp()) \
        .withColumn("data_source", lit("dune_api")) \
        .select(
            col("wallet_address").cast("string"),
            col("balance_decimal").alias("balance"),
            col("balance_readable"),
            col("first_acquired_ts").alias("first_acquired"),
            col("has_initiated_transfer"),
            col("chain_id"),
            col("token_address"),
            col("snapshot_date"),
            col("snapshot_timestamp"),
            col("data_source")
        )
    
    # 预览数据
    print("\n📋 数据预览 (前20条):")
    df_transformed.show(20, truncate=False)
    
    print("\n📊 数据统计:")
    df_transformed.select(
        "balance_readable"
    ).summary("count", "min", "max", "mean").show()
    
    if args.dry_run:
        print("\n⚠️  Dry Run 模式，不写入数据")
        return df_transformed
    
    # 创建数据库和表
    create_database_and_table(spark, args.database, args.table)
    
    # 写入 Paimon
    print(f"\n💾 写入数据到 paimon.{args.database}.{args.table}...")
    
    df_transformed.write \
        .format("paimon") \
        .mode("append") \
        .save(f"paimon.{args.database}.{args.table}")
    
    print("✅ 数据写入完成")
    
    # 验证写入
    result_count = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM paimon.{args.database}.{args.table}
        WHERE chain_id = {chain_id} 
          AND token_address = '{token_address}'
          AND snapshot_date = '{snapshot_date}'
    """).collect()[0]['cnt']
    
    print(f"✅ 验证: 表中有 {result_count} 条记录 (chain_id={chain_id}, token_address={token_address}, snapshot_date={snapshot_date})")
    
    return df_transformed


def main():
    """主函数"""
    args = parse_args()
    
    print("=" * 80)
    print("Token Holders 数据导入作业")
    print("=" * 80)
    
    # 创建 Spark Session
    spark = create_spark_session(args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 处理数据
        process_data(spark, args)
        
        print("\n" + "=" * 80)
        print("✅ 作业执行成功")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ 作业执行失败: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


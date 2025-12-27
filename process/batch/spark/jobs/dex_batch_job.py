#!/usr/bin/env python3
"""Spark batch pipeline for DEX mock data -> Paimon ODS/DWD -> StarRocks DWS."""
import argparse
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Token 指标宽表 Schema，保持与 StarRocks 目标表字段顺序一致
TOKEN_METRIC_SCHEMA = T.StructType([
    T.StructField("token_id", T.LongType()),
    T.StructField("time_window", T.StringType()),
    T.StructField("end_time", T.TimestampType()),
    T.StructField("tag", T.StringType()),
    T.StructField("txcnt", T.LongType()),
    T.StructField("buy_count", T.LongType()),
    T.StructField("sell_count", T.LongType()),
    T.StructField("volume_usd", T.DecimalType(38, 18)),
    T.StructField("buy_volume_usd", T.DecimalType(38, 18)),
    T.StructField("sell_volume_usd", T.DecimalType(38, 18)),
    T.StructField("buy_pressure_usd", T.DecimalType(38, 18)),
    T.StructField("token_price_usd", T.DecimalType(38, 18)),
    T.StructField("mcap_usd", T.DecimalType(38, 18)),
    T.StructField("fdv_usd", T.DecimalType(38, 18)),
    T.StructField("liquidity_usd", T.DecimalType(38, 18)),
    T.StructField("process_time", T.TimestampType()),
    T.StructField("create_time", T.TimestampType()),
])


def parse_args() -> argparse.Namespace:
    # Airflow/脚本调度入口，为 Spark job 暴露主要参数
    parser = argparse.ArgumentParser(description="DEX batch ETL using Spark + Paimon + StarRocks")
    # mock 配置决定输出规模、倾斜程度、账户/Token 基础元数据
    parser.add_argument("--config", default="runtime/batch/spark/config/dex_batch_job_config.json", help="mock metadata config")
    # warehouse/s3 参数需要和 docker-compose 中的 MinIO 完全一致
    parser.add_argument("--warehouse", default="s3a://paimon-warehouse/wh", help="Paimon warehouse URI")
    parser.add_argument("--s3-endpoint", default="http://minio:9000", help="MinIO/S3 endpoint")
    parser.add_argument("--s3-access-key", default="admin")
    parser.add_argument("--s3-secret-key", default="password123")
    # StarRocks JDBC 写入仅用于 DWS 层，如果未启动 StarRocks 可忽略
    parser.add_argument("--starrocks-jdbc", default="jdbc:mysql://starrocks-allin1:9030/analytics", help="StarRocks MySQL endpoint")
    parser.add_argument("--starrocks-table", default="token_recent_metric_sr", help="StarRocks target table")
    parser.add_argument("--starrocks-user", default="root")
    parser.add_argument("--starrocks-password", default="")
    parser.add_argument("--write-starrocks", action="store_true", help="enable StarRocks sink")
    # overwrite-ods 允许按天重跑，避免旧分区干扰
    parser.add_argument("--overwrite-ods", action="store_true", help="truncate ODS partition for the run date before append")
    # dry-run 仅打印数据样例，方便核对字段
    parser.add_argument("--dry-run", action="store_true", help="skip all writes; print sample rows only")
    return parser.parse_args()


def load_config(path: str) -> Dict:
    # 读取配置并给地址统一小写、按流动性估算 pair 权重
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    # 地址全部 lower-case，防止 join 时因大小写导致维度缺失
    for account in cfg["accounts"]:
        account["address"] = account["address"].lower()
    for token in cfg["tokens"]:
        token["address"] = token["address"].lower()
    for pair in cfg["pairs"]:
        pair["pair_address"] = pair["pair_address"].lower()
    total_liquidity = sum(pair["base_liquidity_usd"] for pair in cfg["pairs"])
    # 使用 Token 权重 + Pair 流动性来推导“事件生成概率”，便于模拟热点交易对
    token_weight_map = {token["token_id"]: token.get("weight", 0.0) for token in cfg["tokens"]}
    for pair in cfg["pairs"]:
        liq_weight = pair["base_liquidity_usd"] / total_liquidity if total_liquidity else 0.0
        bias = 0.5 * (token_weight_map.get(pair["token0_id"], 0.0) + token_weight_map.get(pair["token1_id"], 0.0))
        pair["event_weight"] = round(0.5 * liq_weight + 0.5 * bias, 6)
    return cfg


def init_spark(args: argparse.Namespace) -> SparkSession:
    # Spark Session 初始化：设置 Paimon Catalog、S3 凭证和常用序列化参数
    spark = (
        SparkSession.builder.appName("dex-batch-job")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon.warehouse", args.warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_paimon_tables(spark: SparkSession):
    # 在 Paimon Catalog 中按 ODS/DWD 分层创建表结构
    # lake_bronze: 主要用于承接 Kafka -> ODS 的全量明细
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.lake_bronze")
    # lake_silver: DWD + 维度表，承接批处理后的整洁数据
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.lake_silver")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS paimon.lake_bronze.tx_transaction (
          chain_id STRING,
          block_number BIGINT,
          block_timestamp TIMESTAMP,
          transaction_hash STRING,
          transaction_index INT,
          from_address STRING,
          to_address STRING,
          nonce BIGINT,
          gas_used BIGINT,
          gas_price_wei DECIMAL(38, 0),
          transaction_value_wei DECIMAL(38, 0),
          tx_status STRING,
          input_data STRING,
          source STRING,
          ingest_time TIMESTAMP,
          pt STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
          'write-mode' = 'append-only',
          'file.format' = 'parquet'
        )
        """
    )

    # tx_events 承载更细粒度字段，允许二次解析 decoded_args
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS paimon.lake_bronze.tx_events (
          chain_id STRING,
          block_number BIGINT,
          block_timestamp TIMESTAMP,
          transaction_hash STRING,
          log_index INT,
          event_name STRING,
          contract_address STRING,
          maker_address STRING,
          taker_address STRING,
          token0_address STRING,
          token1_address STRING,
          token_in_address STRING,
          token_out_address STRING,
          token_in_symbol STRING,
          token_out_symbol STRING,
          amount0 DECIMAL(38, 18),
          amount1 DECIMAL(38, 18),
          amount_usd DECIMAL(38, 18),
          direction STRING,
          pair_id BIGINT,
          pair_address STRING,
          topics_json STRING,
          decoded_args_json STRING,
          source STRING,
          ingest_time TIMESTAMP,
          pt STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
          'write-mode' = 'append-only',
          'file.format' = 'parquet'
        )
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS paimon.lake_silver.dim_account (
          account_id BIGINT,
          address STRING,
          account_type STRING,
          entity_category STRING,
          region STRING,
          risk_tier STRING,
          kyc_level STRING,
          vip_level INT,
          status STRING,
          labels ARRAY<STRING>,
          primary_segment STRING,
          first_seen TIMESTAMP,
          last_updated TIMESTAMP,
          pt STRING
        ) USING paimon
        TBLPROPERTIES (
          'primary-key' = 'account_id',
          'bucket' = '1'
        )
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS paimon.lake_silver.dim_token (
          token_id BIGINT,
          symbol STRING,
          token_address STRING,
          decimals INT,
          base_price_usd DECIMAL(38, 8),
          volatility_bps INT,
          liquidity_rank INT,
          circulating_supply BIGINT,
          weight DOUBLE,
          status STRING,
          last_updated TIMESTAMP,
          pt STRING
        ) USING paimon
        TBLPROPERTIES (
          'primary-key' = 'token_id',
          'bucket' = '1'
        )
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS paimon.lake_silver.dim_pair (
          pair_id BIGINT,
          pair_address STRING,
          token0_id BIGINT,
          token1_id BIGINT,
          fee_tier_bps INT,
          base_liquidity_usd DECIMAL(38, 8),
          event_weight DOUBLE,
          status STRING,
          last_updated TIMESTAMP,
          pt STRING
        ) USING paimon
        TBLPROPERTIES (
          'primary-key' = 'pair_id',
          'bucket' = '1'
        )
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS paimon.lake_silver.dwd_dex_trade_di (
          biz_date STRING,
          biz_hour STRING,
          chain_id STRING,
          block_number BIGINT,
          block_timestamp TIMESTAMP,
          transaction_hash STRING,
          log_index INT,
          pair_id BIGINT,
          pair_address STRING,
          token_in_id BIGINT,
          token_out_id BIGINT,
          token_in_symbol STRING,
          token_out_symbol STRING,
          side STRING,
          account_id BIGINT,
          account_address STRING,
          account_segment STRING,
          risk_tier STRING,
          qty_token_in DECIMAL(38, 18),
          qty_token_out DECIMAL(38, 18),
          price_usd DECIMAL(38, 18),
          value_usd DECIMAL(38, 18),
          liquidity_usd DECIMAL(38, 18),
          fee_bps INT,
          ingest_batch STRING,
          ingest_time TIMESTAMP
        ) USING paimon
        PARTITIONED BY (biz_date)
        TBLPROPERTIES (
          'primary-key' = 'biz_date,transaction_hash,log_index,account_id',
          'bucket' = '3'
        )
        """
    )


def create_dim_dfs(spark: SparkSession, cfg: Dict, run_dt: str) -> Tuple[DataFrame, DataFrame, DataFrame]:
    # 依据 Binance 内部口径生成账户/Token/交易对维表
    """创建维度表 DataFrame，使用明确的 schema 避免类型推断问题"""
    now_ts = datetime.now(timezone.utc)
    first_seen_ts = datetime.fromisoformat(cfg["mock"]["start_time_utc"].replace("Z", "+00:00"))
    
    # 定义 account schema
    account_schema = T.StructType([
        T.StructField("account_id", T.LongType()),
        T.StructField("address", T.StringType()),
        T.StructField("account_type", T.StringType()),
        T.StructField("entity_category", T.StringType()),
        T.StructField("region", T.StringType()),
        T.StructField("risk_tier", T.StringType()),
        T.StructField("kyc_level", T.StringType()),
        T.StructField("vip_level", T.IntegerType()),
        T.StructField("status", T.StringType()),
        T.StructField("labels", T.ArrayType(T.StringType())),
        T.StructField("primary_segment", T.StringType()),
        T.StructField("first_seen", T.TimestampType()),
        T.StructField("last_updated", T.TimestampType()),
        T.StructField("pt", T.StringType()),
    ])
    
    # 直接构造 tuple，避免 toDF 时推断成 string
    account_rows = [
        (
            acc["account_id"],
            acc["address"],
            acc["account_type"],
            acc["entity_category"],
            acc["region"],
            acc["risk_tier"],
            acc["kyc_level"],
            acc["vip_level"],
            acc["status"],
            acc["labels"],
            segment_from_category(acc["entity_category"]),
            first_seen_ts,
            now_ts,
            run_dt,
        )
        for acc in cfg["accounts"]
    ]
    account_df = spark.createDataFrame(account_rows, schema=account_schema)

    # 定义 token schema
    token_schema = T.StructType([
        T.StructField("token_id", T.LongType()),
        T.StructField("symbol", T.StringType()),
        T.StructField("token_address", T.StringType()),
        T.StructField("decimals", T.IntegerType()),
        T.StructField("base_price_usd", T.DecimalType(38, 8)),
        T.StructField("volatility_bps", T.IntegerType()),
        T.StructField("liquidity_rank", T.IntegerType()),
        T.StructField("circulating_supply", T.LongType()),
        T.StructField("weight", T.DoubleType()),
        T.StructField("status", T.StringType()),
        T.StructField("last_updated", T.TimestampType()),
        T.StructField("pt", T.StringType()),
    ])
    
    # Token 行同样走 tuple 以防止 decimal 精度被截断
    token_rows = [
        (
            token["token_id"],
            token["symbol"],
            token["address"],
            token["decimals"],
            Decimal(str(token["base_price_usd"])),
            token["volatility_bps"],
            token["liquidity_rank"],
            token["circulating_supply"],
            float(token.get("weight", 0.0)),
            "ACTIVE",
            now_ts,
            run_dt,
        )
        for token in cfg["tokens"]
    ]
    token_df = spark.createDataFrame(token_rows, schema=token_schema)

    # 定义 pair schema
    pair_schema = T.StructType([
        T.StructField("pair_id", T.LongType()),
        T.StructField("pair_address", T.StringType()),
        T.StructField("token0_id", T.LongType()),
        T.StructField("token1_id", T.LongType()),
        T.StructField("fee_tier_bps", T.IntegerType()),
        T.StructField("base_liquidity_usd", T.DecimalType(38, 8)),
        T.StructField("event_weight", T.DoubleType()),
        T.StructField("status", T.StringType()),
        T.StructField("last_updated", T.TimestampType()),
        T.StructField("pt", T.StringType()),
    ])
    
    # Pair schema 关键字段：fee tier、base_liquidity 与 event_weight
    pair_rows = [
        (
            pair["pair_id"],
            pair["pair_address"],
            pair["token0_id"],
            pair["token1_id"],
            pair["fee_tier_bps"],
            Decimal(str(pair["base_liquidity_usd"])),
            float(pair.get("event_weight", 0.0)),
            "ACTIVE",
            now_ts,
            run_dt,
        )
        for pair in cfg["pairs"]
    ]
    pair_df = spark.createDataFrame(pair_rows, schema=pair_schema)
    return account_df, token_df, pair_df


def segment_from_category(category: str) -> str:
    mapping = {
        "CEX_HOT": "cex",
        "SMART_MONEY": "smart",
        "WHALE": "whale",
        "FRESH_WALLET": "fresh",
        "PUBLIC_FIGURE": "public",
    }
    return mapping.get(category, "other")


def build_mock_swaps(spark: SparkSession, cfg: Dict) -> Tuple[DataFrame, DataFrame, str]:
    # 依据小时、交易对权重生成可控规模的 Swap 数据
    mock_cfg = cfg["mock"]
    start_ts = datetime.fromisoformat(mock_cfg["start_time_utc"].replace("Z", "+00:00"))
    start_ms = int(start_ts.timestamp() * 1000)
    hours = int(mock_cfg["hours"])
    base_events = int(mock_cfg["base_events_per_hour"])
    block_interval = int(mock_cfg["block_interval"])
    qty_min, qty_max = mock_cfg["qty_range"]
    price_noise_bps = mock_cfg["price_noise_bps"] / 10000.0
    seed = int(mock_cfg.get("seed", 2024))
    ingest_batch = f"mock_{start_ts.strftime('%Y%m%d%H')}_{uuid.uuid4().hex[:8]}"

    # 将账户信息放入 array literal，后面用 rand 实现可调倾斜的 taker 采样
    accounts_literal = F.array(
        *[
            F.struct(
                F.lit(acc["account_id"]).alias("account_id"),
                F.lit(acc["address"]).alias("address"),
                F.lit(segment_from_category(acc["entity_category"])).alias("segment"),
                F.lit(acc["risk_tier"]).alias("risk_tier"),
            )
            for acc in cfg["accounts"]
        ]
    )
    account_size = len(cfg["accounts"])

    token_df = spark.createDataFrame(cfg["tokens"])
    pair_df = spark.createDataFrame(cfg["pairs"])
    pair_enriched = (
        pair_df.alias("p")
        .join(token_df.alias("t0"), F.col("p.token0_id") == F.col("t0.token_id"))
        .join(token_df.alias("t1"), F.col("p.token1_id") == F.col("t1.token_id"))
        .select(
            F.col("p.pair_id"),
            F.col("p.pair_address"),
            F.col("p.fee_tier_bps"),
            F.col("p.base_liquidity_usd"),
            F.col("p.event_weight"),
            F.col("t0.token_id").alias("token0_id"),
            F.col("t0.symbol").alias("token0_symbol"),
            F.col("t0.address").alias("token0_address"),
            F.col("t0.base_price_usd").alias("token0_price"),
            F.col("t0.volatility_bps").alias("token0_vol"),
            F.col("t1.token_id").alias("token1_id"),
            F.col("t1.symbol").alias("token1_symbol"),
            F.col("t1.address").alias("token1_address"),
            F.col("t1.base_price_usd").alias("token1_price"),
            F.col("t1.volatility_bps").alias("token1_vol"),
        )
    )

    # 以“小时 x 交易对”构造网格，再展开为事件粒度
    hours_df = spark.range(0, hours).withColumn("hour_index", F.col("id")).drop("id")
    hours_df = hours_df.withColumn("hour_start_ms", F.lit(start_ms) + F.col("hour_index") * 3600 * 1000)

    # 对每个“小时 x 交易对”计算事件量后再展开成单条交易
    grid = hours_df.crossJoin(pair_enriched)
    grid = grid.withColumn(
        "events_per_hour",
        F.greatest(
            F.lit(0),
            (
                F.col("event_weight") * base_events * (F.lit(0.7) + F.rand(seed) * 0.6)
            ).cast("int"),
        ),
    )

    exploded = grid.selectExpr(
        "*",
        "posexplode_outer(case when events_per_hour>0 then sequence(0, events_per_hour-1) end) as (event_pos, seq_id)"
    ).where(F.col("seq_id").isNotNull())

    expanded = (
        exploded
        .withColumn(
            "block_timestamp_ms",
            F.col("hour_start_ms") + F.col("seq_id") * block_interval * 1000
        )
        .withColumn(
            "direction_flag",
            F.when(F.rand(seed + 1) > 0.5, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "taker",
            F.element_at(
                accounts_literal,
                (F.floor(F.rand(seed + 2) * account_size).cast("int") + 1)
            )
        )
    )

    qty_range = qty_max - qty_min
    # amount_in 结合 pair 权重随机生成 — 可通过权重模拟倾斜
    expanded = expanded.withColumn(
        "qty_token_in",
        F.round(
            F.lit(qty_min)
            + (F.rand(seed + 3) * qty_range * (F.lit(0.5) + F.col("event_weight"))),
            8,
        )
    )

    def price_with_noise(base_col: F.Column, vol_col: F.Column, rnd_seed: int) -> F.Column:
        noise = (F.rand(rnd_seed) - 0.5) * (vol_col / F.lit(10000.0) + price_noise_bps)
        return base_col * (F.lit(1) + noise)

    # price_with_noise 模拟波动率对 mid price 的影响
    expanded = expanded.withColumn("token0_price_live", price_with_noise(F.col("token0_price"), F.col("token0_vol"), seed + 4))
    expanded = expanded.withColumn("token1_price_live", price_with_noise(F.col("token1_price"), F.col("token1_vol"), seed + 5))

    expanded = expanded.withColumn(
        "token_in_id",
        F.when(F.col("direction_flag") == 1, F.col("token0_id")).otherwise(F.col("token1_id"))
    ).withColumn(
        "token_out_id",
        F.when(F.col("direction_flag") == 1, F.col("token1_id")).otherwise(F.col("token0_id"))
    ).withColumn(
        "token_in_symbol",
        F.when(F.col("direction_flag") == 1, F.col("token0_symbol")).otherwise(F.col("token1_symbol"))
    ).withColumn(
        "token_out_symbol",
        F.when(F.col("direction_flag") == 1, F.col("token1_symbol")).otherwise(F.col("token0_symbol"))
    ).withColumn(
        "token_in_address",
        F.when(F.col("direction_flag") == 1, F.col("token0_address")).otherwise(F.col("token1_address"))
    ).withColumn(
        "token_out_address",
        F.when(F.col("direction_flag") == 1, F.col("token1_address")).otherwise(F.col("token0_address"))
    ).withColumn(
        "token_in_price",
        F.when(F.col("direction_flag") == 1, F.col("token0_price_live")).otherwise(F.col("token1_price_live"))
    ).withColumn(
        "token_out_price",
        F.when(F.col("direction_flag") == 1, F.col("token1_price_live")).otherwise(F.col("token0_price_live"))
    )

    expanded = expanded.withColumn(
        "amount_out",
        F.round(
            F.col("qty_token_in") * F.col("token_in_price") / F.col("token_out_price") * (1 - F.col("fee_tier_bps") / 10000.0),
            8,
        )
    )
    expanded = expanded.withColumn("value_usd", F.round(F.col("qty_token_in") * F.col("token_in_price"), 8))

    expanded = expanded.withColumn(
        "transaction_hash",
        F.sha2(
            F.concat_ws(
                "",
                F.lit("0xmock"),
                F.col("pair_address"),
                F.col("block_timestamp_ms").cast("string"),
                F.col("event_pos").cast("string")
            ),
            256,
        )
    ).withColumn(
        "log_index",
        F.row_number().over(Window.orderBy("block_timestamp_ms", "pair_id")) - 1
    ).withColumn(
        "block_number",
        F.lit(18000000) + F.row_number().over(Window.orderBy("block_timestamp_ms"))
    ).withColumn(
        "block_timestamp",
        (F.col("block_timestamp_ms") / 1000).cast("timestamp")
    )

    expanded = expanded.withColumn("direction", F.when(F.col("direction_flag") == 1, F.lit("sell_token0")).otherwise(F.lit("sell_token1")))

    # 构建 transactions_df，确保所有字段类型与 Paimon 表定义匹配
    transactions_df = expanded.select(
        F.lit(cfg["mock"]["chain_id"]).cast("string").alias("chain_id"),
        F.col("block_number").cast("bigint").alias("block_number"),
        F.col("block_timestamp").cast("timestamp").alias("block_timestamp"),
        F.col("transaction_hash").cast("string").alias("transaction_hash"),
        F.col("event_pos").cast("int").alias("transaction_index"),
        F.col("taker.address").cast("string").alias("from_address"),
        F.col("pair_address").cast("string").alias("to_address"),
        F.row_number().over(Window.orderBy("transaction_hash")).cast("bigint").alias("nonce"),
        F.lit(120000).cast("bigint").alias("gas_used"),
        F.lit(30_000_000_000).cast("decimal(38,0)").alias("gas_price_wei"),
        F.round(F.col("value_usd") * F.lit(10 ** 18)).cast("decimal(38,0)").alias("transaction_value_wei"),
        F.lit("SUCCESS").cast("string").alias("tx_status"),
        F.lit("0x").cast("string").alias("input_data"),
        F.lit("spark-mock").cast("string").alias("source"),
        F.current_timestamp().cast("timestamp").alias("ingest_time"),
        F.date_format("block_timestamp", "yyyy-MM-dd").cast("string").alias("pt")
    )

    decoded_args = F.to_json(F.struct(
        F.col("qty_token_in").alias("amountIn"),
        F.col("amount_out").alias("amountOut"),
        F.col("token_in_symbol").alias("tokenIn"),
        F.col("token_out_symbol").alias("tokenOut"),
        F.col("value_usd").alias("valueUsd")
    ))

    # 构建 events_df，确保所有字段类型与 Paimon 表定义匹配
    events_df = expanded.select(
        F.lit(cfg["mock"]["chain_id"]).cast("string").alias("chain_id"),
        F.col("block_number").cast("bigint").alias("block_number"),
        F.col("block_timestamp").cast("timestamp").alias("block_timestamp"),
        F.col("transaction_hash").cast("string").alias("transaction_hash"),
        F.col("log_index").cast("int").alias("log_index"),
        F.lit("Swap").cast("string").alias("event_name"),
        F.col("pair_address").cast("string").alias("contract_address"),
        F.col("pair_address").cast("string").alias("maker_address"),
        F.col("taker.address").cast("string").alias("taker_address"),
        F.col("token0_address").cast("string").alias("token0_address"),
        F.col("token1_address").cast("string").alias("token1_address"),
        F.col("token_in_address").cast("string").alias("token_in_address"),
        F.col("token_out_address").cast("string").alias("token_out_address"),
        F.col("token_in_symbol").cast("string").alias("token_in_symbol"),
        F.col("token_out_symbol").cast("string").alias("token_out_symbol"),
        F.col("qty_token_in").cast("decimal(38,18)").alias("amount0"),
        F.col("amount_out").cast("decimal(38,18)").alias("amount1"),
        F.col("value_usd").cast("decimal(38,18)").alias("amount_usd"),
        F.col("direction").cast("string").alias("direction"),
        F.col("pair_id").cast("bigint").alias("pair_id"),
        F.col("pair_address").cast("string").alias("pair_address"),
        F.to_json(F.array(F.lit("topic0"), F.lit("topic1"))).cast("string").alias("topics_json"),
        decoded_args.cast("string").alias("decoded_args_json"),
        F.lit("spark-mock").cast("string").alias("source"),
        F.current_timestamp().cast("timestamp").alias("ingest_time"),
        F.date_format("block_timestamp", "yyyy-MM-dd").cast("string").alias("pt")
    )

    return transactions_df, events_df, ingest_batch


def build_dwd(spark: SparkSession, run_dt: str, ingest_batch: str) -> DataFrame:
    # 通过维度表补充事件元数据，写入三层建模中的 DWD 事实表
    events = spark.table("paimon.lake_bronze.tx_events").where(F.col("pt") == run_dt)
    dim_account = spark.table("paimon.lake_silver.dim_account").where(F.col("pt") == run_dt)
    dim_pair = spark.table("paimon.lake_silver.dim_pair").where(F.col("pt") == run_dt)
    dim_token = spark.table("paimon.lake_silver.dim_token").where(F.col("pt") == run_dt)

    events = events.filter(F.col("event_name") == "Swap")
    joined = (
        events.alias("e")
        .join(dim_pair.alias("p"), F.col("e.pair_id") == F.col("p.pair_id"), "left")
        .join(dim_account.alias("a"), F.col("e.taker_address") == F.col("a.address"), "left")
        .join(dim_token.alias("tin"), F.col("e.token_in_address") == F.col("tin.token_address"), "left")
        .join(dim_token.alias("tout"), F.col("e.token_out_address") == F.col("tout.token_address"), "left")
    )

    segment_expr = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in {
        "CEX_HOT": "cex",
        "SMART_MONEY": "smart",
        "WHALE": "whale",
        "FRESH_WALLET": "fresh",
        "PUBLIC_FIGURE": "public",
    }.items()], []))

    # 构建 DWD，确保所有字段类型与 Paimon 表定义匹配
    dwd = (
        joined
        .withColumn("account_segment", F.coalesce(segment_expr[F.col("a.entity_category")], F.lit("other")))
        .withColumn("risk_tier", F.coalesce(F.col("a.risk_tier"), F.lit("UNKNOWN")))
        .withColumn("side", F.when(F.col("direction") == "sell_token0", F.lit("sell")).otherwise(F.lit("buy")))
        .withColumn("biz_date", F.date_format("e.block_timestamp", "yyyy-MM-dd"))
        .withColumn("biz_hour", F.date_format("e.block_timestamp", "HH"))
        # 以 amount0 推导成交均价，缺失时填 1 避免除零
        .withColumn("price_usd", F.col("amount_usd") / F.when(F.col("amount0") > 0, F.col("amount0")).otherwise(F.lit(1)))
        .withColumn("liquidity_usd", F.col("p.base_liquidity_usd"))
        .withColumn("ingest_batch", F.lit(ingest_batch))
        .withColumn("ingest_time", F.current_timestamp())
        .select(
            F.col("biz_date").cast("string").alias("biz_date"),
            F.col("biz_hour").cast("string").alias("biz_hour"),
            F.col("e.chain_id").cast("string").alias("chain_id"),
            F.col("e.block_number").cast("bigint").alias("block_number"),
            F.col("e.block_timestamp").cast("timestamp").alias("block_timestamp"),
            F.col("e.transaction_hash").cast("string").alias("transaction_hash"),
            F.col("e.log_index").cast("int").alias("log_index"),
            F.col("e.pair_id").cast("bigint").alias("pair_id"),
            F.col("e.pair_address").cast("string").alias("pair_address"),
            F.col("tin.token_id").cast("bigint").alias("token_in_id"),
            F.col("tout.token_id").cast("bigint").alias("token_out_id"),
            F.col("tin.symbol").cast("string").alias("token_in_symbol"),
            F.col("tout.symbol").cast("string").alias("token_out_symbol"),
            F.col("side").cast("string").alias("side"),
            F.col("a.account_id").cast("bigint").alias("account_id"),
            F.col("e.taker_address").cast("string").alias("account_address"),
            F.col("account_segment").cast("string").alias("account_segment"),
            F.col("risk_tier").cast("string").alias("risk_tier"),
            F.col("e.amount0").cast("decimal(38,18)").alias("qty_token_in"),
            F.col("e.amount1").cast("decimal(38,18)").alias("qty_token_out"),
            F.round(F.col("price_usd"), 8).cast("decimal(38,18)").alias("price_usd"),
            F.col("e.amount_usd").cast("decimal(38,18)").alias("value_usd"),
            F.col("liquidity_usd").cast("decimal(38,18)").alias("liquidity_usd"),
            F.col("p.fee_tier_bps").cast("int").alias("fee_bps"),
            F.col("ingest_batch").cast("string").alias("ingest_batch"),
            F.col("ingest_time").cast("timestamp").alias("ingest_time"),
        )
    )
    return dwd


def aggregate_token_metrics(spark: SparkSession, dwd_df: DataFrame, run_dt: str) -> DataFrame:
    # 从 DWD 事实表做多窗口聚合，并映射到 StarRocks 的指标表结构
    if dwd_df.rdd.isEmpty():
        empty_rdd = spark.sparkContext.emptyRDD()
        return spark.createDataFrame(empty_rdd, schema=TOKEN_METRIC_SCHEMA)

    dim_token = spark.table("paimon.lake_silver.dim_token").where(F.col("pt") == run_dt)
    metric_base = dwd_df.select(
        "block_timestamp",
        F.col("token_in_id").alias("token_id"),
        "side",
        "value_usd",
        "price_usd",
        F.col("account_segment").alias("segment"),
    )
    metric_base = metric_base.withColumn("tag", F.explode(F.array(F.col("segment"), F.lit("all"))))

    # 多粒度窗口配置，可按需扩展 time_window
    windows = [
        ("20s", "20 seconds"),
        ("1min", "1 minute"),
        ("5min", "5 minutes"),
        ("1h", "1 hour"),
    ]
    agg_frames: List[DataFrame] = []

    for label, interval in windows:
        w_df = (
            metric_base
            .groupBy(F.window("block_timestamp", interval).alias("w"), "token_id", "tag")
            .agg(
                F.count("*").alias("txcnt"),
                F.sum(F.when(F.col("side") == "buy", 1).otherwise(0)).alias("buy_count"),
                F.sum(F.when(F.col("side") == "sell", 1).otherwise(0)).alias("sell_count"),
                F.sum(F.when(F.col("side") == "buy", F.col("value_usd")).otherwise(F.lit(0))).alias("buy_volume_usd"),
                F.sum(F.when(F.col("side") == "sell", F.col("value_usd")).otherwise(F.lit(0))).alias("sell_volume_usd"),
                F.sum("value_usd").alias("volume_usd"),
                F.avg("price_usd").alias("avg_price"),
            )
            .withColumn("time_window", F.lit(label))
            .withColumn("end_time", F.col("w").end.cast("timestamp"))
            .drop("w")
        )
        agg_frames.append(w_df)

    metrics = agg_frames[0]
    for frame in agg_frames[1:]:
        metrics = metrics.unionByName(frame, allowMissingColumns=True)

    # buy_pressure 便于后续在 StarRocks 上直接计算买卖压力
    metrics = metrics.withColumn("buy_pressure_usd", F.col("buy_volume_usd") - F.col("sell_volume_usd"))

    # 使用别名避免 token_id 列名歧义
    metrics_alias = metrics.alias("m")
    dim_token_alias = dim_token.alias("d")
    metrics = metrics_alias.join(dim_token_alias, F.col("m.token_id") == F.col("d.token_id"), "left")
    metrics = metrics.withColumn("token_price_usd", F.coalesce(F.col("avg_price"), F.col("d.base_price_usd")))
    metrics = metrics.withColumn("mcap_usd", F.col("token_price_usd") * F.col("d.circulating_supply"))
    metrics = metrics.withColumn("fdv_usd", F.col("mcap_usd"))
    metrics = metrics.withColumn("liquidity_usd", F.col("m.volume_usd") * F.lit(0.1))
    metrics = metrics.select(
        F.col("m.token_id").cast("bigint").alias("token_id"),
        F.col("m.time_window").cast("string").alias("time_window"),
        F.col("m.end_time").cast("timestamp").alias("end_time"),
        F.col("m.tag").cast("string").alias("tag"),
        F.col("m.txcnt").cast("bigint").alias("txcnt"),
        F.col("m.buy_count").cast("bigint").alias("buy_count"),
        F.col("m.sell_count").cast("bigint").alias("sell_count"),
        F.col("m.volume_usd").cast("decimal(38,18)").alias("volume_usd"),
        F.col("m.buy_volume_usd").cast("decimal(38,18)").alias("buy_volume_usd"),
        F.col("m.sell_volume_usd").cast("decimal(38,18)").alias("sell_volume_usd"),
        F.col("buy_pressure_usd").cast("decimal(38,18)").alias("buy_pressure_usd"),
        F.col("token_price_usd").cast("decimal(38,18)").alias("token_price_usd"),
        F.col("mcap_usd").cast("decimal(38,18)").alias("mcap_usd"),
        F.col("fdv_usd").cast("decimal(38,18)").alias("fdv_usd"),
        F.col("liquidity_usd").cast("decimal(38,18)").alias("liquidity_usd"),
        F.current_timestamp().cast("timestamp").alias("process_time"),
        F.current_timestamp().cast("timestamp").alias("create_time"),
    )
    return metrics


def write_starrocks(df: DataFrame, args: argparse.Namespace):
    # 通过 StarRocks MySQL 接口批量写入 DWS 结果
    if df.rdd.isEmpty():
        print("[WARN] token metric dataframe empty; skip StarRocks write")
        return
    (df.write.format("jdbc")
     .option("url", args.starrocks_jdbc)
     .option("dbtable", args.starrocks_table)
     .option("user", args.starrocks_user)
     .option("password", args.starrocks_password)
     .option("driver", "com.mysql.cj.jdbc.Driver")
     .mode("append")
     .save())


def main():
    args = parse_args()
    cfg = load_config(args.config)
    spark = init_spark(args)
    ensure_paimon_tables(spark)

    run_dt = cfg["mock"]["start_time_utc"][:10]
    account_df, token_df, pair_df = create_dim_dfs(spark, cfg, run_dt)

    if not args.dry_run:
        # 维度表使用主键去重，直接追加即可（Paimon会自动合并相同主键的记录）
        account_df.writeTo("paimon.lake_silver.dim_account").append()
        token_df.writeTo("paimon.lake_silver.dim_token").append()
        pair_df.writeTo("paimon.lake_silver.dim_pair").append()

    # 生成 mock 交易数据（控制写入量/倾斜）并分流成交易、事件两套 ODS
    tx_df, ev_df, ingest_batch = build_mock_swaps(spark, cfg)

    if args.dry_run:
        tx_df.show(5, truncate=False)
        ev_df.show(5, truncate=False)
    else:
        if args.overwrite_ods:
            # 为了方便重放，允许通过参数清空当日 ODS 分区
            spark.sql(f"ALTER TABLE paimon.lake_bronze.tx_transaction DROP IF EXISTS PARTITION (pt='{run_dt}')")
            spark.sql(f"ALTER TABLE paimon.lake_bronze.tx_events DROP IF EXISTS PARTITION (pt='{run_dt}')")
        tx_df.writeTo("paimon.lake_bronze.tx_transaction").append()
        ev_df.writeTo("paimon.lake_bronze.tx_events").append()

    # 进入 DWD 层前先补齐账号标签、token 元信息
    dwd_df = build_dwd(spark, run_dt, ingest_batch)
    if args.dry_run:
        dwd_df.show(5, truncate=False)
    else:
        dwd_df.writeTo("paimon.lake_silver.dwd_dex_trade_di").append()

    # DWS 层：聚合后的 Token 指标既可用于 StarRocks，也能回写湖仓
    metrics_df = aggregate_token_metrics(spark, dwd_df, run_dt)
    if args.dry_run:
        metrics_df.show(5, truncate=False)
    elif args.write_starrocks:
        write_starrocks(metrics_df, args)

    spark.stop()


if __name__ == "__main__":
    main()

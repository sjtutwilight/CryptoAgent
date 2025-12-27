#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Probe - Spark 作业与数据湖探针

职责：
- 验证 Paimon 表数据
- 检查 Spark 集群状态
"""

import subprocess
import sys
from pathlib import Path
from typing import Optional

# 添加项目根目录到 Python 路径
ROOT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR / "automation" / "test"))

from ..shared.core.context import RunContext
from ..shared.core.result import ProbeResult, ProbeStatus


def verify_paimon_table(
    ctx: RunContext,
    spark_container: str = "spark-lab-client",
    database: str = "crypto_analytics",
    table: str = "token_holders_snapshot",
    chain_id: Optional[int] = None,
    token_address: Optional[str] = None
) -> ProbeResult:
    """
    验证 Paimon 表数据
    
    Args:
        ctx: 运行上下文
        spark_container: Spark 客户端容器名称
        database: 数据库名称
        table: 表名称
        chain_id: 可选的链 ID 过滤
        token_address: 可选的 token 地址过滤
    """
    try:
        # 检查容器是否运行
        check_cmd = f"docker ps --format '{{{{.Names}}}}' | grep -q '^{spark_container}$'"
        result = subprocess.run(check_cmd, shell=True, capture_output=True)
        if result.returncode != 0:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail=f"Spark 客户端容器未运行: {spark_container}"
            )
        
        # 构建 WHERE 子句
        where_parts = []
        if chain_id:
            where_parts.append(f"chain_id = {chain_id}")
        if token_address:
            where_parts.append(f"token_address = '{token_address}'")
        where_clause = " AND ".join(where_parts)
        
        # 创建验证脚本
        verify_script = f"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("VerifyPaimon") \\
    .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog") \\
    .config("spark.sql.catalog.paimon.warehouse", "s3a://paimon-warehouse/wh") \\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \\
    .config("spark.hadoop.fs.s3a.access.key", "admin") \\
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \\
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

sql = "SELECT COUNT(*) as cnt FROM paimon.{database}.{table}"
{"if where_clause:" if where_clause else ""}
sql += " WHERE {where_clause}"

result = spark.sql(sql).collect()
count = result[0]['cnt']

print(f"PROBE_RESULT:{{count}}")

spark.stop()
"""
        
        # 写入临时文件
        script_path = "/tmp/verify_paimon_probe.py"
        with open(script_path, "w") as f:
            f.write(verify_script)
        
        # 复制脚本到容器
        subprocess.run(
            f"docker cp {script_path} {spark_container}:/tmp/verify_paimon_probe.py",
            shell=True,
            check=True,
            capture_output=True
        )
        
        # 运行验证
        result = subprocess.run(
            f"docker exec {spark_container} /opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 "
            f"--packages org.apache.paimon:paimon-spark-3.5:1.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            f"/tmp/verify_paimon_probe.py",
            shell=True,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        # 解析结果
        for line in result.stdout.split('\n'):
            if line.startswith("PROBE_RESULT:"):
                count = int(line.split(":")[1])
                
                if count > 0:
                    return ProbeResult(
                        status=ProbeStatus.SUCCESS,
                        detail=f"表 paimon.{database}.{table} 包含 {count} 条记录",
                        metrics={"record_count": count}
                    )
                else:
                    return ProbeResult(
                        status=ProbeStatus.FAIL,
                        detail=f"表 paimon.{database}.{table} 为空"
                    )
        
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail="无法解析验证结果"
        )
        
    except subprocess.TimeoutExpired:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail="验证超时（120秒）"
        )
    except subprocess.CalledProcessError as e:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail=f"验证失败: {e.stderr.decode() if e.stderr else str(e)}"
        )
    except Exception as e:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail=f"验证失败: {str(e)}"
        )
    finally:
        # 清理临时文件
        try:
            Path(script_path).unlink(missing_ok=True)
        except:
            pass


def check_spark_cluster(ctx: RunContext) -> ProbeResult:
    """检查 Spark 集群状态"""
    try:
        # 检查 master
        master_check = subprocess.run(
            "docker exec spark-lab-master curl -sf http://localhost:8080/json/ > /dev/null",
            shell=True,
            capture_output=True
        )
        
        if master_check.returncode != 0:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail="Spark Master 不可用"
            )
        
        # 检查 worker
        worker_check = subprocess.run(
            "docker ps --filter 'name=spark-lab-worker' --format '{{.Status}}' | grep -q 'Up'",
            shell=True,
            capture_output=True
        )
        
        if worker_check.returncode != 0:
            return ProbeResult(
                status=ProbeStatus.FAIL,
                detail="Spark Worker 不可用"
            )
        
        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail="Spark 集群运行正常"
        )
        
    except Exception as e:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail=f"检查失败: {str(e)}"
        )


if __name__ == "__main__":
    # 简单的 CLI 测试
    import sys
    
    ctx = RunContext(
        run_id="test-run",
        scenario="spark_test",
        env="local"
    )
    
    if len(sys.argv) > 1:
        action = sys.argv[1]
        
        if action == "check-cluster":
            result = check_spark_cluster(ctx)
            print(f"Status: {result.status.value}")
            print(f"Detail: {result.detail}")
            sys.exit(0 if result.status == ProbeStatus.SUCCESS else 1)
        
        elif action == "verify":
            result = verify_paimon_table(ctx)
            print(f"Status: {result.status.value}")
            print(f"Detail: {result.detail}")
            if result.metrics:
                print(f"Metrics: {result.metrics}")
            sys.exit(0 if result.status == ProbeStatus.SUCCESS else 1)
    
    print("Usage: spark_probe.py [check-cluster|verify]")
    sys.exit(1)

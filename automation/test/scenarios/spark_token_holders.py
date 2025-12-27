from __future__ import annotations

from automation.test.shared.core.scenario import Scenario
from automation.test.shared.process.spark import SparkProcessStage


def build_scenario() -> Scenario:
    """构建 Spark Token Holders 测试场景"""
    
    return Scenario(
        name="spark_token_holders",
        tags=["spark", "paimon"],
        stages=[
            SparkProcessStage(
                name="spark",
                spark_container="spark-lab-client",
                database="crypto_analytics",
                table="token_holders_snapshot",
            ).to_stage(),
        ],
    )

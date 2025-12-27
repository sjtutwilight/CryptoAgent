from __future__ import annotations

from automation.test.shared.core.scenario import Scenario
from automation.test.shared.core.stages import CleanupStage, InfraCheckStage
from automation.test.shared.ingress.datainjector import DataInjectorIngressStage
from automation.test.shared.process.flink import FlinkProcessStage

# 使用 config.yaml 中定义的 role_id
DEFAULT_ROLE_IDS = ["binance-kline"]
DEFAULT_ENTRY_CLASS = "com.twilight.aggregator.KlineSignalJob"


def build_scenario() -> Scenario:
    """构建 Binance Kline 端到端测试场景"""
    
    return Scenario(
        name="binance_kline",
        tags=["type:e2e", "module:pipeline"],
        stages=[
            InfraCheckStage(
                name="infra",
                checks=["clickhouse", "flink"],
            ).to_stage(),
            DataInjectorIngressStage(
                name="ingress",
                role_ids=DEFAULT_ROLE_IDS,
                kafka_topics=["binance.kline"],
                cleanup_tables=["kline_metrics", "kline_indicator_metrics"],
            ).to_stage(),
            FlinkProcessStage(
                name="process",
                entry_class=DEFAULT_ENTRY_CLASS,
                verify_tables=["kline_metrics", "kline_indicator_metrics"],
            ).to_stage(),
            CleanupStage(name="cleanup").to_stage(),
        ],
    )

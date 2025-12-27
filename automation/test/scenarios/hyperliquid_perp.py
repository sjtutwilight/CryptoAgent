from __future__ import annotations

from automation.test.shared.core.scenario import Scenario
from automation.test.shared.core.stages import CleanupStage, InfraCheckStage
from automation.test.shared.ingress.datainjector import DataInjectorIngressStage
from automation.test.shared.process.flink import FlinkProcessStage

DEFAULT_ENTRY_CLASSES = [
    "com.twilight.aggregator.PerpExecutionMetricsJob",
    "com.twilight.aggregator.PerpContextMetricsJob",
    "com.twilight.aggregator.PerpPanelAggregatorJob",
]

# 使用 config.yaml 中定义的 role_id
DEFAULT_ROLE_IDS = [
    "hyperliquid-perp-orderbook",
    "hyperliquid-perp-trades",
    "hyperliquid-perp-asset-ctx",
]


def build_scenario() -> Scenario:
    """构建 Hyperliquid Perp 端到端测试场景"""
    
    return Scenario(
        name="hyperliquid_perp",
        tags=["type:e2e", "module:perp", "exchange:hyperliquid"],
        stages=[
            InfraCheckStage(
                name="infra",
                checks=["clickhouse", "flink"],
            ).to_stage(),
            DataInjectorIngressStage(
                name="ingress",
                role_ids=DEFAULT_ROLE_IDS,
                kafka_topics=["perp.orderbook", "perp.asset_ctx"],
                cleanup_tables=["dws_exec_1s", "dws_perps_ctx_1m", "dws_perps_panel_1m"],
            ).to_stage(),
            FlinkProcessStage(
                name="process",
                entry_classes=DEFAULT_ENTRY_CLASSES,
                verify_tables=["dws_exec_1s", "dws_perps_ctx_1m", "dws_perps_panel_1m"],
            ).to_stage(),
            CleanupStage(name="cleanup").to_stage(),
        ],
    )

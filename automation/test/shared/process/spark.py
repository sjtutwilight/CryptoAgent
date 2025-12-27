"""Spark 数据处理相关操作"""
from __future__ import annotations

from typing import Dict, List, Optional

from automation.test.probes import spark_probe
from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult
from automation.test.shared.core.scenario import ProbeCall
from automation.test.shared.core.stages import BaseStage


class SparkProcessStage(BaseStage):
    """Spark 数据处理 Stage"""

    def __init__(
        self,
        name: str = "spark",
        spark_container: str = "spark-lab-client",
        database: Optional[str] = None,
        table: Optional[str] = None,
        verify_params: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
    ):
        super().__init__(name, tags or ["layer:process", "tech:spark"])
        self.spark_container = spark_container
        self.database = database
        self.table = table
        self.verify_params = verify_params or {}

    def build_probes(self) -> List[ProbeCall]:
        return [
            ProbeCall("spark.check_cluster", self._probe_check_cluster),
            ProbeCall("spark.verify_paimon", self._probe_verify_paimon),
        ]

    def _probe_check_cluster(self, ctx: RunContext) -> ProbeResult:
        """检查 Spark 集群状态"""
        return spark_probe.check_spark_cluster(ctx)

    def _probe_verify_paimon(self, ctx: RunContext) -> ProbeResult:
        """验证 Paimon 表数据"""
        cfg = self._get_config(ctx)
        return spark_probe.verify_paimon_table(
            ctx,
            spark_container=cfg.get("spark_container", self.spark_container),
            database=cfg.get("database", self.database),
            table=cfg.get("table", self.table),
            **self.verify_params,
        )

    def _get_config(self, ctx: RunContext) -> Dict:
        """获取配置"""
        cfg = {}
        cfg.update(ctx.metadata or {})
        return cfg


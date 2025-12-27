"""通用 Stage 基类和标准 Stage 实现"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from automation.test.probes import db_probe, infra_probe, kafka_probe
from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult, ProbeStatus
from automation.test.shared.core.scenario import ProbeCall, Stage


class BaseStage(ABC):
    """Stage 基类，定义 Stage 的基本接口"""

    def __init__(self, name: str, tags: Optional[List[str]] = None):
        self.name = name
        self.tags = tags or []

    @abstractmethod
    def build_probes(self) -> List[ProbeCall]:
        """构建该 Stage 的 Probe 列表（不依赖 context）"""
        pass

    def to_stage(self) -> Stage:
        """转换为 Stage 对象"""
        probes = self.build_probes()
        return Stage(name=self.name, probes=probes, tags=self.tags)


class InfraCheckStage(BaseStage):
    """基础设施检查 Stage"""

    def __init__(self, name: str = "infra", checks: Optional[List[str]] = None, tags: Optional[List[str]] = None):
        super().__init__(name, tags or ["layer:infra"])
        self.checks = checks or ["clickhouse", "flink"]

    def build_probes(self) -> List[ProbeCall]:
        probes = []
        if "clickhouse" in self.checks:
            probes.append(ProbeCall("infra.clickhouse_ready", self._probe_clickhouse_ready))
        if "flink" in self.checks:
            probes.append(ProbeCall("infra.flink_ready", self._probe_flink_ready))
        if "kafka" in self.checks:
            probes.append(ProbeCall("infra.kafka_ready", self._probe_kafka_ready))
        return probes

    def _probe_clickhouse_ready(self, ctx: RunContext) -> ProbeResult:
        payload = infra_probe.snapshot_clickhouse()
        ok = payload.get("clickhouse", {}).get("ok") is True
        status = ProbeStatus.SUCCESS if ok else ProbeStatus.FAIL
        detail = "clickhouse ok" if ok else "clickhouse not ready"
        return ProbeResult(status=status, detail=detail, payload=payload)

    def _probe_flink_ready(self, ctx: RunContext) -> ProbeResult:
        payload = infra_probe.snapshot_flink()
        ok = payload.get("rest", {}).get("rest_ok") is True
        status = ProbeStatus.SUCCESS if ok else ProbeStatus.FAIL
        detail = "flink rest ok" if ok else "flink rest not ready"
        return ProbeResult(status=status, detail=detail, payload=payload)

    def _probe_kafka_ready(self, ctx: RunContext) -> ProbeResult:
        payload = infra_probe.snapshot_kafka()
        ok = payload.get("kafka", {}).get("reachable") is True
        status = ProbeStatus.SUCCESS if ok else ProbeStatus.FAIL
        detail = "kafka ok" if ok else "kafka not ready"
        return ProbeResult(status=status, detail=detail, payload=payload)


class VerifyStage(BaseStage):
    """结果验证 Stage"""

    def __init__(
        self,
        name: str = "verify",
        kafka_topics: Optional[List[str]] = None,
        db_tables: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ):
        super().__init__(name, tags or ["layer:verify"])
        self.kafka_topics = kafka_topics or []
        self.db_tables = db_tables or []

    def build_probes(self) -> List[ProbeCall]:
        probes = []
        for topic in self.kafka_topics:
            probes.append(ProbeCall(f"verify.kafka.{topic}", lambda c, t=topic: self._probe_kafka_topic(c, t)))
        for table in self.db_tables:
            probes.append(ProbeCall(f"verify.db.{table}", lambda c, t=table: self._probe_db_table(c, t)))
        return probes

    def _probe_kafka_topic(self, ctx: RunContext, topic: str) -> ProbeResult:
        return kafka_probe.has_message_with_run_id(ctx, topic)

    def _probe_db_table(self, ctx: RunContext, table: str) -> ProbeResult:
        return db_probe.result_exists(ctx, table, min_rows=1)


class CleanupStage(BaseStage):
    """清理 Stage - 从 RunContext.state 读取需要清理的资源"""

    def __init__(self, name: str = "cleanup", tags: Optional[List[str]] = None):
        super().__init__(name, tags or ["layer:cleanup"])

    def build_probes(self) -> List[ProbeCall]:
        return [ProbeCall("cleanup.resources", self._probe_cleanup)]

    def _probe_cleanup(self, ctx: RunContext) -> ProbeResult:
        """清理资源，具体逻辑由各个 Stage 写入 state 中的清理函数执行"""
        errors = []
        cleanup_funcs = ctx.state.get("cleanup_funcs", [])
        
        for cleanup_func in cleanup_funcs:
            try:
                cleanup_func(ctx)
            except Exception as exc:
                errors.append(str(exc))
        
        if errors:
            return ProbeResult(status=ProbeStatus.FAIL, detail="; ".join(errors))
        return ProbeResult(status=ProbeStatus.SUCCESS, detail="cleanup ok")


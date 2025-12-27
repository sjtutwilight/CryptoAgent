from __future__ import annotations

from ..shared.core.context import RunContext
from ..shared.core.result import ProbeResult, ProbeStatus


def run_processed(_: RunContext) -> ProbeResult:
    # TODO: implement Flink processing probe (metrics or sink check).
    return ProbeResult(status=ProbeStatus.SKIP, detail="TODO: flink_probe.run_processed")

from __future__ import annotations

from ..shared.core.context import RunContext
from ..shared.core.result import ProbeResult, ProbeStatus


def run_dq_rules_for_run(_: RunContext) -> ProbeResult:
    # TODO: implement DQ rules execution for a single run.
    return ProbeResult(status=ProbeStatus.SKIP, detail="TODO: dq_probe.run_dq_rules_for_run")


def global_token_balance_consistency() -> ProbeResult:
    # TODO: implement global consistency checks.
    return ProbeResult(status=ProbeStatus.SKIP, detail="TODO: dq_probe.global_token_balance_consistency")

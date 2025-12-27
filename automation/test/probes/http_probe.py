from __future__ import annotations

from ..shared.core.context import RunContext
from ..shared.core.result import ProbeResult, ProbeStatus


def send_request(_: RunContext, __: str, ___: str = "GET") -> ProbeResult:
    # TODO: implement HTTP probe for flow validation.
    return ProbeResult(status=ProbeStatus.SKIP, detail="TODO: http_probe.send_request")

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from .core.context import RunContext
from .core.result import ProbeResult


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")


def log_probe_result(
    run_dir: Path,
    ctx: RunContext,
    probe_name: str,
    result: ProbeResult,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    payload = {
        "run_id": ctx.run_id,
        "scenario": ctx.scenario,
        "env": ctx.env,
        "stage": ctx.stage,
        "probe": probe_name,
        "result": result.to_dict(),
        "extra": extra or {},
    }
    append_jsonl(run_dir / "probe_events.jsonl", payload)

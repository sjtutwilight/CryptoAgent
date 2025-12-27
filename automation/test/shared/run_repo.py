from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class RunRecord:
    run_id: str
    scenario: str
    status: str
    meta: Optional[Dict[str, Any]] = None


class RunRepo:
    """Placeholder for future persistence (DB or file-backed)."""

    def create_run(self, _: RunRecord) -> None:
        # TODO: persist run record.
        raise NotImplementedError("RunRepo.create_run is not implemented")

    def update_stage(self, _: str, __: str, ___: str) -> None:
        # TODO: persist stage status changes.
        raise NotImplementedError("RunRepo.update_stage is not implemented")

    def update_run(self, _: str, __: str) -> None:
        # TODO: persist run status changes.
        raise NotImplementedError("RunRepo.update_run is not implemented")

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class ProbeStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class ProbeResult:
    status: ProbeStatus
    detail: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)
    payload: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "detail": self.detail,
            "metrics": self.metrics,
            "payload": self.payload,
        }


from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List

from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult


@dataclass(frozen=True)
class ProbeCall:
    name: str
    func: Callable[[RunContext], ProbeResult]


@dataclass(frozen=True)
class Stage:
    name: str
    probes: List[ProbeCall]
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            object.__setattr__(self, 'tags', [])


@dataclass(frozen=True)
class Scenario:
    name: str
    tags: List[str]
    stages: List[Stage]





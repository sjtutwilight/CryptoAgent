from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class RunContext:
    """Runtime context passed to probes and scenarios.
    
    增强版本：支持可变的 state 字段用于 Stage 间状态传递
    """

    run_id: str
    scenario: str
    env: str = "local"
    stage: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)  # 新增：Stage 间共享状态

    def with_stage(self, stage: Optional[str]) -> "RunContext":
        """创建一个新的 context，更新 stage 字段，保持 state 引用"""
        return RunContext(
            run_id=self.run_id,
            scenario=self.scenario,
            env=self.env,
            stage=stage,
            metadata=dict(self.metadata),
            state=self.state,  # 共享同一个 state 对象
        )





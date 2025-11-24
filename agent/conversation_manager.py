"""
简单的会话管理器，负责短期记忆与压缩
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from config import AGENT_RUNTIME_CONFIG


@dataclass
class ConversationState:
    turns: Deque[Dict[str, str]] = field(default_factory=deque)
    summary: str = ""


class ConversationManager:
    """管理基于内存的会话历史"""

    def __init__(self):
        self.sessions: Dict[str, ConversationState] = {}
        self.max_messages = max(2, AGENT_RUNTIME_CONFIG["max_history_turns"] * 2)
        self.compact_threshold = AGENT_RUNTIME_CONFIG["history_compact_threshold"]
        self.preserve_recent = max(2, AGENT_RUNTIME_CONFIG["preserve_recent_turns"] * 2)
        self.summary_max_chars = max(500, AGENT_RUNTIME_CONFIG.get("history_summary_max_chars", 4000))

    def _ensure_state(self, session_id: str) -> ConversationState:
        if session_id not in self.sessions:
            self.sessions[session_id] = ConversationState(
                turns=deque(maxlen=self.max_messages)
            )
        return self.sessions[session_id]

    def append_turn(self, session_id: str, role: str, content: str) -> None:
        if not session_id:
            return
        state = self._ensure_state(session_id)
        state.turns.append({"role": role, "content": content})
        self._maybe_compact(state)

    def _maybe_compact(self, state: ConversationState) -> None:
        if self.compact_threshold <= 0:
            return
        total_chars = sum(len(turn["content"]) for turn in state.turns)
        if total_chars <= self.compact_threshold:
            return

        preserve_count = min(len(state.turns), self.preserve_recent)
        preserved = list(state.turns)[-preserve_count:]
        removed = list(state.turns)[:-preserve_count]

        if removed:
            removed_text = " ".join(
                f"{turn['role']}: {turn['content']}" for turn in removed
            )
            if state.summary:
                state.summary = f"{state.summary} {removed_text}"
            else:
                state.summary = removed_text
            self._trim_summary(state)

        state.turns = deque(preserved, maxlen=self.max_messages)

    def _trim_summary(self, state: ConversationState) -> None:
        if not state.summary:
            return
        if len(state.summary) <= self.summary_max_chars:
            return
        state.summary = state.summary[-self.summary_max_chars :]

    def get_history(self, session_id: Optional[str]) -> Dict[str, List[Dict[str, str]]]:
        if not session_id or session_id not in self.sessions:
            return {"summary": "", "turns": []}
        state = self.sessions[session_id]
        return {"summary": state.summary, "turns": list(state.turns)}

    def reset(self, session_id: Optional[str]) -> None:
        if session_id and session_id in self.sessions:
            del self.sessions[session_id]


# 全局会话管理器
conversation_manager = ConversationManager()

__all__ = ["conversation_manager", "ConversationManager"]

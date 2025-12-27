"""
领域工具聚合
"""
from .base import api_client
from .kline_tools import (
    KLINE_TOOLS,
    get_kline_indicators,
    get_kline_market_snapshots,
    get_kline_series,
)
from .perp_tools import (
    PERP_TOOLS,
    get_perp_context_series,
    get_perp_execution_series,
    get_perp_market_snapshots,
    get_perp_panel_series,
    get_perp_signals,
)
from .system_tools import SYSTEM_TOOLS, get_health_status
from .token_tools import (
    TOKEN_TOOLS,
    get_account_detail,
    get_token_distribution,
    get_token_list,
    get_token_overview,
    get_token_pnl,
)

ALL_TOOLS = SYSTEM_TOOLS + TOKEN_TOOLS + KLINE_TOOLS + PERP_TOOLS

__all__ = [
    "api_client",
    "get_health_status",
    "get_token_list",
    "get_token_overview",
    "get_token_distribution",
    "get_account_detail",
    "get_token_pnl",
    "get_kline_market_snapshots",
    "get_kline_series",
    "get_kline_indicators",
    "get_perp_market_snapshots",
    "get_perp_execution_series",
    "get_perp_context_series",
    "get_perp_panel_series",
    "get_perp_signals",
    "ALL_TOOLS",
]

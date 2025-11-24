"""
领域工具聚合层

保持对 `api_client` 与 `AVAILABLE_TOOLS` 的兼容导出，供 Agent 与API服务使用。
"""
from tools import (
    ALL_TOOLS,
    api_client,
    get_account_detail,
    get_health_status,
    get_kline_indicators,
    get_kline_market_snapshots,
    get_kline_series,
    get_perp_context_series,
    get_perp_execution_series,
    get_perp_market_snapshots,
    get_perp_panel_series,
    get_perp_signals,
    get_token_distribution,
    get_token_list,
    get_token_overview,
    get_token_pnl,
)

AVAILABLE_TOOLS = ALL_TOOLS

__all__ = [
    "api_client",
    "AVAILABLE_TOOLS",
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
]

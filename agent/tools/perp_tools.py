"""
永续合约相关API工具
"""
import json
from typing import Dict, List, Optional
from urllib.parse import quote

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from .base import api_client, to_csv


QUOTE_SUFFIXES = [
    "PERP",
    "-PERP",
    "_PERP",
    "USDT",
    "-USDT",
    "_USDT",
    "USD",
    "-USD",
    "_USD",
    "USDC",
    "-USDC",
    "_USDC",
]


def _normalize_exchange(exchange: Optional[str]) -> Optional[str]:
    if not exchange:
        return None
    cleaned = exchange.strip()
    return cleaned.lower() if cleaned else None


def _normalize_symbol(symbol: Optional[str]) -> Optional[str]:
    if not symbol:
        return None
    cleaned = symbol.strip().upper()
    for ch in ["/", ":", " "]:
        cleaned = cleaned.replace(ch, "")
    for suffix in QUOTE_SUFFIXES:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]
            break
    cleaned = cleaned.strip("-_")
    return cleaned or None


def _normalize_symbols(values: Optional[List[str]]) -> Optional[List[str]]:
    if not values:
        return None
    normalized = [_normalize_symbol(item) for item in values]
    normalized = [item for item in normalized if item]
    return normalized or None


def _normalize_exchanges(values: Optional[List[str]]) -> Optional[List[str]]:
    if not values:
        return None
    normalized = [_normalize_exchange(item) for item in values]
    normalized = [item for item in normalized if item]
    return normalized or None


def get_perp_market_snapshots(
    symbols: Optional[List[str]] = None,
    exchange: Optional[str] = None,
    algo: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    sort_by: str = "volume",
    order: str = "desc",
) -> Dict:
    normalized_symbols = _normalize_symbols(symbols)
    params = {
        "symbols": to_csv(normalized_symbols),
        "exchange": _normalize_exchange(exchange),
        "algo": algo,
        "page": page,
        "pageSize": page_size,
        "sortBy": sort_by,
        "order": order,
    }
    return api_client._make_request("GET", "/perps/markets", params=params)


def get_perp_execution_series(
    symbol: str,
    exchange: Optional[str] = None,
    algo: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1800,
) -> Dict:
    normalized_symbol = _normalize_symbol(symbol)
    params = {
        "exchange": _normalize_exchange(exchange),
        "algo": algo,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit,
    }
    encoded_symbol = quote(normalized_symbol or "", safe="")
    return api_client._make_request(
        "GET", f"/perps/{encoded_symbol}/execution", params=params
    )


def get_perp_context_series(
    symbol: str,
    exchange: Optional[str] = None,
    algo: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1440,
) -> Dict:
    normalized_symbol = _normalize_symbol(symbol)
    params = {
        "exchange": _normalize_exchange(exchange),
        "algo": algo,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit,
    }
    encoded_symbol = quote(normalized_symbol or "", safe="")
    return api_client._make_request(
        "GET", f"/perps/{encoded_symbol}/context", params=params
    )


def get_perp_panel_series(
    symbol: str,
    exchange: Optional[str] = None,
    algo: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1440,
) -> Dict:
    normalized_symbol = _normalize_symbol(symbol)
    params = {
        "exchange": _normalize_exchange(exchange),
        "algo": algo,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit,
    }
    encoded_symbol = quote(normalized_symbol or "", safe="")
    return api_client._make_request(
        "GET", f"/perps/{encoded_symbol}/panel", params=params
    )


def get_perp_signals(
    symbols: Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    types: Optional[List[str]] = None,
    levels: Optional[List[str]] = None,
    algo: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 200,
) -> Dict:
    normalized_symbols = _normalize_symbols(symbols)
    normalized_exchanges = _normalize_exchanges(exchanges)
    params = {
        "symbols": to_csv(normalized_symbols),
        "exchanges": to_csv(normalized_exchanges),
        "types": to_csv(types),
        "levels": to_csv(levels),
        "algo": algo,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit,
    }
    return api_client._make_request("GET", "/perps/signals", params=params)


class PerpMarketInput(BaseModel):
    symbols: Optional[List[str]] = Field(default=None, description="交易对过滤")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    algo: Optional[str] = Field(default=None, description="算法版本，如 prod/exp")
    page: int = Field(default=1, description="页码，从1开始")
    page_size: int = Field(default=20, description="每页数量，最大500")
    sort_by: str = Field(default="volume", description="排序字段")
    order: str = Field(default="desc", description="排序方向")


class PerpExecutionInput(BaseModel):
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    algo: Optional[str] = Field(default=None, description="算法版本过滤")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1800, description="返回条数，最大10000")


class PerpContextInput(BaseModel):
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    algo: Optional[str] = Field(default=None, description="算法版本过滤")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1440, description="返回条数，最大1440")


class PerpPanelInput(BaseModel):
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    algo: Optional[str] = Field(default=None, description="算法版本过滤")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1440, description="返回条数，最大1440")


class PerpSignalsInput(BaseModel):
    symbols: Optional[List[str]] = Field(default=None, description="交易对过滤")
    exchanges: Optional[List[str]] = Field(default=None, description="交易所过滤")
    types: Optional[List[str]] = Field(default=None, description="信号类型过滤")
    levels: Optional[List[str]] = Field(default=None, description="信号等级过滤")
    algo: Optional[str] = Field(default=None, description="算法版本过滤")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=200, description="返回条数，最大1000")


class PerpMarketTool(BaseTool):
    name: str = "get_perp_market_snapshots"
    description: str = """获取永续合约市场最新快照，用于大盘排序与选取流动性良好的合约。
    建议输出：价格、成交量、avgSpreadBps、avgDepth50k、crowdingScore、liquidityRegime，并明确 exchange/algo/page 参数。
    可接受 BTC、BTCUSDT、BTC-PERP 等输入，会自动映射为数据库使用的短符号。"""
    args_schema: type = PerpMarketInput

    def _run(
        self,
        symbols: Optional[List[str]] = None,
        exchange: Optional[str] = None,
        algo: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "volume",
        order: str = "desc",
    ) -> str:
        result = get_perp_market_snapshots(symbols, exchange, algo, page, page_size, sort_by, order)
        return json.dumps(result, ensure_ascii=False)


class PerpExecutionTool(BaseTool):
    name: str = "get_perp_execution_series"
    description: str = """获取秒级执行面指标，如点差、盘口深度、成交量等。
    适用于判断流动性恶化、交易冲击。总结时请引用最近时间窗口内的 spreadBps、ofi、impact/深度指标与成交量。"""
    args_schema: type = PerpExecutionInput

    def _run(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        algo: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1800,
    ) -> str:
        result = get_perp_execution_series(symbol, exchange, algo, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


class PerpContextTool(BaseTool):
    name: str = "get_perp_context_series"
    description: str = """获取分钟级语境指标，包括资金费率、持仓量、OI变化等。
    输出中突出 fundingRate/fundingEma24h、oiUsd、oiDeltaPct、isOiCarried 等字段，并说明时间范围。"""
    args_schema: type = PerpContextInput

    def _run(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        algo: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1440,
    ) -> str:
        result = get_perp_context_series(symbol, exchange, algo, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


class PerpPanelTool(BaseTool):
    name: str = "get_perp_panel_series"
    description: str = """获取面板指标（执行面+语境面整合），适合做综合得分跟踪。
    常与执行/语境结果一起使用，强调 crowdingScore、liquidityRegime、avgImpact50kBps 等综合得分。"""
    args_schema: type = PerpPanelInput

    def _run(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        algo: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1440,
    ) -> str:
        result = get_perp_panel_series(symbol, exchange, algo, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


class PerpSignalsTool(BaseTool):
    name: str = "get_perp_signals"
    description: str = """获取永续合约异常信号，支持按交易对、交易所、信号类型和等级筛选。
    建议按 signalLevel 排序展示最近告警，提及 signalType、metricName、threshold 及 contextJson 关键原因。"""
    args_schema: type = PerpSignalsInput

    def _run(
        self,
        symbols: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        types: Optional[List[str]] = None,
        levels: Optional[List[str]] = None,
        algo: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 200,
    ) -> str:
        result = get_perp_signals(symbols, exchanges, types, levels, algo, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


PERP_TOOLS = [
    PerpMarketTool(),
    PerpExecutionTool(),
    PerpContextTool(),
    PerpPanelTool(),
    PerpSignalsTool(),
]

__all__ = [
    "get_perp_market_snapshots",
    "get_perp_execution_series",
    "get_perp_context_series",
    "get_perp_panel_series",
    "get_perp_signals",
    "PERP_TOOLS",
]

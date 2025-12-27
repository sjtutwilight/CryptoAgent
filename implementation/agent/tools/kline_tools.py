"""
K线相关API工具
"""
import json
import logging
from typing import Any, Dict, List, Optional

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field
from urllib.parse import quote

from .base import api_client, to_csv

logger = logging.getLogger(__name__)

SUPPORTED_KLINE_INTERVALS = {"1m", "5m"}


def _normalize_kline_interval(interval: Optional[str]) -> str:
    default_interval = "1m"
    if not interval:
        return default_interval

    normalized = interval.strip().lower()
    if not normalized:
        return default_interval

    if normalized.endswith("m") and normalized[:-1].isdigit():
        normalized = f"{int(normalized[:-1])}m"

    if normalized in SUPPORTED_KLINE_INTERVALS:
        return normalized

    fallback = "5m" if normalized.endswith(("m", "h", "d", "w")) else default_interval
    if normalized.endswith("m") and normalized[:-1].isdigit():
        minutes = int(normalized[:-1])
        fallback = "5m" if minutes >= 5 else "1m"

    logger.warning("Unsupported kline interval '%s', fallback to '%s'", interval, fallback)
    return fallback if fallback in SUPPORTED_KLINE_INTERVALS else default_interval


def get_kline_market_snapshots(
    symbols: Optional[List[str]] = None,
    exchange: Optional[str] = None,
    interval: str = "1m",
    page: int = 1,
    page_size: int = 50,
    sort_by: str = "volume",
    order: str = "desc",
) -> Dict[str, Any]:
    normalized_interval = _normalize_kline_interval(interval)
    params = {
        "symbols": to_csv(symbols),
        "exchange": exchange,
        "interval": normalized_interval,
        "page": page,
        "pageSize": page_size,
        "sortBy": sort_by,
        "order": order,
    }
    return api_client._make_request("GET", "/klines/markets", params=params)


def get_kline_series(
    symbol: str,
    exchange: Optional[str] = None,
    interval: str = "1m",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1000,
) -> Dict[str, Any]:
    normalized_interval = _normalize_kline_interval(interval)
    params = {
        "exchange": exchange,
        "interval": normalized_interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit,
    }
    encoded_symbol = quote(symbol, safe="")
    return api_client._make_request("GET", f"/klines/{encoded_symbol}/candles", params=params)


def get_kline_indicators(
    symbol: str,
    exchange: Optional[str] = None,
    interval: str = "1m",
    indicators: Optional[List[str]] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1000,
) -> Dict[str, Any]:
    normalized_interval = _normalize_kline_interval(interval)
    params = {
        "exchange": exchange,
        "interval": normalized_interval,
        "indicators": to_csv(indicators),
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit,
    }
    encoded_symbol = quote(symbol, safe="")
    return api_client._make_request("GET", f"/klines/{encoded_symbol}/indicators", params=params)


class KlineMarketInput(BaseModel):
    symbols: Optional[List[str]] = Field(default=None, description="交易对列表，如 ['BTCUSDT']")
    exchange: Optional[str] = Field(default=None, description="交易所")
    interval: str = Field(default="1m", description="K线周期，仅支持1m或5m，其他值将自动回退")
    page: int = Field(default=1, description="页码，从1开始")
    page_size: int = Field(default=50, description="每页数量，最大500")
    sort_by: str = Field(default="volume", description="排序字段: volume/change等")
    order: str = Field(default="desc", description="排序方向: asc 或 desc")


class KlineSeriesInput(BaseModel):
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    interval: str = Field(default="1m", description="K线周期，仅支持1m或5m，其他值将自动回退")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1000, description="返回条数，最大5000")


class KlineIndicatorInput(BaseModel):
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    interval: str = Field(default="1m", description="K线周期，仅支持1m或5m，其他值将自动回退")
    indicators: Optional[List[str]] = Field(default=None, description="指标列表，如 ['RSI','MACD']")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1000, description="返回条数，最大5000")


class KlineMarketTool(BaseTool):
    name: str = "get_kline_market_snapshots"
    description: str = """获取K线市场快照，适用于大盘排序、筛选高动量或高成交量标的（仅支持1m/5m）。
    建议输出：价格、涨跌幅、成交量、振幅、信号字段，并明确 interval/page/排序参数。
    可与永续市场快照结合，对比现货与永续情绪差异。"""
    args_schema: type = KlineMarketInput

    def _run(
        self,
        symbols: Optional[List[str]] = None,
        exchange: Optional[str] = None,
        interval: str = "1m",
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "volume",
        order: str = "desc",
    ) -> str:
        result = get_kline_market_snapshots(symbols, exchange, interval, page, page_size, sort_by, order)
        return json.dumps(result, ensure_ascii=False)


class KlineSeriesTool(BaseTool):
    name: str = "get_kline_series"
    description: str = """查询历史K线序列，支持指定交易所、时间区间和返回条数（仅支持1m/5m）。
    建议总结最新收盘价、累计涨跌%、成交量及信号字段，并注明 limit/时间窗口。
    当需要与永续执行面对照时，请保持时间窗口一致并在结论中说明差异。"""
    args_schema: type = KlineSeriesInput

    def _run(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        interval: str = "1m",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1000,
    ) -> str:
        result = get_kline_series(symbol, exchange, interval, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


class KlineIndicatorTool(BaseTool):
    name: str = "get_kline_indicators"
    description: str = """查询指定交易对的技术指标输出，支持一次拉取多个指标（仅支持1m/5m）。
    输出建议：报告最新指标值及阈值位置，解释多/空信号与价格走势是否一致。
    将指标信号与永续资金费率/拥挤度对比，可判断现货与永续的共振或背离。"""
    args_schema: type = KlineIndicatorInput

    def _run(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        interval: str = "1m",
        indicators: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1000,
    ) -> str:
        result = get_kline_indicators(symbol, exchange, interval, indicators, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


KLINE_TOOLS = [KlineMarketTool(), KlineSeriesTool(), KlineIndicatorTool()]

__all__ = [
    "get_kline_market_snapshots",
    "get_kline_series",
    "get_kline_indicators",
    "KLINE_TOOLS",
]

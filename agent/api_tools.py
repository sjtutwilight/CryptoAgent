"""
API工具包装器
将后端NewAnalyticsController的API接口包装成LangChain工具
"""
import requests
import json
from typing import Dict, Any, List, Optional
from urllib.parse import quote
from pydantic import BaseModel, Field
import logging
from config import BACKEND_API_CONFIG

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPPORTED_KLINE_INTERVALS = {"1m", "5m"}


def _to_csv(values: Optional[Any]) -> Optional[str]:
    """将列表或集合转换为逗号分隔字符串"""
    if values is None:
        return None
    if isinstance(values, str):
        cleaned = values.strip()
        return cleaned or None
    if isinstance(values, (list, tuple, set)):
        tokens = [str(v).strip() for v in values if v is not None and str(v).strip()]
        if not tokens:
            return None
        return ",".join(tokens)
    # 其他类型直接转为字符串
    return str(values)


def _sanitize_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """去除参数中值为None的字段"""
    if not params:
        return None
    sanitized = {key: value for key, value in params.items() if value is not None}
    return sanitized or None


def _normalize_kline_interval(interval: Optional[str]) -> str:
    """将任意间隔规范化为支持的K线周期"""
    default_interval = "1m"
    if not interval:
        return default_interval

    normalized = interval.strip().lower()
    if not normalized:
        return default_interval

    # 统一大小写，例如 1M -> 1m
    if normalized.endswith("m") and normalized[:-1].isdigit():
        normalized = f"{int(normalized[:-1])}m"

    if normalized in SUPPORTED_KLINE_INTERVALS:
        return normalized

    # 非分钟级或超出支持范围的周期，回退到5m
    fallback = "5m"
    if normalized.endswith("m") and normalized[:-1].isdigit():
        minutes = int(normalized[:-1])
        fallback = "5m" if minutes >= 5 else "1m"
    elif normalized.endswith(("h", "d", "w")):
        fallback = "5m"
    if fallback not in SUPPORTED_KLINE_INTERVALS:
        fallback = default_interval

    logger.warning("Unsupported kline interval '%s', fallback to '%s'", interval, fallback)
    return fallback


class APIConfig:
    """API配置"""
    BASE_URL = BACKEND_API_CONFIG["base_url"]  # 后端API基础URL
    TIMEOUT = BACKEND_API_CONFIG["timeout"]    # 请求超时时间

class APIClient:
    """API客户端"""
    
    def __init__(self):
        self.base_url = APIConfig.BASE_URL
        self.timeout = APIConfig.TIMEOUT
        
    def _make_request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict[str, Any]:
        """发送API请求"""
        url = f"{self.base_url}{endpoint}"
        params = _sanitize_params(params)
        data = _sanitize_params(data)

        try:
            logger.info(f"发送{method}请求: {url}, params: {params}")
            
            if method.upper() == "GET":
                response = requests.get(url, params=params, timeout=self.timeout)
            elif method.upper() == "POST":
                response = requests.post(url, json=data, timeout=self.timeout)
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"API请求成功，状态码: {response.status_code}")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API请求失败: {e}")
            return {
                "status": "error",
                "message": f"API请求失败: {str(e)}"
            }
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}")
            return {
                "status": "error", 
                "message": f"响应解析失败: {str(e)}"
            }

# 全局API客户端
api_client = APIClient()

def get_health_status() -> Dict[str, Any]:
    """
    获取API健康状态
    
    Returns:
        Dict: 健康状态信息
    """
    return api_client._make_request("GET", "/health")

def get_token_list(page: int = 1, page_size: int = 50, sort_by: str = "mcap", order: str = "desc") -> Dict[str, Any]:
    """
    获取代币列表
    
    Args:
        page: 页码，默认1
        page_size: 每页大小，默认50
        sort_by: 排序字段，可选: mcap(市值), volume(交易量), holders(持有者数), price(价格)
        order: 排序方向，asc或desc
        
    Returns:
        Dict: 包含代币列表的响应
    """
    params = {
        "page": page,
        "pageSize": page_size,
        "sortBy": sort_by,
        "order": order
    }
    
    return api_client._make_request("GET", "/tokens/list", params=params)

def get_token_overview(token_id: int, time_range: str = "5min") -> Dict[str, Any]:
    """
    获取代币完整概览
    
    Args:
        token_id: 代币ID
        time_range: 时间窗口，可选: 20s/1min/5min/1h
        
    Returns:
        Dict: 代币概览数据，包含基础信息+宏观指标+交易流分析
    """
    params = {"timeRange": time_range}
    
    return api_client._make_request("GET", f"/tokens/{token_id}/overview", params=params)

def get_token_distribution(token_id: int, time_range: str = "5min") -> Dict[str, Any]:
    """
    获取代币分布分析
    
    Args:
        token_id: 代币ID
        time_range: 时间范围，默认5min
        
    Returns:
        Dict: 代币持有者分布分析，包括持有者统计、标签分布、Top持币地址等
    """
    params = {"timeRange": time_range}
    
    return api_client._make_request("GET", f"/tokens/{token_id}/distribution", params=params)

def get_account_detail(account_id: int) -> Dict[str, Any]:
    """
    获取账户详情
    
    Args:
        account_id: 账户ID
        
    Returns:
        Dict: 账户详细信息，包含基础信息、资产持仓、转账历史等
    """
    return api_client._make_request("GET", f"/accounts/{account_id}")

def get_token_pnl(token_id: int, time_range: str = "1min", top_limit: int = 50) -> Dict[str, Any]:
    """
    获取代币PnL分析数据
    
    Args:
        token_id: 代币ID
        time_range: 时间范围，默认1min
        top_limit: Top PnL排行榜数量限制，默认50
        
    Returns:
        Dict: 代币PnL分析数据，包含PnL排行榜、宏观指标和汇总统计
    """
    params = {
        "timeRange": time_range,
        "topLimit": top_limit
    }
    
    return api_client._make_request("GET", f"/tokens/{token_id}/pnl", params=params)


def get_kline_market_snapshots(
        symbols: Optional[List[str]] = None,
        exchange: Optional[str] = None,
        interval: str = "1m",
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "volume",
        order: str = "desc") -> Dict[str, Any]:
    """
    获取K线市场快照
    """
    normalized_interval = _normalize_kline_interval(interval)
    params = {
        "symbols": _to_csv(symbols),
        "exchange": exchange,
        "interval": normalized_interval,
        "page": page,
        "pageSize": page_size,
        "sortBy": sort_by,
        "order": order
    }
    return api_client._make_request("GET", "/klines/markets", params=params)


def get_kline_series(
        symbol: str,
        exchange: Optional[str] = None,
        interval: str = "1m",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1000) -> Dict[str, Any]:
    """
    获取K线时间序列
    """
    normalized_interval = _normalize_kline_interval(interval)
    params = {
        "exchange": exchange,
        "interval": normalized_interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
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
        limit: int = 1000) -> Dict[str, Any]:
    """
    获取K线指标时间序列
    """
    normalized_interval = _normalize_kline_interval(interval)
    params = {
        "exchange": exchange,
        "interval": normalized_interval,
        "indicators": _to_csv(indicators),
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    encoded_symbol = quote(symbol, safe="")
    return api_client._make_request("GET", f"/klines/{encoded_symbol}/indicators", params=params)


def get_perp_market_snapshots(
        symbols: Optional[List[str]] = None,
        exchange: Optional[str] = None,
        algo: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "volume",
        order: str = "desc") -> Dict[str, Any]:
    """
    获取永续市场快照
    """
    params = {
        "symbols": _to_csv(symbols),
        "exchange": exchange,
        "algo": algo,
        "page": page,
        "pageSize": page_size,
        "sortBy": sort_by,
        "order": order
    }
    return api_client._make_request("GET", "/perps/markets", params=params)


def get_perp_execution_series(
        symbol: str,
        exchange: Optional[str] = None,
        algo: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1800) -> Dict[str, Any]:
    """
    获取永续执行面时间序列
    """
    params = {
        "exchange": exchange,
        "algo": algo,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    encoded_symbol = quote(symbol, safe="")
    return api_client._make_request("GET", f"/perps/{encoded_symbol}/execution", params=params)


def get_perp_context_series(
        symbol: str,
        exchange: Optional[str] = None,
        algo: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1440) -> Dict[str, Any]:
    """
    获取永续语境面时间序列
    """
    params = {
        "exchange": exchange,
        "algo": algo,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    encoded_symbol = quote(symbol, safe="")
    return api_client._make_request("GET", f"/perps/{encoded_symbol}/context", params=params)


def get_perp_panel_series(
        symbol: str,
        exchange: Optional[str] = None,
        algo: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1440) -> Dict[str, Any]:
    """
    获取永续面板时间序列
    """
    params = {
        "exchange": exchange,
        "algo": algo,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    encoded_symbol = quote(symbol, safe="")
    return api_client._make_request("GET", f"/perps/{encoded_symbol}/panel", params=params)


def get_perp_signals(
        symbols: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        types: Optional[List[str]] = None,
        levels: Optional[List[str]] = None,
        algo: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 200) -> Dict[str, Any]:
    """
    获取永续异常信号
    """
    params = {
        "symbols": _to_csv(symbols),
        "exchanges": _to_csv(exchanges),
        "types": _to_csv(types),
        "levels": _to_csv(levels),
        "algo": algo,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    return api_client._make_request("GET", "/perps/signals", params=params)

# ====== LangChain 工具定义 ======

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

class TokenListInput(BaseModel):
    """代币列表查询输入"""
    page: int = Field(default=1, description="页码")
    page_size: int = Field(default=50, description="每页大小")
    sort_by: str = Field(default="mcap", description="排序字段: mcap/volume/holders/price")
    order: str = Field(default="desc", description="排序方向: asc/desc")

class TokenOverviewInput(BaseModel):
    """代币概览查询输入"""
    token_id: int = Field(description="代币ID")
    time_range: str = Field(default="5min", description="时间窗口: 20s/1min/5min/1h")

class TokenDistributionInput(BaseModel):
    """代币分布查询输入"""
    token_id: int = Field(description="代币ID")
    time_range: str = Field(default="5min", description="时间范围")

class AccountDetailInput(BaseModel):
    """账户详情查询输入"""
    account_id: int = Field(description="账户ID")

class TokenPnLInput(BaseModel):
    """代币PnL查询输入"""
    token_id: int = Field(description="代币ID")
    time_range: str = Field(default="1min", description="时间范围")
    top_limit: int = Field(default=50, description="Top PnL排行榜数量限制")


class KlineMarketInput(BaseModel):
    """K线市场快照查询输入"""
    symbols: Optional[List[str]] = Field(default=None, description="交易对列表，示例: ['BTCUSDT','ETHUSDT']")
    exchange: Optional[str] = Field(default=None, description="交易所，如 binance/okx/hyperliquid")
    interval: str = Field(default="1m", description="K线周期，仅支持1m或5m，其他值将自动回退")
    page: int = Field(default=1, description="页码，从1开始")
    page_size: int = Field(default=50, description="每页数量，最大500")
    sort_by: str = Field(default="volume", description="排序字段: volume/change/amplitude/tradeCount等")
    order: str = Field(default="desc", description="排序方向: asc 或 desc")


class KlineSeriesInput(BaseModel):
    """K线时间序列查询输入"""
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    interval: str = Field(default="1m", description="K线周期，仅支持1m或5m，其他值将自动回退")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1000, description="返回条数，最大5000")


class KlineIndicatorInput(BaseModel):
    """K线指标查询输入"""
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    interval: str = Field(default="1m", description="K线周期，仅支持1m或5m，其他值将自动回退")
    indicators: Optional[List[str]] = Field(default=None, description="指标列表，如 ['RSI','MACD']")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1000, description="返回条数，最大5000")


class PerpMarketInput(BaseModel):
    """永续市场快照输入"""
    symbols: Optional[List[str]] = Field(default=None, description="交易对列表，如 ['BTCUSDT']")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    algo: Optional[str] = Field(default=None, description="算法版本，如 prod/exp")
    page: int = Field(default=1, description="页码，从1开始")
    page_size: int = Field(default=20, description="每页数量，最大500")
    sort_by: str = Field(default="volume", description="排序字段: volume/spread/basis等")
    order: str = Field(default="desc", description="排序方向")


class PerpExecutionInput(BaseModel):
    """永续执行面查询输入"""
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    algo: Optional[str] = Field(default=None, description="算法版本过滤")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1800, description="返回条数，最大10000")


class PerpContextInput(BaseModel):
    """永续语境面查询输入"""
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    algo: Optional[str] = Field(default=None, description="算法版本过滤")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1440, description="返回条数，最大1440")


class PerpPanelInput(BaseModel):
    """永续面板查询输入"""
    symbol: str = Field(description="交易对，如 BTCUSDT")
    exchange: Optional[str] = Field(default=None, description="交易所过滤")
    algo: Optional[str] = Field(default=None, description="算法版本过滤")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=1440, description="返回条数，最大1440")


class PerpSignalsInput(BaseModel):
    """永续异常信号查询输入"""
    symbols: Optional[List[str]] = Field(default=None, description="交易对过滤")
    exchanges: Optional[List[str]] = Field(default=None, description="交易所过滤")
    types: Optional[List[str]] = Field(default=None, description="信号类型过滤")
    levels: Optional[List[str]] = Field(default=None, description="信号等级过滤")
    algo: Optional[str] = Field(default=None, description="算法版本过滤")
    start_time: Optional[str] = Field(default=None, description="起始时间 ISO8601")
    end_time: Optional[str] = Field(default=None, description="结束时间 ISO8601")
    limit: int = Field(default=200, description="返回条数，最大1000")

class HealthCheckTool(BaseTool):
    """健康检查工具"""
    name: str = "health_check"
    description: str = "检查API服务健康状态"
    
    def _run(self) -> str:
        result = get_health_status()
        return json.dumps(result, ensure_ascii=False)

class TokenListTool(BaseTool):
    """代币列表工具"""
    name: str = "get_token_list"
    description: str = """获取代币列表，包含基础信息和关键指标。
    用于回答以下类型的问题：
    - 查看所有代币
    - 按市值/交易量/持有者数排序的代币
    - 获取代币概览列表"""
    args_schema: type = TokenListInput
    
    def _run(self, page: int = 1, page_size: int = 50, sort_by: str = "mcap", order: str = "desc") -> str:
        result = get_token_list(page, page_size, sort_by, order)
        return json.dumps(result, ensure_ascii=False)

class TokenOverviewTool(BaseTool):
    """代币概览工具"""
    name: str = "get_token_overview"
    description: str = """获取代币的完整概览信息，包含基础信息、宏观指标和交易流分析。
    用于回答以下类型的问题：
    - 某个代币的详细信息
    - 代币的价格、市值、交易量等指标
    - 代币的交易流分析
    - 代币的基础数据概览"""
    args_schema: type = TokenOverviewInput
    
    def _run(self, token_id: int, time_range: str = "5min") -> str:
        result = get_token_overview(token_id, time_range)
        return json.dumps(result, ensure_ascii=False)

class TokenDistributionTool(BaseTool):
    """代币分布工具"""
    name: str = "get_token_distribution"
    description: str = """获取代币的持有者分布分析，包括持有者统计、标签分布、Top持币地址等。
    用于回答以下类型的问题：
    - 代币的持有者分布
    - 巨鲸持币情况
    - 代币的标签分析（fresh、whale、smart、cex等）
    - Top持币地址排行"""
    args_schema: type = TokenDistributionInput
    
    def _run(self, token_id: int, time_range: str = "5min") -> str:
        result = get_token_distribution(token_id, time_range)
        return json.dumps(result, ensure_ascii=False)

class AccountDetailTool(BaseTool):
    """账户详情工具"""
    name: str = "get_account_detail"
    description: str = """获取账户的详细信息，包含基础信息、资产持仓、转账历史等。
    用于回答以下类型的问题：
    - 获取账户详情
    - 查看账户的资产持仓
    - 账户的转账历史
    - 账户的标签信息
    - 账户的资产统计"""
    args_schema: type = AccountDetailInput
    
    def _run(self, account_id: int) -> str:
        result = get_account_detail(account_id)
        return json.dumps(result, ensure_ascii=False)

class TokenPnLTool(BaseTool):
    """代币PnL分析工具"""
    name: str = "get_token_pnl"
    description: str = """获取代币的PnL分析数据，包含PnL排行榜、宏观指标和汇总统计。
    用于回答以下类型的问题：
    - 代币的盈亏分析
    - Top PnL排行榜
    - 代币的盈利者和亏损者统计
    - PnL宏观指标"""
    args_schema: type = TokenPnLInput
    
    def _run(self, token_id: int, time_range: str = "1min", top_limit: int = 50) -> str:
        result = get_token_pnl(token_id, time_range, top_limit)
        return json.dumps(result, ensure_ascii=False)


class KlineMarketTool(BaseTool):
    """K线市场快照工具"""
    name: str = "get_kline_market_snapshots"
    description: str = """获取K线市场快照，适用于大盘排序、筛选高动量或高成交量标的（当前仅支持1m/5m）。
    常见问题：
    - 最近成交量最高的交易对有哪些？
    - 指定交易所某周期的涨幅排名？
    - 某几个交易对的最新K线快照详情"""
    args_schema: type = KlineMarketInput

    def _run(
            self,
            symbols: Optional[List[str]] = None,
            exchange: Optional[str] = None,
            interval: str = "1m",
            page: int = 1,
            page_size: int = 50,
            sort_by: str = "volume",
            order: str = "desc") -> str:
        result = get_kline_market_snapshots(symbols, exchange, interval, page, page_size, sort_by, order)
        return json.dumps(result, ensure_ascii=False)


class KlineSeriesTool(BaseTool):
    """K线时间序列工具"""
    name: str = "get_kline_series"
    description: str = """查询历史K线序列，支持指定交易所、时间区间和返回条数（周期仅支持1m/5m，如输入其他值将自动回退）。
    常见问题：
    - 获取某交易对最近N根K线用于画图
    - 分析某个时间段内的涨跌、均线、信号
    - 对比不同交易所同一交易对的走势"""
    args_schema: type = KlineSeriesInput

    def _run(
            self,
            symbol: str,
            exchange: Optional[str] = None,
            interval: str = "1m",
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            limit: int = 1000) -> str:
        result = get_kline_series(symbol, exchange, interval, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


class KlineIndicatorTool(BaseTool):
    """K线指标时间序列工具"""
    name: str = "get_kline_indicators"
    description: str = """查询指定交易对的技术指标输出，支持一次拉取多个指标（周期仅支持1m/5m，其他值自动回退）。
    常见问题：
    - 获取MACD/RSI指标序列
    - 观察指标给出的买卖信号
    - 结合价格走势分析指标背离"""
    args_schema: type = KlineIndicatorInput

    def _run(
            self,
            symbol: str,
            exchange: Optional[str] = None,
            interval: str = "1m",
            indicators: Optional[List[str]] = None,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            limit: int = 1000) -> str:
        result = get_kline_indicators(symbol, exchange, interval, indicators, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


class PerpMarketTool(BaseTool):
    """永续市场快照工具"""
    name: str = "get_perp_market_snapshots"
    description: str = """获取永续合约市场最新快照，用于大盘排序与选取流动性良好的合约。
    常见问题：
    - 当前点差/深度最好的合约
    - 某交易所永续合约的成交量排名
    - 关注交易对的最新面板指标"""
    args_schema: type = PerpMarketInput

    def _run(
            self,
            symbols: Optional[List[str]] = None,
            exchange: Optional[str] = None,
            algo: Optional[str] = None,
            page: int = 1,
            page_size: int = 20,
            sort_by: str = "volume",
            order: str = "desc") -> str:
        result = get_perp_market_snapshots(symbols, exchange, algo, page, page_size, sort_by, order)
        return json.dumps(result, ensure_ascii=False)


class PerpExecutionTool(BaseTool):
    """永续执行面时间序列工具"""
    name: str = "get_perp_execution_series"
    description: str = """获取秒级执行面指标，如点差、盘口深度、成交量等。
    常见问题：
    - 监控最近30分钟的流动性变化
    - 分析点差、冲击成本是否异常
    - 结合成交量判断交易活跃度"""
    args_schema: type = PerpExecutionInput

    def _run(
            self,
            symbol: str,
            exchange: Optional[str] = None,
            algo: Optional[str] = None,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            limit: int = 1800) -> str:
        result = get_perp_execution_series(symbol, exchange, algo, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


class PerpContextTool(BaseTool):
    """永续语境面时间序列工具"""
    name: str = "get_perp_context_series"
    description: str = """获取分钟级语境指标，包括资金费率、持仓量、OI变化等。
    常见问题：
    - 分析资金费率异常与持仓量共振
    - 判断是否存在强制平仓或杠杆攀升风险
    - 计算某时间段的持仓变化"""
    args_schema: type = PerpContextInput

    def _run(
            self,
            symbol: str,
            exchange: Optional[str] = None,
            algo: Optional[str] = None,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            limit: int = 1440) -> str:
        result = get_perp_context_series(symbol, exchange, algo, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


class PerpPanelTool(BaseTool):
    """永续面板时间序列工具"""
    name: str = "get_perp_panel_series"
    description: str = """获取面板指标（执行面+语境面整合），适合做综合得分跟踪。
    常见问题：
    - 观察拥挤度、流动性 regime 的演化
    - 跟踪策略得分或信号强度
    - 分析跨时段的执行/语境指标组合"""
    args_schema: type = PerpPanelInput

    def _run(
            self,
            symbol: str,
            exchange: Optional[str] = None,
            algo: Optional[str] = None,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            limit: int = 1440) -> str:
        result = get_perp_panel_series(symbol, exchange, algo, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)


class PerpSignalsTool(BaseTool):
    """永续异常信号工具"""
    name: str = "get_perp_signals"
    description: str = """获取永续合约异常信号，支持按交易对、交易所、信号类型和等级筛选。
    常见问题：
    - 查看最近的高危信号
    - 过滤指定交易所的拥挤或执行面异常
    - 生成自动化告警或建议"""
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
            limit: int = 200) -> str:
        result = get_perp_signals(symbols, exchanges, types, levels, algo, start_time, end_time, limit)
        return json.dumps(result, ensure_ascii=False)

# 工具列表
AVAILABLE_TOOLS = [
    HealthCheckTool(),
    TokenListTool(),
    TokenOverviewTool(), 
    TokenDistributionTool(),
    AccountDetailTool(),
    TokenPnLTool(),
    KlineMarketTool(),
    KlineSeriesTool(),
    KlineIndicatorTool(),
    PerpMarketTool(),
    PerpExecutionTool(),
    PerpContextTool(),
    PerpPanelTool(),
    PerpSignalsTool()
]

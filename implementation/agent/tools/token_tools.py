"""
代币与账户相关API封装
"""
import json
from typing import Dict

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from .base import api_client


def get_token_list(
    page: int = 1, page_size: int = 50, sort_by: str = "mcap", order: str = "desc"
) -> Dict:
    params = {"page": page, "pageSize": page_size, "sortBy": sort_by, "order": order}
    return api_client._make_request("GET", "/tokens/list", params=params)


def get_token_overview(token_id: int, time_range: str = "5min") -> Dict:
    params = {"timeRange": time_range}
    return api_client._make_request("GET", f"/tokens/{token_id}/overview", params=params)


def get_token_distribution(token_id: int, time_range: str = "5min") -> Dict:
    params = {"timeRange": time_range}
    return api_client._make_request(
        "GET", f"/tokens/{token_id}/distribution", params=params
    )


def get_account_detail(account_id: int) -> Dict:
    return api_client._make_request("GET", f"/accounts/{account_id}")


def get_token_pnl(token_id: int, time_range: str = "1min", top_limit: int = 50) -> Dict:
    params = {"timeRange": time_range, "topLimit": top_limit}
    return api_client._make_request("GET", f"/tokens/{token_id}/pnl", params=params)


class TokenListInput(BaseModel):
    page: int = Field(default=1, description="页码")
    page_size: int = Field(default=50, description="每页大小")
    sort_by: str = Field(default="mcap", description="排序字段: mcap/volume/holders/price")
    order: str = Field(default="desc", description="排序方向: asc/desc")


class TokenOverviewInput(BaseModel):
    token_id: int = Field(description="代币ID")
    time_range: str = Field(default="5min", description="时间窗口: 20s/1min/5min/1h")


class TokenDistributionInput(BaseModel):
    token_id: int = Field(description="代币ID")
    time_range: str = Field(default="5min", description="时间范围")


class AccountDetailInput(BaseModel):
    account_id: int = Field(description="账户ID")


class TokenPnLInput(BaseModel):
    token_id: int = Field(description="代币ID")
    time_range: str = Field(default="1min", description="时间范围")
    top_limit: int = Field(default=50, description="Top PnL排行榜数量限制")


class TokenListTool(BaseTool):
    name: str = "get_token_list"
    description: str = """获取代币列表，包含基础信息和关键指标。
    用于查看代币排行、筛选高成交量或高市值标的"""
    args_schema: type = TokenListInput

    def _run(self, page: int = 1, page_size: int = 50, sort_by: str = "mcap", order: str = "desc") -> str:
        result = get_token_list(page, page_size, sort_by, order)
        return json.dumps(result, ensure_ascii=False)


class TokenOverviewTool(BaseTool):
    name: str = "get_token_overview"
    description: str = """获取代币的完整概览信息，包含基础信息、宏观指标和交易流分析。"""
    args_schema: type = TokenOverviewInput

    def _run(self, token_id: int, time_range: str = "5min") -> str:
        result = get_token_overview(token_id, time_range)
        return json.dumps(result, ensure_ascii=False)


class TokenDistributionTool(BaseTool):
    name: str = "get_token_distribution"
    description: str = """获取代币的持有者分布分析，包括巨鲸、聪明钱等标签。"""
    args_schema: type = TokenDistributionInput

    def _run(self, token_id: int, time_range: str = "5min") -> str:
        result = get_token_distribution(token_id, time_range)
        return json.dumps(result, ensure_ascii=False)


class AccountDetailTool(BaseTool):
    name: str = "get_account_detail"
    description: str = """获取账户的详细信息，包含基础信息、资产持仓、转账历史等。"""
    args_schema: type = AccountDetailInput

    def _run(self, account_id: int) -> str:
        result = get_account_detail(account_id)
        return json.dumps(result, ensure_ascii=False)


class TokenPnLTool(BaseTool):
    name: str = "get_token_pnl"
    description: str = """获取代币的PnL分析数据，包含排行榜、宏观指标和汇总统计。"""
    args_schema: type = TokenPnLInput

    def _run(self, token_id: int, time_range: str = "1min", top_limit: int = 50) -> str:
        result = get_token_pnl(token_id, time_range, top_limit)
        return json.dumps(result, ensure_ascii=False)


TOKEN_TOOLS = [
    TokenListTool(),
    TokenOverviewTool(),
    TokenDistributionTool(),
    AccountDetailTool(),
    TokenPnLTool(),
]

__all__ = [
    "get_token_list",
    "get_token_overview",
    "get_token_distribution",
    "get_account_detail",
    "get_token_pnl",
    "TOKEN_TOOLS",
]

"""
API工具包装器
将后端NewAnalyticsController的API接口包装成LangChain工具
"""
import requests
import json
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import logging
from config import BACKEND_API_CONFIG

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# 工具列表
AVAILABLE_TOOLS = [
    HealthCheckTool(),
    TokenListTool(),
    TokenOverviewTool(), 
    TokenDistributionTool(),
    AccountDetailTool(),
    TokenPnLTool()
]

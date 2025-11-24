"""
系统相关工具（健康检查等）
"""
import json
from langchain_core.tools import BaseTool

from .base import api_client


def get_health_status():
    """获取API健康状态"""
    return api_client._make_request("GET", "/health")


class HealthCheckTool(BaseTool):
    """健康检查工具"""

    name: str = "health_check"
    description: str = "检查API服务健康状态"

    def _run(self) -> str:
        result = get_health_status()
        return json.dumps(result, ensure_ascii=False)


SYSTEM_TOOLS = [HealthCheckTool()]

__all__ = ["get_health_status", "HealthCheckTool", "SYSTEM_TOOLS"]

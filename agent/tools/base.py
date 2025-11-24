"""
公共API客户端与工具辅助函数
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

import requests

from config import BACKEND_API_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def to_csv(values: Optional[Any]) -> Optional[str]:
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
    return str(values)


def sanitize_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """去除参数中值为None的字段"""
    if not params:
        return None
    sanitized = {key: value for key, value in params.items() if value is not None}
    return sanitized or None


class APIConfig:
    """API配置"""
    BASE_URL = BACKEND_API_CONFIG["base_url"]
    TIMEOUT = BACKEND_API_CONFIG["timeout"]


class APIClient:
    """与后端交互的通用HTTP客户端"""

    def __init__(self):
        self.base_url = APIConfig.BASE_URL
        self.timeout = APIConfig.TIMEOUT

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        params = sanitize_params(params)
        data = sanitize_params(data)

        try:
            logger.info("发送%s请求: %s, params: %s", method, url, params)

            if method.upper() == "GET":
                response = requests.get(url, params=params, timeout=self.timeout)
            elif method.upper() == "POST":
                response = requests.post(url, json=data, timeout=self.timeout)
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")

            response.raise_for_status()
            result = response.json()

            logger.info("API请求成功，状态码: %s", response.status_code)
            return result

        except requests.exceptions.RequestException as exc:
            logger.error("API请求失败: %s", exc)
            return {"status": "error", "message": f"API请求失败: {exc}"}
        except json.JSONDecodeError as exc:
            logger.error("JSON解析失败: %s", exc)
            return {"status": "error", "message": f"响应解析失败: {exc}"}


# 单例客户端供工具复用
api_client = APIClient()

__all__ = ["api_client", "APIClient", "to_csv", "sanitize_params", "logger"]

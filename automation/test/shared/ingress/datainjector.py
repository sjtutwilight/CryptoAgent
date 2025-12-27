"""DataInjector 数据接入相关操作"""
from __future__ import annotations

import urllib.parse
from typing import Callable, Dict, List, Optional

from automation.test.probes import kafka_probe
from automation.test.shared.core.config import get_default_config
from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult, ProbeStatus
from automation.test.shared.core.scenario import ProbeCall
from automation.test.shared.core.stages import BaseStage
from automation.test.shared.infra.ops import (
    clickhouse_truncate,
    docker_exec_curl_post,
    ensure_container_curl,
    http_post_json,
)


# ==================== 便捷适配函数（供测试场景直接使用）====================

def apply_roles_by_ids(
    ctx: RunContext,
    role_ids: List[str],
    api: Optional[str] = None,
    container: str = "datainjector-worker",
    token: Optional[str] = None,
    skip_docker: bool = False,
) -> ProbeResult:
    """应用 roles（通过 role_id）
    
    这是测试场景的适配函数，内部调用 automation/ops/role_apply.py
    
    Args:
        ctx: 运行上下文
        role_ids: 要应用的 role_id 列表
        api: DataInjector API URL
        container: Docker 容器名
        token: 认证令牌
        skip_docker: 是否跳过 docker 方式
        
    Returns:
        ProbeResult
    """
    from automation.ops.role_apply import apply_roles_programmatic
    
    if not api and skip_docker:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail="datainjector api not set and docker disabled",
        )
    
    try:
        response = apply_roles_programmatic(
            role_ids=role_ids,
            api=api,
            container=container,
            token=token,
        )
        method = "http" if api else "docker"
    except Exception as exc:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail=f"apply roles failed: {exc}",
        )
    
    return ProbeResult(
        status=ProbeStatus.SUCCESS,
        detail=f"applied {len(role_ids)} roles via {method}",
        payload={"role_ids": role_ids, "response": response},
    )


def stop_roles_by_ids(
    ctx: RunContext,
    role_ids: List[str],
    api: Optional[str] = None,
    container: str = "datainjector-worker",
    token: Optional[str] = None,
    skip_docker: bool = False,
) -> ProbeResult:
    """停止 roles（通过 role_id）
    
    这是测试场景的适配函数，内部调用 automation/ops/role_stop.py
    
    Args:
        ctx: 运行上下文
        role_ids: 要停止的 role_id 列表
        api: DataInjector API URL
        container: Docker 容器名
        token: 认证令牌
        skip_docker: 是否跳过 docker 方式
        
    Returns:
        ProbeResult
    """
    from automation.ops.role_stop import stop_roles_programmatic
    
    if not role_ids:
        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail="no roles to stop",
        )
    
    if not api and skip_docker:
        return ProbeResult(
            status=ProbeStatus.SKIP,
            detail="docker disabled, skipping stop",
        )
    
    try:
        response = stop_roles_programmatic(
            role_ids=role_ids,
            api=api,
            container=container,
            token=token,
        )
        method = "http" if api else "docker"
    except Exception as exc:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail=f"stop roles failed: {exc}",
        )
    
    return ProbeResult(
        status=ProbeStatus.SUCCESS,
        detail=f"stopped {len(role_ids)} roles via {method}",
        payload={"role_ids": role_ids, "response": response},
    )


# ==================== 旧版 API（兼容性保留，不推荐使用）====================


def apply_roles_http(api_url: str, payload: Dict) -> Dict:
    """通过 HTTP API 应用 roles"""
    url = urllib.parse.urljoin(api_url, "/api/roles/apply")
    status, resp = http_post_json(url, payload)
    if status != 200:
        raise RuntimeError(f"apply role failed: HTTP {status}")
    return resp


def apply_roles_docker(container: str, payload: Dict) -> Dict:
    """通过 Docker 容器应用 roles"""
    ensure_container_curl(container)
    url = "http://localhost:8090/api/roles/apply"
    return docker_exec_curl_post(container, url, payload)


def stop_roles_http(api_url: str, role_ids: List[str]) -> Dict:
    """通过 HTTP API 停止 roles"""
    url = urllib.parse.urljoin(api_url, "/api/roles/stop")
    payload = {"role_ids": role_ids}
    status, resp = http_post_json(url, payload)
    if status != 200:
        raise RuntimeError(f"stop role failed: HTTP {status}")
    return resp


def stop_roles_docker(container: str, role_ids: List[str]) -> Dict:
    """通过 Docker 容器停止 roles"""
    url = "http://localhost:8090/api/roles/stop"
    payload = {"role_ids": role_ids}
    return docker_exec_curl_post(container, url, payload)


class DataInjectorIngressStage(BaseStage):
    """DataInjector 数据接入 Stage"""

    def __init__(
        self,
        name: str = "ingress",
        role_ids: Optional[List[str]] = None,
        role_builder: Optional[Callable] = None,
        role_builder_kwargs: Optional[Dict] = None,
        kafka_topics: Optional[List[str]] = None,
        cleanup_tables: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ):
        super().__init__(name, tags or ["layer:ingress"])
        # 新模式：直接使用 role_ids（推荐）
        self.role_ids = role_ids
        # 旧模式：使用 role_builder（兼容）
        self.role_builder = role_builder
        self.role_builder_kwargs = role_builder_kwargs or {}
        self.kafka_topics = kafka_topics or []
        self.cleanup_tables = cleanup_tables or []

    def build_probes(self) -> List[ProbeCall]:
        probes = [
            ProbeCall("ingress.prepare", self._probe_prepare),
            ProbeCall("ingress.apply_roles", self._probe_apply_roles),
        ]
        for topic in self.kafka_topics:
            probes.append(ProbeCall(f"ingress.kafka.{topic}", lambda c, t=topic: self._probe_kafka_topic(c, t)))
        return probes

    def _get_config(self, ctx: RunContext) -> Dict:
        cfg = get_default_config()
        cfg.update(ctx.metadata or {})
        return cfg

    def _probe_prepare(self, ctx: RunContext) -> ProbeResult:
        """准备工作：清理 ClickHouse 表"""
        cfg = self._get_config(ctx)
        try:
            if cfg.get("clean_clickhouse") and not cfg.get("skip_clean_clickhouse"):
                for table in self.cleanup_tables:
                    clickhouse_truncate(
                        cfg["clickhouse_http"], table, cfg["clickhouse_user"], cfg["clickhouse_password"]
                    )
        except Exception as exc:
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"prepare failed: {exc}")
        return ProbeResult(status=ProbeStatus.SUCCESS, detail="prepare ok")

    def _probe_apply_roles(self, ctx: RunContext) -> ProbeResult:
        """应用 DataInjector roles"""
        cfg = self._get_config(ctx)
        
        # 新模式：直接使用 role_ids（推荐）
        if self.role_ids:
            role_ids_to_apply = self.role_ids
            # 使用 apply_roles_programmatic
            from automation.ops.role_apply import apply_roles_programmatic
            try:
                if cfg.get("datainjector_api"):
                    response = apply_roles_programmatic(
                        role_ids=role_ids_to_apply,
                        api=cfg["datainjector_api"],
                        token=cfg.get("datainjector_token"),
                    )
                    method = "http"
                else:
                    if cfg.get("skip_datainjector_docker"):
                        raise RuntimeError("datainjector api not set and docker disabled")
                    response = apply_roles_programmatic(
                        role_ids=role_ids_to_apply,
                        container=cfg["datainjector_container"],
                        token=cfg.get("datainjector_token"),
                    )
                    method = "docker"
            except Exception as exc:
                return ProbeResult(status=ProbeStatus.FAIL, detail=f"apply roles failed: {exc}")
            
            # 保存 role_ids 到 state，供 cleanup 使用
            ctx.state["role_ids"] = role_ids_to_apply
            ctx.state.setdefault("cleanup_funcs", []).append(self._cleanup_roles)
            
            return ProbeResult(status=ProbeStatus.SUCCESS, detail=f"roles applied ({method})", payload=response)
        
        # 旧模式：使用 role_builder（兼容，但不推荐）
        if not self.role_builder:
            return ProbeResult(status=ProbeStatus.SKIP, detail="no role_ids or role_builder provided")
        
        # 构建 role payload
        kwargs = dict(self.role_builder_kwargs)
        kwargs.setdefault("kafka_broker", cfg["kafka_broker"])
        kwargs.setdefault("run_id", ctx.run_id)
        payload = self.role_builder(**kwargs)
        
        # 提取 role_ids
        role_ids = [role["role_id"] for role in payload.get("roles", [])]
        
        try:
            if cfg.get("datainjector_api"):
                response = apply_roles_http(cfg["datainjector_api"], payload)
                method = "http"
            else:
                if cfg.get("skip_datainjector_docker"):
                    raise RuntimeError("datainjector api not set and docker disabled")
                ensure_container_curl(cfg["datainjector_container"])
                response = apply_roles_docker(cfg["datainjector_container"], payload)
                method = "docker"
        except Exception as exc:
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"apply roles failed: {exc}")
        
        # 保存 role_ids 到 state，供 cleanup 使用
        ctx.state["role_ids"] = role_ids
        ctx.state.setdefault("cleanup_funcs", []).append(self._cleanup_roles)
        
        return ProbeResult(status=ProbeStatus.SUCCESS, detail=f"roles applied ({method})", payload=response)

    def _probe_kafka_topic(self, ctx: RunContext, topic: str) -> ProbeResult:
        """验证 Kafka topic 中有消息"""
        return kafka_probe.has_message_with_run_id(ctx, topic)

    def _cleanup_roles(self, ctx: RunContext) -> None:
        """清理 roles"""
        from automation.ops.role_stop import stop_roles_programmatic
        
        cfg = self._get_config(ctx)
        role_ids = ctx.state.get("role_ids", [])
        if not role_ids:
            return
        
        try:
            if cfg.get("datainjector_api"):
                stop_roles_programmatic(
                    role_ids=role_ids,
                    api=cfg["datainjector_api"],
                    token=cfg.get("datainjector_token"),
                )
            else:
                if not cfg.get("skip_datainjector_docker"):
                    stop_roles_programmatic(
                        role_ids=role_ids,
                        container=cfg["datainjector_container"],
                        token=cfg.get("datainjector_token"),
                    )
        except Exception as exc:
            # 清理失败不影响整体流程，只记录日志
            print(f"Warning: cleanup roles failed: {exc}")

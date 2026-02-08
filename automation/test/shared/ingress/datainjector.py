"""DataInjector 数据接入相关操作"""
from __future__ import annotations

from typing import Dict, List, Optional

from automation.test.probes import kafka_probe
from automation.test.shared.core.config import get_default_config
from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult, ProbeStatus
from automation.test.shared.core.scenario import ProbeCall
from automation.test.shared.core.stages import BaseStage
from automation.test.shared.infra.ops import clickhouse_truncate


# ==================== 便捷适配函数（供测试场景直接使用）====================

def apply_roles_by_ids(
    ctx: RunContext,
    role_ids: List[str],
    api: Optional[str] = None,
    container: str = "datainjector-worker",
    token: Optional[str] = None,
) -> ProbeResult:
    """应用 roles（通过 role_id）
    
    这是测试场景的适配函数，内部调用 automation/ops/role/start.py
    
    Args:
        ctx: 运行上下文
        role_ids: 要应用的 role_id 列表
        api: DataInjector API URL
        container: Docker 容器名
        token: 认证令牌
    Returns:
        ProbeResult
    """
    from automation.ops.role.start import apply_roles_programmatic

    try:
        response = apply_roles_programmatic(
            role_ids=role_ids,
            api=api,
            container=container,
            token=token,
        )
    except Exception as exc:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail=f"apply roles failed: {exc}",
        )
    
    return ProbeResult(
        status=ProbeStatus.SUCCESS,
        detail=f"applied {len(role_ids)} roles",
        payload={"role_ids": role_ids, "response": response},
    )


def stop_roles_by_ids(
    ctx: RunContext,
    role_ids: List[str],
    api: Optional[str] = None,
    container: str = "datainjector-worker",
    token: Optional[str] = None,
) -> ProbeResult:
    """停止 roles（通过 role_id）
    
    这是测试场景的适配函数，内部调用 automation/ops/role/stop.py
    
    Args:
        ctx: 运行上下文
        role_ids: 要停止的 role_id 列表
        api: DataInjector API URL
        container: Docker 容器名
        token: 认证令牌
    Returns:
        ProbeResult
    """
    from automation.ops.role.stop import stop_roles_programmatic
    
    if not role_ids:
        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail="no roles to stop",
        )
    
    try:
        response = stop_roles_programmatic(
            role_ids=role_ids,
            api=api,
            container=container,
            token=token,
        )
    except Exception as exc:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail=f"stop roles failed: {exc}",
        )
    
    return ProbeResult(
        status=ProbeStatus.SUCCESS,
        detail=f"stopped {len(role_ids)} roles",
        payload={"role_ids": role_ids, "response": response},
    )



class DataInjectorIngressStage(BaseStage):
    """DataInjector 数据接入 Stage"""

    def __init__(
        self,
        name: str = "ingress",
        role_ids: Optional[List[str]] = None,
        kafka_topics: Optional[List[str]] = None,
        cleanup_tables: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ):
        super().__init__(name, tags or ["layer:ingress"])
        self.role_ids = role_ids
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
        
        if not self.role_ids:
            return ProbeResult(status=ProbeStatus.SKIP, detail="no role_ids provided")

        role_ids_to_apply = self.role_ids
        from automation.ops.role.start import apply_roles_programmatic
        try:
            response = apply_roles_programmatic(
                role_ids=role_ids_to_apply,
                api=cfg.get("datainjector_api"),
                container=cfg.get("datainjector_container", "datainjector-worker"),
                token=cfg.get("datainjector_token"),
            )
        except Exception as exc:
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"apply roles failed: {exc}")

        ctx.state["role_ids"] = role_ids_to_apply
        ctx.state.setdefault("cleanup_funcs", []).append(self._cleanup_roles)
        return ProbeResult(status=ProbeStatus.SUCCESS, detail="roles applied", payload=response)

    def _probe_kafka_topic(self, ctx: RunContext, topic: str) -> ProbeResult:
        """验证 Kafka topic 中有消息"""
        return kafka_probe.has_message_with_run_id(ctx, topic)

    def _cleanup_roles(self, ctx: RunContext) -> None:
        """清理 roles"""
        from automation.ops.role.stop import stop_roles_programmatic
        
        cfg = self._get_config(ctx)
        role_ids = ctx.state.get("role_ids", [])
        if not role_ids:
            return
        
        try:
            stop_roles_programmatic(
                role_ids=role_ids,
                api=cfg.get("datainjector_api"),
                container=cfg.get("datainjector_container", "datainjector-worker"),
                token=cfg.get("datainjector_token"),
            )
        except Exception as exc:
            # 清理失败不影响整体流程，只记录日志
            print(f"Warning: cleanup roles failed: {exc}")

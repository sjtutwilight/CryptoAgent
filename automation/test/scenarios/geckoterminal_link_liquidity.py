from __future__ import annotations

import os
from typing import Dict, List

from automation.test.probes import file_probe
from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult, ProbeStatus
from automation.test.shared.core.scenario import ProbeCall, Scenario, Stage
from automation.test.shared.ingress import datainjector
from automation.test.shared.paths import resolve_container_path

# 使用 config.yaml 中定义的 role_id
DEFAULT_ETH_LINK = "0x514910771af9ca656af840dff83e8264ecf986ca"

# 根据环境变量决定使用哪些 role
def _get_role_ids() -> List[str]:
    """根据配置的地址决定使用哪些 role_id"""
    role_ids = [
        "geckoterminal-link-token-ethereum",
        "geckoterminal-link-pools-ethereum",
    ]
    # 如果配置了 base 和 arbitrum 地址，也可以添加对应的 role
    # 这里暂时只使用 ethereum
    return role_ids


def _default_config() -> Dict:
    return {
        "datainjector_api": None,
        "datainjector_container": os.getenv("DATAINJECTOR_CONTAINER", "datainjector-worker"),
        "polling_interval": 30,
        "output_root": "runtime/data/geckoterminal/liquidity",
        "wait_timeout": 60,
        "wait_interval": 5,
        "token_addresses": {
            "ethereum": DEFAULT_ETH_LINK,
            "base": os.getenv("LINK_BASE_ADDRESS", ""),
            "arbitrum": os.getenv("LINK_ARBITRUM_ADDRESS", ""),
        },
    }


def _get_config(ctx: RunContext) -> Dict:
    cfg = _default_config()
    cfg.update(ctx.metadata or {})
    return cfg


def _resolve_output_root(cfg: Dict) -> str:
    output_root = cfg["output_root"]
    if cfg.get("datainjector_api"):
        return output_root
    return resolve_container_path(output_root)


def _probe_apply_roles(ctx: RunContext, state: Dict) -> ProbeResult:
    """应用 GeckoTerminal roles"""
    cfg = _get_config(ctx)
    role_ids = _get_role_ids()
    
    result = datainjector.apply_roles_by_ids(
        ctx=ctx,
        role_ids=role_ids,
        api=cfg.get("datainjector_api"),
        container=cfg["datainjector_container"],
        token=cfg.get("datainjector_token"),
    )
    
    if result.status == ProbeStatus.SUCCESS:
        state["role_ids"] = role_ids
        # 根据 role_id 推断输出目录
        outputs = []
        output_root = _resolve_output_root(cfg)
        outputs.append({"network": "ethereum", "type": "token", "dir": f"{output_root}/ethereum/{DEFAULT_ETH_LINK}/token"})
        outputs.append({"network": "ethereum", "type": "pools", "dir": f"{output_root}/ethereum/{DEFAULT_ETH_LINK}/pools"})
        state["outputs"] = outputs
    
    return result


def _probe_verify_files(ctx: RunContext, state: Dict) -> ProbeResult:
    """验证文件输出"""
    cfg = _get_config(ctx)
    outputs = state.get("outputs", [])
    if not outputs:
        return ProbeResult(status=ProbeStatus.SKIP, detail="no outputs to verify")

    container = cfg["datainjector_container"]
    wait_timeout = int(cfg.get("wait_timeout", 60))
    wait_interval = int(cfg.get("wait_interval", 5))

    return file_probe.verify_outputs(
        ctx,
        outputs=outputs,
        container=container,
        wait_timeout=wait_timeout,
        wait_interval=wait_interval,
        pattern="*.json",
    )


def _probe_cleanup(ctx: RunContext, state: Dict) -> ProbeResult:
    """清理 roles"""
    cfg = _get_config(ctx)
    role_ids = state.get("role_ids", [])
    
    return datainjector.stop_roles_by_ids(
        ctx=ctx,
        role_ids=role_ids,
        api=cfg.get("datainjector_api"),
        container=cfg["datainjector_container"],
        token=cfg.get("datainjector_token"),
    )


def build_scenario() -> Scenario:
    """构建 GeckoTerminal Link Liquidity 测试场景"""
    state: Dict = {}
    
    return Scenario(
        name="geckoterminal_link_liquidity",
        tags=["type:integration", "module:file_sink"],
        stages=[
            Stage(
                name="ingress",
                probes=[
                    ProbeCall("ingress.apply_roles", lambda ctx: _probe_apply_roles(ctx, state)),
                    ProbeCall("ingress.verify_files", lambda ctx: _probe_verify_files(ctx, state)),
                ],
                tags=["layer:ingress"],
            ),
            Stage(
                name="cleanup",
                probes=[ProbeCall("cleanup.stop", lambda ctx: _probe_cleanup(ctx, state))],
                tags=["layer:cleanup"],
            ),
        ],
    )

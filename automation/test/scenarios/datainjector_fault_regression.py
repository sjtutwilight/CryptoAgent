from __future__ import annotations

import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

from automation.ops.role.start import apply_roles_programmatic
from automation.ops.role.stop import stop_roles_programmatic
from automation.test.probes import worker_probe
from automation.test.shared.core.config import get_default_config
from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult, ProbeStatus
from automation.test.shared.core.scenario import ProbeCall, Scenario
from automation.test.shared.core.stages import BaseStage


class DataInjectorFaultRegressionStage(BaseStage):
    def __init__(self, name: str = "fault_regression"):
        super().__init__(name, tags=["layer:fault-regression"])

    def build_probes(self) -> List[ProbeCall]:
        return [
            ProbeCall("prepare.worker_health", self._probe_worker_health),
            ProbeCall("prepare.role_scope", self._probe_prepare_scope),
            ProbeCall("prepare.apply_roles", self._probe_prepare_apply_roles),
            ProbeCall("prepare.mock_reset", self._probe_mock_reset),
            ProbeCall("inject.execute", self._probe_inject),
            ProbeCall("observe.window", self._probe_observe),
            ProbeCall("assert.rules", self._probe_assert),
            ProbeCall("report.artifacts", self._probe_report),
            ProbeCall("cleanup.restore", self._probe_cleanup),
        ]

    def _project_root(self) -> Path:
        return Path(__file__).resolve().parents[3]

    def _get_cfg(self, ctx: RunContext) -> Dict[str, Any]:
        cfg = get_default_config()
        cfg.update(ctx.metadata or {})
        return cfg

    def _parse_role_ids(self, raw: Any) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            return [item.strip() for item in raw.split(",") if item.strip()]
        if isinstance(raw, list):
            out: List[str] = []
            for item in raw:
                if item is None:
                    continue
                text = str(item).strip()
                if text:
                    out.append(text)
            return out
        return []

    def _resolve_config_path(self, raw_path: Any) -> Path:
        path = Path(str(raw_path or "datainjector/worker/configs/config.yaml"))
        if not path.is_absolute():
            path = self._project_root() / path
        return path

    def _http_post(self, url: str, timeout: int = 5) -> int:
        req = urllib.request.Request(url, data=b"{}", method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status)

    def _probe_worker_health(self, ctx: RunContext) -> ProbeResult:
        cfg = self._get_cfg(ctx)
        container = str(cfg.get("fault_worker_container") or cfg.get("datainjector_container") or "datainjector-worker")
        ctx.state["fault_worker_container"] = container
        return worker_probe.check_worker_health(ctx, container=container)

    def _probe_prepare_scope(self, ctx: RunContext) -> ProbeResult:
        cfg = self._get_cfg(ctx)
        role_ids = self._parse_role_ids(cfg.get("role_ids"))
        if not role_ids:
            return ProbeResult(status=ProbeStatus.FAIL, detail="role_ids is required (config-json)")

        fault_mode = str(cfg.get("fault_mode") or "mock").strip().lower()
        if fault_mode not in {"mock", "real"}:
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"unsupported fault_mode: {fault_mode}")

        fault_action = str(cfg.get("fault_action") or "role_restart").strip().lower()
        if fault_action not in {"role_restart", "container_pause", "noop"}:
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"unsupported fault_action: {fault_action}")

        pause_seconds = int(cfg.get("fault_pause_seconds") or 8)
        observe_seconds = int(cfg.get("observe_seconds") or 20)
        since_seconds = int(cfg.get("log_since_seconds") or (pause_seconds + observe_seconds + 90))

        ctx.state["fault_cfg"] = cfg
        ctx.state["role_ids"] = role_ids
        ctx.state["fault_mode"] = fault_mode
        ctx.state["fault_action"] = fault_action
        ctx.state["fault_pause_seconds"] = pause_seconds
        ctx.state["observe_seconds"] = observe_seconds
        ctx.state["log_since_seconds"] = since_seconds

        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail=f"scope prepared: mode={fault_mode}, action={fault_action}, roles={len(role_ids)}",
            payload={
                "role_ids": role_ids,
                "fault_mode": fault_mode,
                "fault_action": fault_action,
                "pause_seconds": pause_seconds,
                "observe_seconds": observe_seconds,
                "log_since_seconds": since_seconds,
            },
        )

    def _probe_prepare_apply_roles(self, ctx: RunContext) -> ProbeResult:
        cfg = ctx.state.get("fault_cfg") or {}
        role_ids = ctx.state.get("role_ids") or []
        apply_before = bool(cfg.get("apply_roles_before_inject", True))
        if not apply_before:
            return ProbeResult(status=ProbeStatus.SKIP, detail="apply_roles_before_inject=false")

        try:
            response = apply_roles_programmatic(
                role_ids=role_ids,
                config_path=self._resolve_config_path(cfg.get("role_config_yaml")),
                api=cfg.get("datainjector_api"),
                container=cfg.get("datainjector_container", "datainjector-worker"),
                token=cfg.get("datainjector_token"),
            )
        except Exception as exc:
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"apply roles failed: {exc}")

        ctx.state["roles_applied_in_prepare"] = True
        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail=f"roles applied in prepare: {len(role_ids)}",
            payload={"response": response},
        )

    def _probe_mock_reset(self, ctx: RunContext) -> ProbeResult:
        mode = ctx.state.get("fault_mode")
        if mode != "mock":
            return ProbeResult(status=ProbeStatus.SKIP, detail="mock reset skipped (mode != mock)")

        cfg = ctx.state.get("fault_cfg") or {}
        mock_base = str(cfg.get("mock_provider_base_url") or "http://localhost:8090")
        require_mock_provider = bool(cfg.get("require_mock_provider", False))
        reset_url = f"{mock_base.rstrip('/')}/fault/reset"
        try:
            status = self._http_post(reset_url, timeout=5)
        except urllib.error.URLError as exc:
            if require_mock_provider:
                return ProbeResult(status=ProbeStatus.FAIL, detail=f"mock reset failed: {exc}")
            return ProbeResult(
                status=ProbeStatus.SKIP,
                detail=f"mock provider unavailable, skip reset: {exc}",
                payload={"mock_provider_base_url": mock_base},
            )

        if status < 200 or status >= 300:
            if require_mock_provider:
                return ProbeResult(status=ProbeStatus.FAIL, detail=f"mock reset http {status}")
            return ProbeResult(
                status=ProbeStatus.SKIP,
                detail=f"mock reset http {status}, skip by default",
                payload={"mock_provider_base_url": mock_base},
            )

        return ProbeResult(status=ProbeStatus.SUCCESS, detail="mock fault stats reset")

    def _probe_inject(self, ctx: RunContext) -> ProbeResult:
        mode = ctx.state.get("fault_mode")
        action = ctx.state.get("fault_action")
        cfg = ctx.state.get("fault_cfg") or {}
        role_ids = ctx.state.get("role_ids") or []
        pause_seconds = int(ctx.state.get("fault_pause_seconds") or 8)

        if mode == "mock":
            # mock 模式下故障由 mockDataProvider 配置自动触发，此处只负责标记注入阶段开始。
            ctx.state["inject_started_at"] = int(time.time())
            return ProbeResult(status=ProbeStatus.SUCCESS, detail="mock injection relies on provider config")

        if action == "noop":
            ctx.state["inject_started_at"] = int(time.time())
            return ProbeResult(status=ProbeStatus.SUCCESS, detail="real injection noop")

        if action == "container_pause":
            target = str(cfg.get("fault_target_container") or "")
            if not target:
                return ProbeResult(status=ProbeStatus.FAIL, detail="fault_target_container is required for container_pause")
            try:
                subprocess.run(["docker", "pause", target], check=True, capture_output=True, text=True)
                ctx.state["paused_container"] = target
                time.sleep(pause_seconds)
                subprocess.run(["docker", "unpause", target], check=True, capture_output=True, text=True)
                ctx.state["paused_container"] = None
            except subprocess.CalledProcessError as exc:
                return ProbeResult(status=ProbeStatus.FAIL, detail=f"container_pause failed: {exc.stderr or exc.stdout}")

            ctx.state["inject_started_at"] = int(time.time())
            return ProbeResult(status=ProbeStatus.SUCCESS, detail=f"container paused/unpaused: {target}")

        # role_restart: 仅影响指定 role
        try:
            stop_resp = stop_roles_programmatic(
                role_ids=role_ids,
                api=cfg.get("datainjector_api"),
                container=cfg.get("datainjector_container", "datainjector-worker"),
                token=cfg.get("datainjector_token"),
            )
            time.sleep(pause_seconds)
            apply_resp = apply_roles_programmatic(
                role_ids=role_ids,
                config_path=self._resolve_config_path(cfg.get("role_config_yaml")),
                api=cfg.get("datainjector_api"),
                container=cfg.get("datainjector_container", "datainjector-worker"),
                token=cfg.get("datainjector_token"),
            )
        except Exception as exc:
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"role_restart injection failed: {exc}")

        ctx.state["inject_started_at"] = int(time.time())
        return ProbeResult(
            status=ProbeStatus.SUCCESS,
            detail="role restart injection completed",
            payload={"stop_response": stop_resp, "apply_response": apply_resp},
        )

    def _probe_observe(self, ctx: RunContext) -> ProbeResult:
        observe_seconds = int(ctx.state.get("observe_seconds") or 20)
        time.sleep(observe_seconds)
        return ProbeResult(status=ProbeStatus.SUCCESS, detail=f"observed {observe_seconds}s")

    def _probe_assert(self, ctx: RunContext) -> ProbeResult:
        cfg = ctx.state.get("fault_cfg") or {}
        role_ids = ctx.state.get("role_ids") or []
        container = ctx.state.get("fault_worker_container") or "datainjector-worker"
        since_seconds = int(ctx.state.get("log_since_seconds") or 120)
        tail_lines = int(cfg.get("fault_log_tail_lines") or 5000)
        fault_case = str(cfg.get("fault_case") or "disconnect_reconnect").strip().lower()
        fault_action = str(ctx.state.get("fault_action") or cfg.get("fault_action") or "").strip().lower()
        expect_backfill = bool(cfg.get("expect_backfill", fault_case in {"data_loss", "backfill"}))
        rules = dict(cfg.get("fault_rules") or {})

        # role_restart 属于生命周期注入，不一定经过 ws.reconnect 路径；避免将其误判为重连失败。
        if fault_action == "role_restart":
            required_events = list(rules.get("required_events") or worker_probe.DEFAULT_FAULT_RULES["required_events"])
            required_events = [x for x in required_events if x not in {"ws.reconnect.start", "ws.reconnect.success"}]
            rules["required_events"] = required_events

        result = worker_probe.check_fault_regression(
            ctx=ctx,
            role_ids=role_ids,
            container=container,
            since_seconds=since_seconds,
            tail_lines=tail_lines,
            rules=rules,
            expect_backfill=expect_backfill,
        )

        payload = result.payload or {}
        ctx.state["fault_summary"] = payload.get("summary")
        ctx.state["fault_evidence"] = payload.get("evidence")
        return result

    def _probe_report(self, ctx: RunContext) -> ProbeResult:
        summary = ctx.state.get("fault_summary")
        evidence = ctx.state.get("fault_evidence")
        if not isinstance(summary, dict):
            return ProbeResult(status=ProbeStatus.SKIP, detail="fault summary missing")
        if not isinstance(evidence, list):
            evidence = []
        return worker_probe.write_fault_regression_artifacts(ctx, summary=summary, evidence=evidence)

    def _probe_cleanup(self, ctx: RunContext) -> ProbeResult:
        cfg = ctx.state.get("fault_cfg") or {}
        paused = ctx.state.get("paused_container")
        cleanup_errors: List[str] = []

        if paused:
            try:
                subprocess.run(["docker", "unpause", str(paused)], check=True, capture_output=True, text=True)
                ctx.state["paused_container"] = None
            except subprocess.CalledProcessError as exc:
                cleanup_errors.append(f"cleanup unpause failed: {exc.stderr or exc.stdout}")

        cleanup_stop_roles = bool(cfg.get("cleanup_stop_roles", False))
        if cleanup_stop_roles and ctx.state.get("roles_applied_in_prepare"):
            role_ids = ctx.state.get("role_ids") or []
            try:
                stop_roles_programmatic(
                    role_ids=role_ids,
                    api=cfg.get("datainjector_api"),
                    container=cfg.get("datainjector_container", "datainjector-worker"),
                    token=cfg.get("datainjector_token"),
                )
            except Exception as exc:
                cleanup_errors.append(f"cleanup stop roles failed: {exc}")

        if cleanup_errors:
            return ProbeResult(status=ProbeStatus.FAIL, detail="; ".join(cleanup_errors))
        return ProbeResult(status=ProbeStatus.SUCCESS, detail="cleanup ok")


def build_scenario() -> Scenario:
    return Scenario(
        name="datainjector_fault_regression",
        tags=["type:regression", "module:datainjector", "module:fault"],
        stages=[
            DataInjectorFaultRegressionStage().to_stage(),
        ],
    )

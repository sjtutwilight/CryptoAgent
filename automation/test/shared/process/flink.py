"""Flink 数据处理相关操作"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from automation.ops.flink.build import build_jar
from automation.ops.flink.cancel import cancel_job
from automation.ops.flink.list import list_jars
from automation.ops.flink.run import run_jar
from automation.ops.flink.status import job_status
from automation.ops.flink.upload import upload_jar
from automation.test.probes import db_probe
from automation.test.shared.core.config import get_default_config
from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult, ProbeStatus
from automation.test.shared.core.scenario import ProbeCall
from automation.test.shared.core.stages import BaseStage
from automation.test.shared.infra.ops import http_request


def repo_root() -> Path:
    """获取仓库根目录"""
    from automation.test.shared.repo_utils import repo_root as _repo_root
    return _repo_root()


def flink_wait_ready(rest_url: str, timeout_sec: int, interval_sec: int) -> Tuple[bool, Dict]:
    """等待 Flink 就绪"""
    start = time.time()
    last_error = None
    attempts = 0

    while time.time() - start < timeout_sec:
        attempts += 1
        try:
            status, body = http_request(urllib.parse.urljoin(rest_url, "/overview"), timeout=5)
            if status == 200:
                overview = json.loads(body.decode("utf-8"))
                return True, {
                    "attempts": attempts,
                    "elapsed_sec": round(time.time() - start, 2),
                    "taskmanagers": overview.get("taskmanagers", 0),
                    "slots_total": overview.get("slots-total", 0),
                }
            last_error = f"HTTP {status}"
        except urllib.error.URLError as e:
            last_error = f"URLError: {e.reason}"
        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)}"
        time.sleep(interval_sec)

    return False, {
        "attempts": attempts,
        "elapsed_sec": round(time.time() - start, 2),
        "last_error": last_error,
        "timeout_sec": timeout_sec,
    }


class FlinkProcessStage(BaseStage):
    """Flink 数据处理 Stage"""

    def __init__(
        self,
        name: str = "process",
        entry_classes: Optional[List[str]] = None,
        entry_class: Optional[str] = None,
        verify_tables: Optional[List[str]] = None,
        jar_path: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ):
        super().__init__(name, tags or ["layer:process"])
        # 支持单个或多个 entry class
        if entry_class:
            self.entry_classes = [entry_class]
        elif entry_classes:
            self.entry_classes = entry_classes
        else:
            self.entry_classes = []
        self.verify_tables = verify_tables or []
        self.jar_path = jar_path

    def build_probes(self) -> List[ProbeCall]:
        probes = [
            ProbeCall("process.prepare", self._probe_prepare),
            ProbeCall("process.submit_jobs", self._probe_submit_jobs),
            ProbeCall("process.job_status", self._probe_job_status),
        ]
        # 添加验证 probe
        for table in self.verify_tables:
            probes.append(ProbeCall(f"process.verify.{table}", lambda c, t=table: self._probe_verify_table(c, t)))
        return probes

    def _get_config(self, ctx: RunContext) -> Dict:
        cfg = get_default_config()
        cfg.update(ctx.metadata or {})
        if self.jar_path:
            cfg["jar_path"] = self.jar_path
        return cfg

    def _probe_prepare(self, ctx: RunContext) -> ProbeResult:
        """准备工作：构建 JAR（如果需要）"""
        cfg = self._get_config(ctx)
        try:
            if cfg.get("build_jar"):
                build_jar(repo_root())
        except Exception as exc:
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"prepare failed: {exc}")
        return ProbeResult(status=ProbeStatus.SUCCESS, detail="prepare ok")

    def _probe_submit_jobs(self, ctx: RunContext) -> ProbeResult:
        """提交 Flink 作业"""
        cfg = self._get_config(ctx)
        jar_path = Path(cfg["jar_path"])
        if not jar_path.is_absolute():
            jar_path = repo_root() / jar_path
        if not jar_path.exists():
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"jar not found: {jar_path}")

        # 等待 Flink 就绪
        flink_ready, flink_diag = flink_wait_ready(cfg["flink_rest"], int(cfg["flink_wait_timeout"]), 2)
        if not flink_ready:
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"flink rest not ready: {flink_diag.get('last_error')}")
        job_ids: Dict[str, str] = {}

        # 提交所有作业
        for entry_class in self.entry_classes:
            try:
                upload_jar(cfg["flink_rest"], jar_path)
                jars = list_jars(cfg["flink_rest"])
                jar_name = jar_path.name
                jar_id = None
                for jar in jars.get("files", []):
                    if jar.get("name") == jar_name:
                        jar_id = jar.get("id")
                if jar_id is None:
                    raise RuntimeError(f"jar id not found for {jar_name}")
                run_resp = run_jar(cfg["flink_rest"], jar_id, entry_class)
                job_ids[entry_class] = run_resp.get("jobid")
            except Exception as exc:
                return ProbeResult(status=ProbeStatus.FAIL, detail=f"submit failed ({entry_class}): {exc}")

        # 保存 job_ids 到 state
        ctx.state["job_ids"] = job_ids
        ctx.state.setdefault("cleanup_funcs", []).append(self._cleanup_jobs)

        return ProbeResult(status=ProbeStatus.SUCCESS, detail="jobs submitted", payload=job_ids)

    def _probe_job_status(self, ctx: RunContext) -> ProbeResult:
        """查询作业状态"""
        cfg = self._get_config(ctx)
        job_ids = ctx.state.get("job_ids", {})
        if not job_ids:
            return ProbeResult(status=ProbeStatus.SKIP, detail="no job_ids")

        payload = {}
        for entry_class, job_id in job_ids.items():
            try:
                payload[entry_class] = job_status(cfg["flink_rest"], job_id)
            except Exception as exc:
                return ProbeResult(status=ProbeStatus.FAIL, detail=f"job status failed ({entry_class}): {exc}")
        return ProbeResult(status=ProbeStatus.SUCCESS, detail="job status ok", payload=payload)

    def _probe_verify_table(self, ctx: RunContext, table: str) -> ProbeResult:
        """验证处理结果"""
        return db_probe.result_exists(ctx, table, min_rows=1)

    def _cleanup_jobs(self, ctx: RunContext) -> None:
        """清理 Flink 作业"""
        cfg = self._get_config(ctx)
        if not cfg.get("cancel_job") or cfg.get("keep_job"):
            return

        job_ids = ctx.state.get("job_ids", {})
        for entry_class, job_id in job_ids.items():
            try:
                cancel_job(cfg["flink_rest"], job_id)
            except Exception:
                pass  # 忽略清理错误

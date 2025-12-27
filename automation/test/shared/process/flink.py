"""Flink 数据处理相关操作"""
from __future__ import annotations

import json
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from automation.test.probes import db_probe
from automation.test.shared.core.config import get_default_config
from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult, ProbeStatus
from automation.test.shared.core.scenario import ProbeCall
from automation.test.shared.core.stages import BaseStage
from automation.test.shared.infra.build import build_aggregator_jar
from automation.test.shared.infra.ops import http_request


def repo_root() -> Path:
    """获取仓库根目录"""
    from automation.test.shared.repo_utils import repo_root as _repo_root
    return _repo_root()


def multipart_body(file_field: str, file_path: Path) -> Tuple[bytes, str]:
    """构建 multipart/form-data 请求体"""
    boundary = f"----e2e-{int(time.time() * 1000)}"
    file_bytes = file_path.read_bytes()
    filename = file_path.name
    parts = []
    parts.append(
        f"--{boundary}\r\n"
        f"Content-Disposition: form-data; name=\"{file_field}\"; filename=\"{filename}\"\r\n"
        "Content-Type: application/java-archive\r\n\r\n"
    )
    body = "".join(parts).encode("utf-8") + file_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")
    content_type = f"multipart/form-data; boundary={boundary}"
    return body, content_type


def flink_upload_jar(rest_url: str, jar_path: Path) -> Dict:
    """上传 JAR 到 Flink"""
    body, content_type = multipart_body("jarfile", jar_path)
    url = urllib.parse.urljoin(rest_url, "/jars/upload")
    status, resp = http_request(url, method="POST", data=body, headers={"Content-Type": content_type}, timeout=300)
    if status != 200:
        raise RuntimeError(f"flink upload failed: HTTP {status}")
    return json.loads(resp.decode("utf-8"))


def flink_list_jars(rest_url: str) -> Dict:
    """列出 Flink 中的 JAR"""
    url = urllib.parse.urljoin(rest_url, "/jars")
    status, resp = http_request(url, timeout=30)
    if status != 200:
        raise RuntimeError(f"flink list jars failed: HTTP {status}")
    return json.loads(resp.decode("utf-8"))


def flink_run_jar(rest_url: str, jar_id: str, entry_class: str) -> Dict:
    """运行 Flink JAR"""
    url = urllib.parse.urljoin(rest_url, f"/jars/{jar_id}/run")
    payload = json.dumps({"entryClass": entry_class}).encode("utf-8")
    status, resp = http_request(
        url, method="POST", data=payload, headers={"Content-Type": "application/json"}, timeout=30
    )
    if status != 200:
        raise RuntimeError(f"flink run jar failed: HTTP {status}")
    return json.loads(resp.decode("utf-8"))


def flink_job_status(rest_url: str, job_id: str) -> Dict:
    """查询 Flink 作业状态"""
    url = urllib.parse.urljoin(rest_url, f"/jobs/{job_id}")
    status, resp = http_request(url, timeout=30)
    if status != 200:
        raise RuntimeError(f"flink job status failed: HTTP {status}")
    return json.loads(resp.decode("utf-8"))


def flink_cancel_job(rest_url: str, job_id: str) -> None:
    """取消 Flink 作业"""
    url = urllib.parse.urljoin(rest_url, f"/jobs/{job_id}/cancel")
    status, _ = http_request(url, method="PATCH", timeout=30)
    if status != 202:
        raise RuntimeError(f"flink cancel failed: HTTP {status}")


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


def flink_run_via_docker(container: str, jar_path: Path, entry_class: str) -> str:
    """通过 Docker 运行 Flink 作业"""
    jar_name = f"e2e-{int(time.time())}-{jar_path.name}"
    remote_path = f"/tmp/{jar_name}"
    
    # 复制 JAR 到容器
    cp = subprocess.run(["docker", "cp", str(jar_path), f"{container}:{remote_path}"], capture_output=True)
    if cp.returncode != 0:
        raise RuntimeError(f"docker cp failed: {cp.stderr.decode('utf-8', errors='ignore')}")
    
    # 运行 Flink 作业
    cmd = ["docker", "exec", container, "/opt/flink/bin/flink", "run", "-d", "-c", entry_class, remote_path]
    proc = subprocess.run(cmd, capture_output=True)
    output = (proc.stdout + proc.stderr).decode("utf-8", errors="ignore")
    if proc.returncode != 0:
        raise RuntimeError(f"flink run via docker failed: {output}")
    
    # 提取 job ID
    match = re.search(r"JobID\s*[: ]?\s*([0-9a-f]{32})", output, re.IGNORECASE)
    if match:
        return match.group(1)
    raise RuntimeError(f"unable to parse job id from output: {output}")


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
                build_aggregator_jar(repo_root())
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
        if not flink_ready and cfg.get("require_flink_rest"):
            return ProbeResult(status=ProbeStatus.FAIL, detail=f"flink rest not ready: {flink_diag.get('last_error')}")

        use_rest = flink_ready
        job_ids: Dict[str, str] = {}

        # 提交所有作业
        for entry_class in self.entry_classes:
            try:
                if use_rest:
                    flink_upload_jar(cfg["flink_rest"], jar_path)
                    jars = flink_list_jars(cfg["flink_rest"])
                    jar_name = jar_path.name
                    jar_id = None
                    for jar in jars.get("files", []):
                        if jar.get("name") == jar_name:
                            jar_id = jar.get("id")
                    if jar_id is None:
                        raise RuntimeError(f"jar id not found for {jar_name}")
                    run_resp = flink_run_jar(cfg["flink_rest"], jar_id, entry_class)
                    job_ids[entry_class] = run_resp.get("jobid")
                else:
                    if cfg.get("skip_flink_docker"):
                        raise RuntimeError("flink rest unavailable and docker fallback disabled")
                    job_ids[entry_class] = flink_run_via_docker(cfg["flink_container"], jar_path, entry_class)
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
                payload[entry_class] = flink_job_status(cfg["flink_rest"], job_id)
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
                flink_cancel_job(cfg["flink_rest"], job_id)
            except Exception:
                pass  # 忽略清理错误

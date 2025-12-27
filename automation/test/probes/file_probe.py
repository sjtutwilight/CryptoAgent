"""
File output probes for DataInjector scenarios.
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult, ProbeStatus
from automation.test.shared.paths import resolve_container_path, resolve_host_path


def _latest_mtime_host(output_dir: Path, pattern: str) -> Optional[Tuple[float, str]]:
    if not output_dir.exists():
        return None
    files = sorted(output_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    latest = files[0]
    return latest.stat().st_mtime, str(latest)


def _latest_mtime_container(container: str, output_dir: str, pattern: str) -> Optional[Tuple[float, str]]:
    list_cmd = [
        "docker",
        "exec",
        container,
        "sh",
        "-c",
        f"ls -t {output_dir}/{pattern} 2>/dev/null | head -n 1",
    ]
    proc = subprocess.run(list_cmd, capture_output=True, text=True)
    latest = proc.stdout.strip()
    if proc.returncode != 0 or not latest:
        return None

    stat_cmd = ["docker", "exec", container, "sh", "-c", f"stat -c %Y {latest}"]
    stat_proc = subprocess.run(stat_cmd, capture_output=True, text=True)
    if stat_proc.returncode != 0 or not stat_proc.stdout.strip():
        return None
    try:
        mtime = float(stat_proc.stdout.strip())
    except ValueError:
        return None
    return mtime, latest


def _count_files_host(output_dir: Path, pattern: str) -> int:
    if not output_dir.exists():
        return 0
    return len(list(output_dir.glob(pattern)))


def _count_files_container(container: str, output_dir: str, pattern: str) -> int:
    cmd = [
        "docker",
        "exec",
        container,
        "sh",
        "-c",
        f"ls -1 {output_dir}/{pattern} 2>/dev/null | wc -l",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return 0
    try:
        return int(proc.stdout.strip() or "0")
    except ValueError:
        return 0


def verify_new_file(
    ctx: RunContext,
    output_dir: str,
    container: Optional[str],
    started_at: Optional[float],
    wait_timeout: int = 120,
    wait_interval: int = 5,
    pattern: str = "*.json",
    prefer_host: bool = True,
) -> ProbeResult:
    host_dir = resolve_host_path(output_dir)
    container_dir = resolve_container_path(output_dir)

    start = time.time()
    last_host = None
    last_container = None
    if started_at is None:
        started_at = 0.0

    while time.time() - start < wait_timeout:
        if prefer_host:
            host_hit = _latest_mtime_host(host_dir, pattern)
            if host_hit:
                last_host = host_hit
                if host_hit[0] >= started_at:
                    return ProbeResult(
                        status=ProbeStatus.SUCCESS,
                        detail="file generated (host)",
                        metrics={"elapsed_sec": int(time.time() - start)},
                        payload={"path": host_hit[1], "mtime": host_hit[0]},
                    )

        if container:
            container_hit = _latest_mtime_container(container, container_dir, pattern)
            if container_hit:
                last_container = container_hit
                if container_hit[0] >= started_at:
                    return ProbeResult(
                        status=ProbeStatus.SUCCESS,
                        detail="file generated (container)",
                        metrics={"elapsed_sec": int(time.time() - start)},
                        payload={"path": container_hit[1], "mtime": container_hit[0]},
                    )

        if not prefer_host:
            host_hit = _latest_mtime_host(host_dir, pattern)
            if host_hit:
                last_host = host_hit
                if host_hit[0] >= started_at:
                    return ProbeResult(
                        status=ProbeStatus.SUCCESS,
                        detail="file generated (host)",
                        metrics={"elapsed_sec": int(time.time() - start)},
                        payload={"path": host_hit[1], "mtime": host_hit[0]},
                    )

        time.sleep(wait_interval)

    payload = {
        "host_latest": last_host[1] if last_host else None,
        "host_latest_mtime": last_host[0] if last_host else None,
        "container_latest": last_container[1] if last_container else None,
        "container_latest_mtime": last_container[0] if last_container else None,
        "host_dir": str(host_dir),
        "container_dir": container_dir,
        "started_at": started_at,
    }
    return ProbeResult(
        status=ProbeStatus.FAIL,
        detail=f"no new files after {int(time.time() - start)}s",
        metrics={"elapsed_sec": int(time.time() - start)},
        payload=payload,
    )


def verify_outputs(
    ctx: RunContext,
    outputs: Iterable[Dict[str, Any]],
    container: Optional[str],
    wait_timeout: int = 60,
    wait_interval: int = 5,
    pattern: str = "*.json",
) -> ProbeResult:
    start_time = time.time()
    pending_outputs = list(outputs)
    total_count = len(pending_outputs)
    verified_outputs: List[Dict[str, Any]] = []

    def output_label(output: Dict[str, Any]) -> str:
        if output.get("label"):
            return str(output["label"])
        if output.get("network") and output.get("type"):
            return f"{output['network']}/{output['type']}"
        return str(output.get("dir") or output.get("output_dir") or "unknown")

    while time.time() - start_time < wait_timeout and pending_outputs:
        for output in pending_outputs[:]:
            output_dir = output.get("dir") or output.get("output_dir")
            if not output_dir:
                pending_outputs.remove(output)
                continue
            location = output.get("location")
            if location == "host":
                file_count = _count_files_host(resolve_host_path(output_dir), pattern)
            else:
                if container:
                    container_dir = resolve_container_path(output_dir)
                    file_count = _count_files_container(container, container_dir, pattern)
                else:
                    file_count = _count_files_host(resolve_host_path(output_dir), pattern)

            if file_count > 0:
                verified_outputs.append(
                    {
                        "label": output_label(output),
                        "file_count": file_count,
                        "dir": output_dir,
                    }
                )
                pending_outputs.remove(output)

        if pending_outputs:
            time.sleep(wait_interval)

    verified_count = len(verified_outputs)
    if verified_count == 0:
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail=f"no files generated after {int(time.time() - start_time)}s",
            metrics={"verified": 0, "total": total_count, "elapsed_sec": int(time.time() - start_time)},
        )

    if verified_count < total_count:
        missing = [output_label(o) for o in pending_outputs]
        return ProbeResult(
            status=ProbeStatus.FAIL,
            detail=f"verified {verified_count}/{total_count}, missing: {', '.join(missing)}",
            metrics={"verified": verified_count, "total": total_count, "elapsed_sec": int(time.time() - start_time)},
            payload={"verified": verified_outputs, "missing": missing},
        )

    return ProbeResult(
        status=ProbeStatus.SUCCESS,
        detail=f"verified {verified_count} outputs with data files",
        metrics={"verified": verified_count, "total": total_count, "elapsed_sec": int(time.time() - start_time)},
        payload={"verified": verified_outputs},
    )

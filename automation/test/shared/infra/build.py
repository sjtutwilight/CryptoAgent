"""构建相关操作"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def build_aggregator_jar(repo_root_path: Path) -> None:
    """构建 aggregator JAR 包"""
    if shutil.which("mvn") is None:
        raise RuntimeError("mvn not found in PATH")
    
    cmd = [
        "mvn",
        "-f",
        str(repo_root_path / "process/aggregator/pom.xml"),
        "-DskipTests",
        "package",
    ]
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        output = (proc.stdout + proc.stderr).decode("utf-8", errors="ignore")
        raise RuntimeError(f"mvn build failed: {output}")


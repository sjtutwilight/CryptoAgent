#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Flink aggregator JAR")
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[3]),
        help="repo root path",
    )
    parser.add_argument("--output-json", action="store_true", help="print JSON output")
    return parser.parse_args()


def build_jar(repo_root: Path) -> None:
    if shutil.which("mvn") is None:
        raise RuntimeError("mvn not found in PATH")
    cmd = [
        "mvn",
        "-f",
        str(repo_root / "process/aggregator/pom.xml"),
        "-DskipTests",
        "package",
    ]
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        output = (proc.stdout + proc.stderr).decode("utf-8", errors="ignore")
        raise RuntimeError(f"mvn build failed: {output}")


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root)
    build_jar(repo_root)
    if args.output_json:
        print(json.dumps({"status": "ok"}, ensure_ascii=True))
    else:
        print("build ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=True))
        sys.exit(1)

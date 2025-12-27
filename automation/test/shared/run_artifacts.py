from __future__ import annotations

import json
from pathlib import Path
from typing import Dict


def write_run_meta(run_dir: Path, run_id: str, created_at: str) -> None:
    meta = {
        "run_id": run_id,
        "created_at": created_at,
    }
    with (run_dir / "run_meta.json").open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=True, indent=2)
        f.write("\n")


def build_report(run_dir: Path, report: Dict) -> None:
    with (run_dir / "e2e_report.json").open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=True, indent=2)
        f.write("\n")

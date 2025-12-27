from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List


def read_probe_events(run_dir: Path) -> List[Dict[str, Any]]:
    path = run_dir / "probe_events.jsonl"
    if not path.exists():
        return []
    events = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def build_summary(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    status = "passed"
    counts = defaultdict(int)
    failures = []

    for ev in events:
        result = ev.get("result") or {}
        status_val = result.get("status")
        counts[status_val] += 1
        if status_val == "FAIL":
            status = "failed"
            failures.append({
                "stage": ev.get("stage"),
                "probe": ev.get("probe"),
                "detail": result.get("detail"),
            })
        elif status_val == "SKIP" and status != "failed":
            status = "partial"

    summary = {
        "status": status,
        "counts": dict(counts),
        "failures": failures,
    }
    if events:
        summary["run_id"] = events[0].get("run_id")
        summary["scenario"] = events[0].get("scenario")
    return summary


def build_timeline(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    timeline = []
    for ev in events:
        timeline.append({
            "stage": ev.get("stage"),
            "probe": ev.get("probe"),
            "result": (ev.get("result") or {}).get("status"),
            "detail": (ev.get("result") or {}).get("detail"),
        })
    return timeline


def write_summary(run_dir: Path) -> None:
    events = read_probe_events(run_dir)
    summary = build_summary(events)
    with (run_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=True, indent=2)
        f.write("\n")

    timeline = build_timeline(events)
    with (run_dir / "timeline.json").open("w", encoding="utf-8") as f:
        json.dump(timeline, f, ensure_ascii=True, indent=2)
        f.write("\n")

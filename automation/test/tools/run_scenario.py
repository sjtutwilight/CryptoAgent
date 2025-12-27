#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from automation.test.shared.run_id import new_run_id  # noqa: E402
from automation.test.scenarios import (  # noqa: E402
    binance_kline,
    binance_perp,
    binance_spot_link_kline_batch,
    geckoterminal_link_liquidity,
    hyperliquid_perp,
    spark_token_holders,
)
from automation.test.shared.config_loader import load_env_file  # noqa: E402
from automation.test.shared.core.context import RunContext  # noqa: E402
from automation.test.shared.core.result import ProbeResult, ProbeStatus  # noqa: E402
from automation.test.shared.logging import log_probe_result  # noqa: E402
from automation.test.shared.run_summary import write_summary  # noqa: E402


def load_infra_env(env_file: Optional[str]) -> None:
    default_env = REPO_ROOT / "config/infrastructure/env/docker.env"
    env_path = Path(env_file or os.getenv("INFRA_ENV_FILE", str(default_env)))
    if not env_path.exists():
        return
    for key, value in load_env_file(env_path).items():
        os.environ[key] = value


def load_scenario(name: str):
    if name == "binance_kline":
        return binance_kline.build_scenario()
    if name == "binance_perp":
        return binance_perp.build_scenario()
    if name == "binance_spot_link_kline_batch":
        return binance_spot_link_kline_batch.build_scenario()
    if name == "hyperliquid_perp":
        return hyperliquid_perp.build_scenario()
    if name == "spark_token_holders":
        return spark_token_holders.build_scenario()
    if name == "geckoterminal_link_liquidity":
        return geckoterminal_link_liquidity.build_scenario()
    raise ValueError(f"unknown scenario: {name}")


def stage_range(stages: List[str], from_stage: Optional[str], to_stage: Optional[str]) -> range:
    start = 0
    end = len(stages) - 1
    if from_stage:
        if from_stage not in stages:
            raise ValueError(f"from-stage not found: {from_stage}")
        start = stages.index(from_stage)
    if to_stage:
        if to_stage not in stages:
            raise ValueError(f"to-stage not found: {to_stage}")
        end = stages.index(to_stage)
    if start > end:
        raise ValueError("from-stage is after to-stage")
    return range(start, end + 1)


def select_stages(
    stages: List[str],
    stages_arg: Optional[str],
    from_stage: Optional[str],
    to_stage: Optional[str],
) -> List[int]:
    if stages_arg:
        names = [s.strip() for s in stages_arg.split(",") if s.strip()]
        if not names:
            raise ValueError("stages is empty")
        indices = []
        for name in names:
            if name not in stages:
                raise ValueError(f"stage not found: {name}")
            indices.append(stages.index(name))
        return indices
    return list(stage_range(stages, from_stage, to_stage))


def match_tags(required: List[str], actual: List[str]) -> bool:
    if not required:
        return True
    return all(tag in actual for tag in required)


def run_stage(ctx: RunContext, stage, run_dir: Path) -> List[ProbeResult]:
    results = []
    for probe in stage.probes:
        result = probe.func(ctx)
        log_probe_result(run_dir, ctx, probe.name, result)
        results.append(result)
    return results


def summarize(results: Iterable[ProbeResult]) -> str:
    status = ProbeStatus.SUCCESS
    for result in results:
        if result.status == ProbeStatus.FAIL:
            return "failed"
        if result.status == ProbeStatus.SKIP:
            status = ProbeStatus.SKIP
    return "skipped" if status == ProbeStatus.SKIP else "passed"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scenario runner")
    parser.add_argument("scenario", help="scenario name")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--env", default="local")
    parser.add_argument("--env-file", default="config/infrastructure/env/docker.env")
    parser.add_argument("--stages", default=None, help="comma-separated stage names")
    parser.add_argument("--from-stage", default=None)
    parser.add_argument("--to-stage", default=None)
    parser.add_argument("--tags", default="", help="comma-separated tag filters")
    parser.add_argument("--base-dir", default="automation/test/runs")
    parser.add_argument("--config-json", default=None, help="scenario config JSON string")
    parser.add_argument("--config-file", default=None, help="scenario config JSON file")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> dict:
    config = {}
    if args.config_file:
        path = Path(args.config_file)
        if path.exists():
            config.update(json.loads(path.read_text(encoding="utf-8")))
    if args.config_json:
        config.update(json.loads(args.config_json))
    return config


def main() -> None:
    args = parse_args()
    load_infra_env(args.env_file)
    run_id = args.run_id or new_run_id()
    scenario = load_scenario(args.scenario)

    required_tags = [t.strip() for t in args.tags.split(",") if t.strip()]
    if required_tags and not match_tags(required_tags, scenario.tags):
        print(json.dumps({"status": "skipped", "reason": "tag_mismatch"}, ensure_ascii=True))
        return

    run_dir = Path(args.base_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    stage_names = [stage.name for stage in scenario.stages]
    selected = select_stages(stage_names, args.stages, args.from_stage, args.to_stage)

    config = load_config(args)
    all_results: List[ProbeResult] = []
    for idx in selected:
        stage = scenario.stages[idx]
        if required_tags and not match_tags(required_tags, stage.tags):
            continue
        stage_ctx = RunContext(
            run_id=run_id,
            scenario=scenario.name,
            env=args.env,
            stage=stage.name,
            metadata=config,
        )
        all_results.extend(run_stage(stage_ctx, stage, run_dir))

    summary = {
        "run_id": run_id,
        "scenario": scenario.name,
        "status": summarize(all_results),
        "stage_count": len(list(selected)),
        "probe_count": len(all_results),
    }
    print(json.dumps(summary, ensure_ascii=True))
    write_summary(run_dir)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Frozen configuration for semantic_contract_filter_repair_v1.

This file is intentionally small.  It records the one path we want to preserve
before deleting historical experiment branches from the large research driver.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PureConfig:
    block_lines: int = 100_000
    workers: int = 8
    model: str = "gpt-4o"
    temperature: float = 0.0
    offline_sample_mode: str = "logbatcher"
    offline_logbatcher_per_family: int = 2
    offline_logbatcher_min_support: int = 50
    offline_sample_lines: int = 20_000
    offline_prefix_ratio: float = 1.0
    offline_min_prefix_lines: int = 0
    offline_topk: int = 40
    offline_batch_query: bool = True
    offline_batch_examples: int = 20
    offline_api_workers: int = 4
    offline_max_llm_calls: int = 80
    online_max_llm_calls: int = 0
    sample_train_replay_scope: str = "semantic"


PURE_COMPRESSOR_FLAGS: tuple[str, ...] = (
    "--compact-decoder-contract",
    "--compact-archive-container",
    "--skip-internal-decode-verify",
    "--operation-profile",
    "--whole-line-program-cache",
    "--open-arithmetic-programs",
    "--program-mdl-compiler",
    "--cegis-repair",
    "--program-line-mdl-semantic-grace",
    "--slot-complete-planner",
    "--free-form-program-prompt",
    "--free-form-transducer-sketch-prompt",
    "--semantic-focus-program-prompt",
    "--semantic-rich-examples-prompt",
    "--semantic-three-class-program-prompt",
    "--strict-three-class-context-schema",
    "--verifier-repair-agent",
    "--verifier-repair-max-total",
    "24",
    "--verifier-repair-max-per-family",
    "2",
    "--verifier-repair-max-candidates",
    "2",
    "--function-first-prompt",
    "--generic-token-shapes",
    "--residual-numeric-lattice-fallback",
    "denum_feature",
    "--residual-numeric-lattice-feature-admit",
    "--numeric-lattice-no-exact-fallback",
    "--numeric-lattice-skip-group-codec-cost",
    "--numeric-lattice-fast-select",
)


REMOVED_PRESET_FLAGS: tuple[str, ...] = (
    "--global-value-rescue",
    "--global-value-rescue-min-support",
    "--global-value-rescue-min-score",
    "--numeric-lattice-onepass-exact-select",
)


def build_runner_command(
    repo_root: Path,
    raw_dir: Path,
    result_dir: Path,
    datasets: list[str],
    config: PureConfig = PureConfig(),
) -> list[str]:
    """Return the frozen 100k-block benchmark command.

    The command still targets the existing research runner.  The next refactor
    step is to inline this path into a small `run_blocks_pure.py` and delete the
    alternative presets.
    """

    cmd = [
        "python3",
        str(repo_root / "run_stream_template_blocks.py"),
        "--raw-dir",
        str(raw_dir),
        "--datasets",
        *datasets,
        "--workers",
        str(config.workers),
        "--block-lines",
        str(config.block_lines),
        "--result-dir",
        str(result_dir),
        "--flag-preset",
        "feature-frontier-postgate-shapegate-localimpact-residual-codec-prefilter-auto-rare-familycache-auto-pregate-sample-fastpreprune-templatecache-nl-onepass-admiteligible-defer-trusted-adaptive-globalrescue",
        "--direct-compress",
        "--llm-cache-workflow",
        "sample-train-online",
        "--offline-train-sample-mode",
        config.offline_sample_mode,
        "--offline-train-logbatcher-per-family",
        str(config.offline_logbatcher_per_family),
        "--offline-train-logbatcher-min-support",
        str(config.offline_logbatcher_min_support),
        "--offline-train-sample-lines",
        str(config.offline_sample_lines),
        "--offline-train-topk",
        str(config.offline_topk),
        "--offline-train-batch-examples",
        str(config.offline_batch_examples),
        "--offline-train-api-workers",
        str(config.offline_api_workers),
        "--offline-train-max-llm-calls",
        str(config.offline_max_llm_calls),
        "--online-max-llm-calls",
        str(config.online_max_llm_calls),
        "--no-online-enable-cache-refresh",
        "--disable-default-cache-datasets",
        ",".join(datasets),
        "--force-llm",
        "--write-replay-plans",
    ]
    for flag in REMOVED_PRESET_FLAGS:
        cmd.extend(["--remove-flag", flag])
    for flag in PURE_COMPRESSOR_FLAGS:
        cmd.extend(["--append-flag", flag])
    return cmd

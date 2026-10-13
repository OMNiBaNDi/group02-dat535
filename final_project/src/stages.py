"""CP1 stage conventions and explicitly unfinished processing stages.

Stages receive a RunContext and return a StageResult after successful
execution. Failures raise exceptions.

Stage-specific input, version and rule arguments will be added when
the corresponding stage is implemented.

A StageResult describes execution outputs. It is not a publication
manifest and does not, by itself, establish downstream readiness.
"""

from dataclasses import dataclass
from pathlib import Path

from run_context import RunContext


@dataclass(frozen=True)
class StageResult:
    stage: str
    output_paths: tuple[Path, ...]
    report_path: Path


def run_bronze(context: RunContext) -> StageResult:
    """Reserved entry point for source acquisition."""
    raise NotImplementedError(
        "Bronze acquisition is not implemented; it belongs to CP2."
    )


def run_silver(context: RunContext) -> StageResult:
    """Reserved entry point for validation and preparation."""
    raise NotImplementedError(
        "Silver preparation is not implemented; it belongs to CP3-CP4."
    )


def run_gold(context: RunContext) -> StageResult:
    """Reserved entry point for catalogue and coverage generation."""
    raise NotImplementedError(
        "Gold analysis is not implemented; it belongs to CP5."
    )

"""Shared run identity and storage paths; creates no directories."""

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from project_config import select_environment_root


@dataclass(frozen=True)
class RunContext:
    environment: str
    environment_root: Path
    run_id: str

    @property
    def data_root(self) -> Path:
        return self.environment_root / "data"

    @property
    def staging_root(self) -> Path:
        return self.environment_root / "staging" / self.run_id

    @property
    def published_root(self) -> Path:
        return self.environment_root / "published"

    @property
    def run_root(self) -> Path:
        return self.environment_root / "runs" / self.run_id

    @property
    def logs_dir(self) -> Path:
        return self.run_root / "logs"

    @property
    def reports_dir(self) -> Path:
        return self.run_root / "reports"


def create_run_context(
    config_path: Path,
    environment: str,
    *,
    test_run_id: str | None = None,
) -> RunContext:
    root = select_environment_root(
        config_path,
        environment,
        test_run_id=test_run_id,
    )

    return RunContext(
        environment=environment,
        environment_root=root,
        run_id=uuid4().hex,
    )

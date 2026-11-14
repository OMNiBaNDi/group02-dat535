"""Keep the paths and ID for one pipeline run together.

Each environment has separate storage. Each run has its own staging, logs and
reports, while published releases stay available for later runs to reuse.
Creating this context only calculates paths; it does not create directories."""

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from project_config import select_environment_root


@dataclass(frozen=True)
class RunContext:
    """Hold one run's identity and derive its storage locations without creating them."""
    environment: str
    environment_root: Path
    run_id: str

    @property
    def data_root(self) -> Path:
        """Return the environment's dataset root, shared by published versions."""
        return self.environment_root / "data"

    @property
    def staging_root(self) -> Path:
        """Return this run's working directory for files not yet published."""
        return self.environment_root / "staging" / self.run_id

    @property
    def published_root(self) -> Path:
        """Return the root containing versioned publication manifests."""
        return self.environment_root / "published"

    @property
    def run_root(self) -> Path:
        """Return this run's directory for logs and reports."""
        return self.environment_root / "runs" / self.run_id

    @property
    def logs_dir(self) -> Path:
        """Return the log directory under this run."""
        return self.run_root / "logs"

    @property
    def reports_dir(self) -> Path:
        """Return the report directory under this run."""
        return self.run_root / "reports"


def create_run_context(
    config_path: Path,
    environment: str,
    *,
    test_run_id: str | None = None,
) -> RunContext:
    """Select an isolated environment root and return a context with a fresh run ID."""
    root = select_environment_root(
        config_path,
        environment,
        test_run_id=test_run_id,
    )

    # A test invocation can contain several attempts; each attempt receives a new run ID.
    return RunContext(
        environment=environment,
        environment_root=root,
        run_id=uuid4().hex,
    )

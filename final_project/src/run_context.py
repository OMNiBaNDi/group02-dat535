"""Keep the paths and ID for one pipeline run together.

Each environment has its own storage. Runs have separate staging and
reports folders, while saved releases can be reused.
Creating a context does not create directories.
"""

import subprocess
from pipeline_files import file_sha256, write_json

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from project_config import select_environment_root


@dataclass(frozen=True)
class RunContext:
    """Store the environment, storage folder and ID for one run."""
    environment: str
    environment_root: Path
    run_id: str

    @property
    def data_root(self) -> Path:
        """Folder for original downloads and saved tables."""
        return self.environment_root / "data"

    @property
    def staging_root(self) -> Path:
        """Return this run's working directory for files not yet published."""
        return self.environment_root / "staging" / self.run_id

    @property
    def published_root(self) -> Path:
        """Folder for manifests that describe published releases."""
        return self.environment_root / "published"

    @property
    def run_root(self) -> Path:
        """Parent folder for this run's reports."""
        return self.environment_root / "runs" / self.run_id

    @property
    def reports_dir(self) -> Path:
        """Folder for reports, saved settings and the executed notebook."""
        return self.run_root / "reports"


def create_run_context(
    config_path: Path,
    environment: str,
    *,
    test_run_id: str | None = None,
) -> RunContext:
    """Select the storage folder and give this execution a new run ID."""
    root = select_environment_root(
        config_path,
        environment,
        test_run_id=test_run_id,
    )

    # Several executions can share a test ID. Each gets its own run ID.
    return RunContext(
        environment=environment,
        environment_root=root,
        run_id=uuid4().hex,
    )


def capture_provenance(context, project_dir, settings):
    """Save the run settings and return the Git and runtime details."""
    # Save the settings actually used by this run.
    config_path = context.reports_dir / "effective_config.json"
    write_json(config_path, settings)
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=project_dir, text=True,
    ).strip()
    # The commit ID does not include uncommitted changes.
    changes = subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=project_dir, text=True,
    ).strip()
    return {
        "code_source": "git",
        "code_revision": revision,
        "working_tree_dirty": bool(changes),
        "config_sha256": file_sha256(config_path),
        "evidence_path": context.reports_dir.relative_to(context.environment_root).as_posix(),
        "runtime": settings["runtime_details"],
    }

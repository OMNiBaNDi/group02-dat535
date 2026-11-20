"""Read storage paths and run settings without creating directories."""

import json
from pathlib import Path


def load_environment_roots(config_path: Path) -> dict[str, Path]:
    """Return configured dev/test/prod storage paths without creating any directories."""
    settings = json.loads(config_path.read_text(encoding="utf-8"))
    return {
        environment: Path(settings[environment]).resolve()
        for environment in ("dev", "test", "prod")
    }


def select_environment_root(
    config_path: Path,
    environment: str,
    *,
    test_run_id: str | None = None,
) -> Path:
    """Select an environment root without creating directories."""
    roots = load_environment_roots(config_path)

    if environment not in roots:
        raise ValueError("Environment must be dev, test or prod.")

    root = roots[environment]

    # Dev and prod use their configured folders.
    # Tests use a subfolder named after test_run_id.
    if environment != "test":
        if test_run_id is not None:
            raise ValueError("test_run_id is only valid for the test environment.")
        return root

    if not isinstance(test_run_id, str) or not test_run_id:
        raise ValueError("The test environment requires a non-empty test_run_id.")

    # Keep the test ID to a single folder name.
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if len(test_run_id) > 80 or any(char not in allowed for char in test_run_id):
        raise ValueError(
            "test_run_id must contain 1-80 letters, digits, hyphens or underscores."
        )

    test_root = root / test_run_id

    # Do not let a symbolic link redirect this test folder.
    if test_root.resolve() != test_root:
        raise ValueError("The test invocation directory must not redirect through a symbolic link.")

    return test_root


def load_run_settings(project_dir, environ):
    """Read notebook settings without importing Spark or creating directories."""
    # The runner passes its options through environment variables.
    # Runtime selects where Spark runs. Environment selects the storage folder.
    runtime = environ.get("PIPELINE_RUNTIME", "vm")
    environment = environ.get("PIPELINE_ENVIRONMENT", "dev")
    supplied_config = environ.get("PIPELINE_ENVIRONMENTS_FILE")
    if runtime not in {"vm", "local"}:
        raise ValueError("PIPELINE_RUNTIME must be vm or local.")
    if runtime == "local" and (environment != "test" or not supplied_config):
        raise ValueError("Local runs require explicit test storage.")
    return {
        "runtime": runtime,
        "environment": environment,
        "environments_file": str(Path(supplied_config) if supplied_config else project_dir / "config/environments.json"),
        "test_run_id": environ.get("PIPELINE_TEST_RUN_ID"),
        "mode": environ.get("PIPELINE_MODE", "full"),
        "hourly_receipt": environ.get("PIPELINE_HOURLY_RECEIPT"),
        "gold_rules": environ.get("PIPELINE_GOLD_RULES"),
        "show_diagnostics": environ.get("PIPELINE_SHOW_DIAGNOSTICS", "false").lower() == "true",
        "configured_sources": json.loads((project_dir / "config/sources.json").read_text(encoding="utf-8")),
    }

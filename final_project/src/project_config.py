"""Read and validate environment storage roots without creating files."""

import json
from pathlib import Path


def load_environment_roots(config_path: Path) -> dict[str, Path]:
    settings = json.loads(config_path.read_text(encoding="utf-8"))

    if not isinstance(settings, dict) or set(settings) != {"dev", "test", "prod"}:
        raise ValueError("Configuration must contain exactly dev, test and prod.")

    # This file lives at <repository>/final_project/src/project_config.py.
    repository_root = Path(__file__).resolve().parents[2]
    roots = {}

    for environment, value in settings.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{environment}: storage root must be a non-empty string.")

        path = Path(value)
        if not path.is_absolute():
            raise ValueError(f"{environment}: storage root must be an absolute path.")

        # Normalize the path and follow any existing symbolic links.
        root = path.resolve()

        if root.is_relative_to(repository_root) or repository_root.is_relative_to(root):
            raise ValueError(f"{environment}: storage root must not overlap the repository.")

        if "_work" in root.parts:
            raise ValueError(f"{environment}: storage root must not be in a runner checkout.")

        roots[environment] = root

    for environment, root in roots.items():
        for other_environment, other_root in roots.items():
            if environment != other_environment and root.is_relative_to(other_root):
                raise ValueError(
                    f"{environment} and {other_environment}: storage roots must not overlap."
                )

    return roots


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

    if environment != "test":
        if test_run_id is not None:
            raise ValueError("test_run_id is only valid for the test environment.")
        return root

    if not isinstance(test_run_id, str) or not test_run_id:
        raise ValueError("The test environment requires a non-empty test_run_id.")

    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if len(test_run_id) > 80 or any(char not in allowed for char in test_run_id):
        raise ValueError(
            "test_run_id must contain 1-80 letters, digits, hyphens or underscores."
        )

    test_root = root / test_run_id

    # Reject links that redirect this invocation to a different directory.
    if test_root.resolve() != test_root:
        raise ValueError("The test invocation directory must not redirect through a symbolic link.")

    return test_root

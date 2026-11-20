"""Tests for dev, test and prod storage selection."""

import json
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from project_config import select_environment_root


class EnvironmentSelectionTests(unittest.TestCase):
    """Check the folders selected for each environment."""
    def setUp(self):
        """Use temporary storage paths and a config file for each test."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.roots = {
            environment: self.base / environment
            for environment in ("dev", "test", "prod")
        }
        self.config_path = self.base / "environments.json"
        self.config_path.write_text(
            json.dumps({name: str(path) for name, path in self.roots.items()}),
            encoding="utf-8",
        )

    def test_dev_and_prod_use_configured_roots(self):
        """Dev and prod should use their configured folders without creating them."""
        for environment in ("dev", "prod"):
            with self.subTest(environment=environment):
                selected = select_environment_root(
                    self.config_path, environment
                )
                self.assertEqual(selected, self.roots[environment])
                self.assertFalse(selected.exists())

    def test_test_invocations_have_separate_roots(self):
        """Different test IDs must lead to different folders under the test root."""
        selected_roots = []
        for identifier in ("invocation-001", "invocation-002"):
            selected = select_environment_root(
                self.config_path, "test", test_run_id=identifier
            )
            self.assertEqual(selected, self.roots["test"] / identifier)
            self.assertFalse(selected.exists())
            selected_roots.append(selected)

        self.assertNotEqual(selected_roots[0], selected_roots[1])

    def test_invalid_selections_are_rejected(self):
        """Invalid environments and test IDs should be rejected."""
        cases = {
            "unknown environment": ("production", None),
            "missing test ID": ("test", None),
            "empty test ID": ("test", ""),
            "non-string test ID": ("test", 42),
            "parent traversal": ("test", "../prod"),
            "nested path": ("test", "one/two"),
            "absolute path": ("test", "/tmp/example"),
            "spaces": ("test", "test one"),
            "overlong ID": ("test", "a" * 81),
            "test ID in dev": ("dev", "invocation-001"),
            "test ID in prod": ("prod", "invocation-001"),
        }

        for description, (environment, identifier) in cases.items():
            with self.subTest(case=description):
                with self.assertRaises(ValueError):
                    select_environment_root(
                        self.config_path,
                        environment,
                        test_run_id=identifier,
                    )

    def test_links_cannot_redirect_test_invocations(self):
        """Reject symbolic links that redirect a test folder."""
        self.roots["test"].mkdir()
        targets = (
            self.roots["prod"],
            self.roots["test"] / "another-invocation",
        )

        for index, target in enumerate(targets):
            with self.subTest(target=str(target)):
                target.mkdir()
                identifier = f"alias-{index}"
                alias = self.roots["test"] / identifier
                alias.symlink_to(target, target_is_directory=True)

                with self.assertRaisesRegex(ValueError, "symbolic link"):
                    select_environment_root(
                        self.config_path, "test", test_run_id=identifier
                    )


if __name__ == "__main__":
    unittest.main()

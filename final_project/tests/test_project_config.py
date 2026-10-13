"""Checks for configuration validity and storage-root isolation."""

import json
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from project_config import load_environment_roots


class EnvironmentConfigTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.config_path = self.base / "environments.json"
        self.valid = {
            "dev": str(self.base / "dev"),
            "test": str(self.base / "test"),
            "prod": str(self.base / "prod"),
        }

    def write_config(self, settings):
        self.config_path.write_text(json.dumps(settings), encoding="utf-8")

    def test_valid_roots_without_creating_directories(self):
        self.write_config(self.valid)

        roots = load_environment_roots(self.config_path)

        self.assertEqual(
            roots,
            {
                "dev": self.base / "dev",
                "test": self.base / "test",
                "prod": self.base / "prod",
            },
        )
        for root in roots.values():
            self.assertFalse(root.exists())

    def test_invalid_configurations_are_rejected(self):
        cases = {
            "missing environment": {
                "dev": self.valid["dev"],
                "test": self.valid["test"],
            },
            "unexpected environment": {
                **self.valid, "extra": str(self.base / "extra"),
            },
            "wrong structure": ["dev", "test", "prod"],
            "empty path": {**self.valid, "dev": ""},
            "non-string path": {**self.valid, "dev": None},
            "relative path": {**self.valid, "dev": "data/dev"},
            "identical roots": {**self.valid, "test": self.valid["dev"]},
            "nested roots": {
                **self.valid, "test": str(self.base / "dev" / "tests"),
            },
            "inside repository": {
                **self.valid, "dev": str(PROJECT_ROOT / "data"),
            },
            "contains repository": {
                **self.valid, "dev": str(PROJECT_ROOT.parent.parent),
            },
            "runner checkout": {
                **self.valid, "dev": str(self.base / "_work" / "repo" / "data"),
            },
        }

        for description, settings in cases.items():
            with self.subTest(case=description):
                self.write_config(settings)
                with self.assertRaises(ValueError):
                    load_environment_roots(self.config_path)

    def test_symbolic_link_cannot_hide_overlapping_roots(self):
        dev_root = self.base / "dev"
        dev_root.mkdir()
        alias = self.base / "test-alias"
        alias.symlink_to(dev_root, target_is_directory=True)
        self.write_config({**self.valid, "test": str(alias)})

        with self.assertRaisesRegex(ValueError, "overlap"):
            load_environment_roots(self.config_path)

    def test_malformed_json_is_rejected(self):
        self.config_path.write_text("{ invalid json", encoding="utf-8")

        with self.assertRaises(json.JSONDecodeError):
            load_environment_roots(self.config_path)


if __name__ == "__main__":
    unittest.main()

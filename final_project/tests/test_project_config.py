"""Test loading environment paths and rejecting malformed JSON."""

import json
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from project_config import load_environment_roots


class EnvironmentConfigTests(unittest.TestCase):
    """Check configuration loading with temporary storage paths."""
    def setUp(self):
        """Prepare three separate root paths and remove the temporary folder after each test."""
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
        """Write the settings for the current test to a temporary JSON file."""
        self.config_path.write_text(json.dumps(settings), encoding="utf-8")

    def test_valid_roots_without_creating_directories(self):
        """Valid settings should return paths without creating data directories."""
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

    def test_malformed_json_is_rejected(self):
        """Reject a configuration file with invalid JSON syntax."""
        self.config_path.write_text("{ invalid json", encoding="utf-8")

        with self.assertRaises(json.JSONDecodeError):
            load_environment_roots(self.config_path)


if __name__ == "__main__":
    unittest.main()

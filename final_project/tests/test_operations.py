"""Small check of the default notebook operation; no Spark session is needed."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from run_notebook import build_parser


class RunnerOptionsTests(unittest.TestCase):
    """Check the normal runner defaults used by the development workflow."""
    def test_normal_dev_run_uses_full_without_refresh(self):
        """Check that parsing dev options selects Full and leaves forced source refresh off."""
        args = build_parser().parse_args(["--environment", "dev"])

        self.assertEqual(args.mode, "full")
        self.assertFalse(args.refresh_sources)
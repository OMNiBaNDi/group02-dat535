"""Check the notebook runner's default options without starting Spark."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from run_notebook import build_parser


class RunnerOptionsTests(unittest.TestCase):
    """Check the normal runner defaults used by the development workflow."""
    def test_normal_dev_run_uses_full_on_vm(self):
        """Passing only --environment dev should select Full mode and the VM runtime."""
        args = build_parser().parse_args(["--environment", "dev"])

        self.assertEqual(args.mode, "full")
        self.assertEqual(args.runtime, "vm")
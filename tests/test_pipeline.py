"""Lightweight pipeline verification for the Creator Intelligence Layer."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


class PipelineSmokeTest(unittest.TestCase):
    """Verify the project ships with a working end-to-end run path."""

    def test_main_generates_report_and_processed_outputs(self) -> None:
        project_root = Path(__file__).resolve().parents[1]

        completed = subprocess.run(
            [sys.executable, "main.py"],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True,
        )

        self.assertIn("[creator-intelligence] complete", completed.stdout)
        self.assertTrue((project_root / "outputs" / "creator_report.md").exists())
        self.assertTrue((project_root / "data" / "processed" / "creator_kpis.csv").exists())
        self.assertTrue((project_root / "data" / "processed" / "detected_signals.csv").exists())


if __name__ == "__main__":
    unittest.main()

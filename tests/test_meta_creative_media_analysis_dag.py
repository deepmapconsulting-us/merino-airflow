from __future__ import annotations

import unittest
from pathlib import Path


class MetaCreativeMediaAnalysisDagTest(unittest.TestCase):
    def test_media_analysis_uses_one_combined_task_per_ad(self) -> None:
        dag_path = Path(__file__).resolve().parents[1] / "dags" / "meta_creative_media_analysis.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn("def download_and_analyze_ad_creative(", source)
        self.assertIn('task_id=f"download_and_analyze_ad_{ad_task_id}"', source)
        self.assertNotIn('task_id=f"download_ad_{ad_task_id}"', source)
        self.assertNotIn('task_id=f"analyze_ad_{ad_task_id}"', source)
        self.assertNotIn("downloaded >> analyzed", source)


if __name__ == "__main__":
    unittest.main()

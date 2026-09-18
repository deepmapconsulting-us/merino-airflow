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

    def test_cached_analysis_still_updates_missing_media_preview(self) -> None:
        dag_path = Path(__file__).resolve().parents[1] / "dags" / "meta_creative_media_analysis.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn("upsert_creative_media_preview(", source)
        self.assertIn("linked cached analysis media preview", source)

    def test_active_accounts_prefers_airflow_variable(self) -> None:
        dag_path = Path(__file__).resolve().parents[1] / "dags" / "meta_creative_media_analysis.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn("active_accounts = _dag_setting(ACTIVE_ACCOUNTS_ENV)", source)

    def test_dag_settings_accept_upper_and_lowercase_airflow_variables(self) -> None:
        dag_path = Path(__file__).resolve().parents[1] / "dags" / "meta_creative_media_analysis.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn("variable_get(name).strip()", source)
        self.assertIn("variable_get(name.lower()).strip()", source)


if __name__ == "__main__":
    unittest.main()

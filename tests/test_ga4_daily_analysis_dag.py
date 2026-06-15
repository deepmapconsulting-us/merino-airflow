from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path

import pendulum

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str((REPO / "airflow" / "dags").resolve()))
sys.path.insert(0, str((REPO / "airflow" / "module" / "ga4").resolve()))


def load_dag_module():
    spec = importlib.util.spec_from_file_location(
        "ga4_daily_analysis_dag_for_test",
        REPO / "airflow" / "dags" / "ga4_daily_analysis.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class GA4DailyAnalysisDagTest(unittest.TestCase):
    def test_refresh_candidates_cover_intraday_and_finalized_window(self) -> None:
        module = load_dag_module()
        logical_date = pendulum.datetime(2026, 6, 14, 20, 0, tz="America/Los_Angeles")

        candidates = module.ga4_refresh_candidates(logical_date)

        self.assertEqual(
            candidates,
            [
                {"report_date": date(2026, 6, 14), "source_table_type": "intraday"},
                {"report_date": date(2026, 6, 13), "source_table_type": "intraday"},
                {"report_date": date(2026, 6, 12), "source_table_type": "finalized"},
                {"report_date": date(2026, 6, 11), "source_table_type": "finalized"},
            ],
        )

    def test_refresh_skip_reason_skips_already_finalized(self) -> None:
        module = load_dag_module()

        reason = module.ga4_refresh_skip_reason(
            report_date=date(2026, 6, 12),
            source_table_type="finalized",
            source_table_exists=True,
            already_finalized=True,
        )

        self.assertIsNotNone(reason)
        self.assertIn("already finalized", reason)

    def test_refresh_skip_reason_skips_missing_source_table(self) -> None:
        module = load_dag_module()

        reason = module.ga4_refresh_skip_reason(
            report_date=date(2026, 6, 12),
            source_table_type="finalized",
            source_table_exists=False,
            already_finalized=False,
        )

        self.assertIsNotNone(reason)
        self.assertIn("missing source_table", reason)

    def test_refresh_skip_reason_allows_intraday_when_not_finalized(self) -> None:
        module = load_dag_module()

        reason = module.ga4_refresh_skip_reason(
            report_date=date(2026, 6, 14),
            source_table_type="intraday",
            source_table_exists=True,
            already_finalized=False,
        )

        self.assertIsNone(reason)

    def test_bigquery_table_exists_returns_false_for_not_found(self) -> None:
        module = load_dag_module()

        class NotFound(Exception):
            pass

        class MissingClient:
            def get_table(self, table_id: str) -> None:
                raise NotFound(table_id)

        self.assertFalse(module.bigquery_table_exists(MissingClient(), "missing.table"))


if __name__ == "__main__":
    unittest.main()

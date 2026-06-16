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
    def test_refresh_candidates_cover_four_day_window(self) -> None:
        module = load_dag_module()
        logical_date = pendulum.datetime(2026, 6, 14, 20, 0, tz="America/Los_Angeles")

        candidates = module.ga4_refresh_candidates(logical_date)

        self.assertEqual(
            candidates,
            [
                date(2026, 6, 14),
                date(2026, 6, 13),
                date(2026, 6, 12),
                date(2026, 6, 11),
            ],
        )

    def test_report_dates_for_run_uses_backfill_conf(self) -> None:
        module = load_dag_module()
        logical_date = pendulum.datetime(2026, 6, 14, 8, 0, tz="America/Los_Angeles")

        dates = module.ga4_report_dates_for_run(
            logical_date=logical_date,
            dag_run_conf={
                "backfill_start": "2026-05-28",
                "backfill_end": "2026-06-01",
            },
        )

        self.assertEqual(
            dates,
            [
                date(2026, 5, 28),
                date(2026, 5, 29),
                date(2026, 5, 30),
                date(2026, 5, 31),
                date(2026, 6, 1),
            ],
        )

    def test_report_dates_for_run_without_conf_uses_scheduled_window(self) -> None:
        module = load_dag_module()
        logical_date = pendulum.datetime(2026, 6, 14, 8, 0, tz="America/Los_Angeles")

        dates = module.ga4_report_dates_for_run(
            logical_date=logical_date,
            dag_run_conf={},
        )

        self.assertEqual(dates, module.ga4_refresh_candidates(logical_date))

    def test_resolve_ga4_source_table_prefers_intraday(self) -> None:
        module = load_dag_module()

        def table_exists(_client: object, table_id: str) -> bool:
            return table_id.endswith("events_intraday_20260613")

        resolved = module.resolve_ga4_source_table(
            client=object(),
            report_date=date(2026, 6, 13),
            table_exists=table_exists,
        )

        self.assertEqual(
            resolved,
            (
                "merino-agent.analytics_370932876.events_intraday_20260613",
                "intraday",
            ),
        )

    def test_resolve_ga4_source_table_falls_back_to_finalized(self) -> None:
        module = load_dag_module()

        def table_exists(_client: object, table_id: str) -> bool:
            return table_id.endswith("events_20260613")

        resolved = module.resolve_ga4_source_table(
            client=object(),
            report_date=date(2026, 6, 13),
            table_exists=table_exists,
        )

        self.assertEqual(
            resolved,
            (
                "merino-agent.analytics_370932876.events_20260613",
                "finalized",
            ),
        )

    def test_refresh_skip_reason_skips_already_finalized(self) -> None:
        module = load_dag_module()

        reason = module.ga4_refresh_skip_reason(
            report_date=date(2026, 6, 12),
            already_finalized=True,
            resolved_source=(
                "merino-agent.analytics_370932876.events_20260612",
                "finalized",
            ),
        )

        self.assertIsNotNone(reason)
        self.assertIn("already finalized", reason)

    def test_refresh_skip_reason_skips_when_no_source_tables_exist(self) -> None:
        module = load_dag_module()

        reason = module.ga4_refresh_skip_reason(
            report_date=date(2026, 6, 12),
            already_finalized=False,
            resolved_source=None,
        )

        self.assertIsNotNone(reason)
        self.assertIn("missing intraday and finalized source tables", reason)

    def test_refresh_skip_reason_allows_resolved_source(self) -> None:
        module = load_dag_module()

        reason = module.ga4_refresh_skip_reason(
            report_date=date(2026, 6, 14),
            already_finalized=False,
            resolved_source=(
                "merino-agent.analytics_370932876.events_intraday_20260614",
                "intraday",
            ),
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

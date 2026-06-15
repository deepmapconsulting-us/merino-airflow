from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "ga4"
sys.path.insert(0, str(MODULE_PATH))

from merino_ga4_jobs import experiments  # noqa: E402  # type: ignore[import-not-found]


class GA4ExperimentsTest(unittest.TestCase):
    def test_intraday_source_table(self) -> None:
        self.assertEqual(
            experiments.ga4_source_table(date(2026, 6, 14), intraday=True),
            "merino-agent.analytics_370932876.events_intraday_20260614",
        )

    def test_session_event_steps_query_filters_user_and_session(self) -> None:
        query = experiments.session_event_steps_query(
            experiment_name="20260614_purchaser_139step",
            report_date="2026-06-14",
            source_table="merino-agent.analytics_370932876.events_intraday_20260614",
            user_pseudo_id="2039920110.1781453903",
            ga_session_id=1781453902,
        )

        self.assertIn("experiment_name", query)
        self.assertIn("2039920110.1781453903", query)
        self.assertIn("AND ga_session_id = 1781453902", query)
        self.assertIn("AS event_step", query)
        self.assertIn("AS session_event_count", query)
        self.assertIn("ORDER BY o.user_pseudo_id, o.ga_session_id, o.event_step", query)

    def test_experiment_row_follows_postgres_column_order(self) -> None:
        row = {
            "experiment_name": "sample",
            "report_date": date(2026, 6, 14),
            "source_project_id": "merino-agent",
            "source_dataset_id": "analytics_370932876",
            "source_table": "merino-agent.analytics_370932876.events_intraday_20260614",
            "user_pseudo_id": "2039920110.1781453903",
            "ga_session_id": 1781453902,
            "session_event_count": 139,
            "session_purchase_count": 1,
            "first_purchase_step": 139,
            "event_step": 139,
            "event_name": "purchase",
            "event_timestamp_micros": 1,
            "event_at": "2026-06-14T09:50:29-07:00",
            "page_location": "https://example.com/thank-you",
            "page_path": "/thank-you",
        }

        self.assertEqual(
            experiments.experiment_row(row),
            (
                "sample",
                date(2026, 6, 14),
                "merino-agent",
                "analytics_370932876",
                "merino-agent.analytics_370932876.events_intraday_20260614",
                "2039920110.1781453903",
                1781453902,
                139,
                1,
                139,
                139,
                "purchase",
                1,
                "2026-06-14T09:50:29-07:00",
                "https://example.com/thank-you",
                "/thank-you",
            ),
        )

    def test_schema_unique_index_matches_conflict_columns(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        schema_sql = (repo / "metabase_schema" / "schema" / "ga4_experiments.sql").read_text()

        self.assertIn(
            "ON ga4.experiments (experiment_name, user_pseudo_id, ga_session_id, event_step)",
            schema_sql,
        )
        self.assertEqual(
            experiments.EXPERIMENTS_CONFLICT_COLUMNS,
            ("experiment_name", "user_pseudo_id", "ga_session_id", "event_step"),
        )

    def test_upsert_uses_conflict_update(self) -> None:
        calls: list[tuple[str, list[tuple[Any, ...]]]] = []

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
                calls.append((sql, rows))

        class FakeConn:
            def cursor(self) -> FakeCursor:
                return FakeCursor()

            def commit(self) -> None:
                return None

            def close(self) -> None:
                return None

        class FakePostgresHook:
            def __init__(self, postgres_conn_id: str) -> None:
                self.postgres_conn_id = postgres_conn_id

            def get_conn(self) -> FakeConn:
                return FakeConn()

        experiments.upsert_experiments(
            [
                {
                    "experiment_name": "sample",
                    "report_date": date(2026, 6, 14),
                    "source_project_id": "merino-agent",
                    "source_dataset_id": "analytics_370932876",
                    "source_table": "events_intraday_20260614",
                    "user_pseudo_id": "2039920110.1781453903",
                    "ga_session_id": 1781453902,
                    "session_event_count": 1,
                    "session_purchase_count": 0,
                    "first_purchase_step": None,
                    "event_step": 1,
                    "event_name": "page_view",
                    "event_timestamp_micros": 1,
                    "event_at": None,
                    "page_location": None,
                    "page_path": None,
                }
            ],
            postgres_hook_factory=FakePostgresHook,
        )

        sql, rows = calls[0]
        self.assertIn("INSERT INTO ga4.experiments AS target", sql)
        self.assertIn(
            "ON CONFLICT (experiment_name, user_pseudo_id, ga_session_id, event_step) DO UPDATE",
            sql,
        )
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()

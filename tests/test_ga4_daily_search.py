from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "ga4"
sys.path.insert(0, str(MODULE_PATH))

from merino_ga4_jobs import daily_search  # noqa: E402  # type: ignore[import-not-found]


class GA4DailySearchTest(unittest.TestCase):
    def test_daily_search_query_uses_result_page_and_purchase_after_search(self) -> None:
        query = daily_search.daily_search_query(date(2026, 6, 12))

        self.assertIn("FROM `merino-agent.analytics_370932876.events_20260612`", query)
        self.assertIn("DATE '2026-06-12' AS report_date", query)
        self.assertIn("'finalized' AS source_table_type", query)
        self.assertIn("TRUE AS is_finalized", query)
        self.assertIn("event_name IN ('search', 'view_search_results')", query)
        self.assertIn("AS search_content", query)
        self.assertIn("AS results_found", query)
        self.assertIn("COUNTIF(saw_results_page)", query)
        self.assertIn("AS reached_results_count", query)
        self.assertIn("COUNT(DISTINCT IF(", query)
        self.assertIn("AS purchase_after_search_session_count", query)
        self.assertIn("AS purchase_after_search_rate", query)

    def test_daily_search_content_query_groups_by_search_content(self) -> None:
        query = daily_search.daily_search_content_query(
            "20260614",
            source_table="merino-agent.analytics_370932876.events_intraday_20260614",
            source_table_type="intraday",
        )

        self.assertIn("FROM `merino-agent.analytics_370932876.events_intraday_20260614`", query)
        self.assertIn("'intraday' AS source_table_type", query)
        self.assertIn("FALSE AS is_finalized", query)
        self.assertIn("GROUP BY 1, 2, 3, 4, 5, 6, 7", query)
        self.assertIn("ORDER BY search_count DESC, search_content", query)

    def test_daily_search_row_follows_postgres_column_order(self) -> None:
        row = {
            "report_date": date(2026, 6, 12),
            "source_project_id": "merino-agent",
            "source_dataset_id": "analytics_370932876",
            "source_table": "merino-agent.analytics_370932876.events_20260612",
            "source_table_type": "finalized",
            "is_finalized": True,
            "search_count": 10,
            "search_session_count": 8,
            "search_user_count": 7,
            "reached_results_count": 6,
            "reach_rate": 0.6,
            "positive_results_count": 5,
            "zero_results_count": 1,
            "unknown_results_count": 4,
            "purchase_after_search_session_count": 2,
            "purchase_after_search_rate": 0.25,
        }

        self.assertEqual(
            daily_search.daily_search_row(row),
            (
                date(2026, 6, 12),
                "merino-agent",
                "analytics_370932876",
                "merino-agent.analytics_370932876.events_20260612",
                "finalized",
                True,
                10,
                8,
                7,
                6,
                0.6,
                5,
                1,
                4,
                2,
                0.25,
            ),
        )

    def test_daily_search_content_row_follows_postgres_column_order(self) -> None:
        row = {
            "report_date": date(2026, 6, 12),
            "search_content": "bra",
            "source_project_id": "merino-agent",
            "source_dataset_id": "analytics_370932876",
            "source_table": "merino-agent.analytics_370932876.events_20260612",
            "source_table_type": "finalized",
            "is_finalized": True,
            "search_count": 10,
            "search_session_count": 8,
            "search_user_count": 7,
            "reached_results_count": 6,
            "reach_rate": 0.6,
            "positive_results_count": 5,
            "zero_results_count": 1,
            "unknown_results_count": 4,
            "avg_results_found": 21.5,
            "max_results_found": 35,
            "purchase_after_search_session_count": 2,
            "purchase_after_search_rate": 0.25,
        }

        self.assertEqual(
            daily_search.daily_search_content_row(row),
            (
                date(2026, 6, 12),
                "bra",
                "merino-agent",
                "analytics_370932876",
                "merino-agent.analytics_370932876.events_20260612",
                "finalized",
                True,
                10,
                8,
                7,
                6,
                0.6,
                5,
                1,
                4,
                21.5,
                35,
                2,
                0.25,
            ),
        )

    def test_upsert_conflict_keys_match_schema_constraints(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        schema_sql = (
            repo
            / "metabase_schema"
            / "schema"
            / "merino-analytics"
            / "ga4"
            / "ga4_daily_search.sql"
        ).read_text()

        self.assertIn("ON ga4.daily_search (report_date)", schema_sql)
        self.assertIn("ON ga4.daily_search_content (report_date, search_content)", schema_sql)
        self.assertEqual(daily_search.DAILY_SEARCH_CONFLICT_COLUMNS, ("report_date",))
        self.assertEqual(
            daily_search.DAILY_SEARCH_CONTENT_CONFLICT_COLUMNS,
            ("report_date", "search_content"),
        )

    def test_upsert_uses_conflict_update_and_change_guard(self) -> None:
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

        daily_search.upsert_daily_search(
            [
                {
                    "report_date": date(2026, 6, 12),
                    "source_project_id": "merino-agent",
                    "source_dataset_id": "analytics_370932876",
                    "source_table": "events_20260612",
                    "source_table_type": "finalized",
                    "is_finalized": True,
                    "search_count": 10,
                    "search_session_count": 8,
                    "search_user_count": 7,
                    "reached_results_count": 6,
                    "reach_rate": 0.6,
                    "positive_results_count": 5,
                    "zero_results_count": 1,
                    "unknown_results_count": 4,
                    "purchase_after_search_session_count": 2,
                    "purchase_after_search_rate": 0.25,
                }
            ],
            postgres_hook_factory=FakePostgresHook,
        )

        sql, rows = calls[0]
        self.assertIn("INSERT INTO ga4.daily_search AS target", sql)
        self.assertIn("ON CONFLICT (report_date) DO UPDATE", sql)
        self.assertIn("update_count = target.update_count + 1", sql)
        self.assertIn("target.reach_rate IS DISTINCT FROM EXCLUDED.reach_rate", sql)
        self.assertIn(
            "target.purchase_after_search_rate IS DISTINCT FROM EXCLUDED.purchase_after_search_rate",
            sql,
        )
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()

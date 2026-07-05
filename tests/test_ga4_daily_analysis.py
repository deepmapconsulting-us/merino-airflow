from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from typing import Any

import pendulum

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "ga4"
sys.path.insert(0, str(MODULE_PATH))

from merino_ga4_jobs import daily_analysis  # noqa: E402  # type: ignore[import-not-found]


class GA4DailyAnalysisTest(unittest.TestCase):
    def test_report_date_uses_two_day_delay(self) -> None:
        logical_date = pendulum.datetime(2026, 6, 14, 7, 0, tz="America/Los_Angeles")

        self.assertEqual(daily_analysis.ga4_report_date(logical_date), date(2026, 6, 12))

    def test_table_suffix_accepts_safe_ga4_dates_only(self) -> None:
        self.assertEqual(daily_analysis.ga4_table_suffix(date(2026, 6, 12)), "20260612")
        self.assertEqual(daily_analysis.ga4_table_suffix("2026-06-12"), "20260612")
        self.assertEqual(daily_analysis.ga4_table_suffix("20260612"), "20260612")

        with self.assertRaises(ValueError):
            daily_analysis.ga4_table_suffix("20260612; SELECT 1")

    def test_daily_query_uses_finalized_table_and_stable_event_order(self) -> None:
        query = daily_analysis.daily_analysis_query(date(2026, 6, 12))

        self.assertIn("FROM `merino-agent.analytics_370932876.events_20260612`", query)
        self.assertIn("DATE '2026-06-12' AS report_date", query)
        self.assertIn("'finalized' AS source_table_type", query)
        self.assertIn("TRUE AS is_finalized", query)
        self.assertIn("WHERE key = 'ga_session_id'", query)
        self.assertIn("WHERE key = 'page_location'", query)
        self.assertIn(
            "ORDER BY\n"
            "        event_timestamp,\n"
            "        event_bundle_sequence_id,\n"
            "        batch_page_id,\n"
            "        batch_ordering_id,\n"
            "        batch_event_index",
            query,
        )
        self.assertIn("COUNTIF(event_name = 'purchase') AS purchase_count", query)
        self.assertIn("AS min_session_steps", query)
        self.assertIn("AS max_session_steps", query)
        self.assertIn("AS min_seconds_to_purchase", query)
        self.assertIn("AS max_seconds_to_purchase", query)

    def test_queries_can_use_intraday_source_table(self) -> None:
        query = daily_analysis.daily_analysis_query(
            "20260614",
            source_table="merino-agent.analytics_370932876.events_intraday_20260614",
            source_table_type="intraday",
        )

        self.assertIn("FROM `merino-agent.analytics_370932876.events_intraday_20260614`", query)
        self.assertIn("'intraday' AS source_table_type", query)
        self.assertIn("FALSE AS is_finalized", query)

    def test_landing_page_query_groups_by_first_session_page(self) -> None:
        query = daily_analysis.landing_page_daily_analysis_query("20260612")

        self.assertIn("ARRAY_AGG(page_location IGNORE NULLS ORDER BY event_step LIMIT 1)", query)
        self.assertIn("'merino-agent' AS source_project_id", query)
        self.assertIn("'analytics_370932876' AS source_dataset_id", query)
        self.assertIn("'finalized' AS source_table_type", query)
        self.assertIn("TRUE AS is_finalized", query)
        self.assertIn("AS min_session_seconds", query)
        self.assertIn("AS max_session_seconds", query)
        self.assertIn("AS min_purchase_step", query)
        self.assertIn("AS max_purchase_step", query)
        self.assertIn("GROUP BY 1, 2, 3, 4, 5, 6, 7", query)
        self.assertIn("ORDER BY session_count DESC, landing_page", query)

    def test_rows_follow_postgres_column_order(self) -> None:
        row = {
            "report_date": date(2026, 6, 12),
            "source_project_id": "merino-agent",
            "source_dataset_id": "analytics_370932876",
            "source_table": "merino-agent.analytics_370932876.events_20260612",
            "source_table_type": "finalized",
            "is_finalized": True,
            "event_count": 10,
            "session_count": 2,
            "user_count": 2,
            "purchase_count": 1,
            "purchaser_count": 1,
            "avg_session_steps": 5,
            "min_session_steps": 3,
            "max_session_steps": 7,
            "avg_session_seconds": 120,
            "min_session_seconds": 60,
            "max_session_seconds": 180,
            "avg_session_minutes": 2,
            "min_session_minutes": 1,
            "max_session_minutes": 3,
            "avg_purchase_step": 4,
            "min_purchase_step": 4,
            "max_purchase_step": 4,
            "avg_seconds_to_purchase": 90,
            "min_seconds_to_purchase": 90,
            "max_seconds_to_purchase": 90,
            "avg_minutes_to_purchase": 1.5,
            "min_minutes_to_purchase": 1.5,
            "max_minutes_to_purchase": 1.5,
        }

        self.assertEqual(
            daily_analysis.daily_analysis_row(row),
            (
                date(2026, 6, 12),
                "merino-agent",
                "analytics_370932876",
                "merino-agent.analytics_370932876.events_20260612",
                "finalized",
                True,
                10,
                2,
                2,
                1,
                1,
                5,
                3,
                7,
                120,
                60,
                180,
                2,
                1,
                3,
                4,
                4,
                4,
                90,
                90,
                90,
                1.5,
                1.5,
                1.5,
            ),
        )

    def test_upsert_conflict_keys_match_schema_constraints(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        schema_dir = repo / "metabase_schema" / "schema" / "merino-analytics" / "ga4"
        schema_sql = "\n".join(path.read_text() for path in sorted(schema_dir.glob("ga4_*.sql")))

        self.assertIn("ON ga4.daily_analysis (report_date)", schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS min_session_steps", schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS max_minutes_to_purchase", schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS source_table_type TEXT NOT NULL DEFAULT 'finalized'", schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS is_finalized BOOLEAN NOT NULL DEFAULT true", schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS source_project_id TEXT", schema_sql)
        self.assertEqual(daily_analysis.DAILY_ANALYSIS_CONFLICT_COLUMNS, ("report_date",))
        self.assertIn("ON ga4.landing_page_daily_analysis (report_date, landing_page)", schema_sql)
        self.assertEqual(
            daily_analysis.LANDING_PAGE_DAILY_ANALYSIS_CONFLICT_COLUMNS,
            ("report_date", "landing_page"),
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

        daily_analysis.upsert_ga4_rows(
            daily_analysis.DAILY_ANALYSIS_TABLE,
            daily_analysis.DAILY_ANALYSIS_COLUMNS,
            daily_analysis.DAILY_ANALYSIS_CONFLICT_COLUMNS,
            daily_analysis.DAILY_ANALYSIS_CHANGE_COLUMNS,
            [
                (
                    date(2026, 6, 12),
                    "merino-agent",
                    "analytics_370932876",
                    "events_20260612",
                    "finalized",
                    True,
                    1,
                    1,
                    1,
                    1,
                    1,
                    *([1] * 18),
                )
            ],
            postgres_hook_factory=FakePostgresHook,
        )

        sql, rows = calls[0]
        self.assertIn("INSERT INTO ga4.daily_analysis AS target", sql)
        self.assertIn("ON CONFLICT (report_date) DO UPDATE", sql)
        self.assertIn("update_count = target.update_count + 1", sql)
        self.assertIn("target.purchase_count IS DISTINCT FROM EXCLUDED.purchase_count", sql)
        self.assertIn("target.max_minutes_to_purchase IS DISTINCT FROM EXCLUDED.max_minutes_to_purchase", sql)
        self.assertEqual(len(rows), 1)

    def test_replace_deletes_report_date_before_insert(self) -> None:
        calls: list[tuple[str, Any]] = []

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def execute(self, sql: str, params: tuple[Any, ...]) -> None:
                calls.append((sql, params))

            def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
                calls.append((sql, rows))

        class FakeConn:
            def cursor(self) -> FakeCursor:
                return FakeCursor()

            def commit(self) -> None:
                calls.append(("commit", None))

            def close(self) -> None:
                return None

        class FakePostgresHook:
            def __init__(self, postgres_conn_id: str) -> None:
                self.postgres_conn_id = postgres_conn_id

            def get_conn(self) -> FakeConn:
                return FakeConn()

        daily_analysis.replace_ga4_rows_for_report_date(
            daily_analysis.DAILY_ANALYSIS_TABLE,
            daily_analysis.DAILY_ANALYSIS_COLUMNS,
            [(date(2026, 6, 12), *([1] * (len(daily_analysis.DAILY_ANALYSIS_COLUMNS) - 1)))],
            date(2026, 6, 12),
            postgres_hook_factory=FakePostgresHook,
        )

        self.assertIn("DELETE FROM ga4.daily_analysis WHERE report_date = %s", calls[0][0])
        self.assertEqual(calls[0][1], ("2026-06-12",))
        self.assertIn("INSERT INTO ga4.daily_analysis", calls[1][0])
        self.assertEqual(calls[-1], ("commit", None))

    def test_merge_upserts_rows_and_deletes_orphans_in_one_transaction(self) -> None:
        calls: list[tuple[str, Any]] = []

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
                calls.append((sql, params))

            def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
                calls.append((sql, rows))

        class FakeConn:
            def cursor(self) -> FakeCursor:
                return FakeCursor()

            def commit(self) -> None:
                calls.append(("commit", None))

            def close(self) -> None:
                return None

        class FakePostgresHook:
            def __init__(self, postgres_conn_id: str) -> None:
                self.postgres_conn_id = postgres_conn_id

            def get_conn(self) -> FakeConn:
                return FakeConn()

        landing_page_row = (
            date(2026, 6, 12),
            "/",
            "merino-agent",
            "analytics_370932876",
            "merino-agent.analytics_370932876.events_20260612",
            "finalized",
            True,
            *([1] * (len(daily_analysis.LANDING_PAGE_DAILY_ANALYSIS_COLUMNS) - 7)),
        )

        daily_analysis.merge_ga4_rows_for_report_date(
            daily_analysis.LANDING_PAGE_DAILY_ANALYSIS_TABLE,
            daily_analysis.LANDING_PAGE_DAILY_ANALYSIS_COLUMNS,
            daily_analysis.LANDING_PAGE_DAILY_ANALYSIS_CONFLICT_COLUMNS,
            daily_analysis.LANDING_PAGE_DAILY_ANALYSIS_CHANGE_COLUMNS,
            [landing_page_row],
            date(2026, 6, 12),
            postgres_hook_factory=FakePostgresHook,
        )

        self.assertIn("INSERT INTO ga4.landing_page_daily_analysis AS target", calls[0][0])
        self.assertIn("ON CONFLICT (report_date, landing_page) DO UPDATE", calls[0][0])
        orphan_sql, orphan_params = calls[1]
        self.assertIn("DELETE FROM ga4.landing_page_daily_analysis", orphan_sql)
        self.assertIn("landing_page) NOT IN", orphan_sql)
        self.assertEqual(orphan_params[0], "2026-06-12")
        self.assertEqual(orphan_params[1], "/")
        self.assertEqual(calls[-1], ("commit", None))

    def test_merge_deletes_all_rows_when_batch_is_empty(self) -> None:
        calls: list[tuple[str, Any]] = []

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
                calls.append((sql, params))

            def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
                calls.append((sql, rows))

        class FakeConn:
            def cursor(self) -> FakeCursor:
                return FakeCursor()

            def commit(self) -> None:
                calls.append(("commit", None))

            def close(self) -> None:
                return None

        class FakePostgresHook:
            def __init__(self, postgres_conn_id: str) -> None:
                self.postgres_conn_id = postgres_conn_id

            def get_conn(self) -> FakeConn:
                return FakeConn()

        daily_analysis.merge_ga4_rows_for_report_date(
            daily_analysis.DAILY_ANALYSIS_TABLE,
            daily_analysis.DAILY_ANALYSIS_COLUMNS,
            daily_analysis.DAILY_ANALYSIS_CONFLICT_COLUMNS,
            daily_analysis.DAILY_ANALYSIS_CHANGE_COLUMNS,
            [],
            date(2026, 6, 12),
            postgres_hook_factory=FakePostgresHook,
        )

        self.assertEqual(len(calls), 2)
        self.assertIn("DELETE FROM ga4.daily_analysis WHERE report_date = %s", calls[0][0])
        self.assertEqual(calls[0][1], ("2026-06-12",))
        self.assertEqual(calls[-1], ("commit", None))

    def test_report_date_is_finalized_reads_cloud_sql_flag(self) -> None:
        calls: list[tuple[str, tuple[Any, ...] | None]] = []

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
                calls.append((sql, params))

            def fetchone(self) -> tuple[bool]:
                return (True,)

        class FakeConn:
            def cursor(self) -> FakeCursor:
                return FakeCursor()

            def close(self) -> None:
                return None

        class FakePostgresHook:
            def __init__(self, postgres_conn_id: str) -> None:
                self.postgres_conn_id = postgres_conn_id

            def get_conn(self) -> FakeConn:
                return FakeConn()

        self.assertTrue(
            daily_analysis.report_date_is_finalized(
                date(2026, 6, 12),
                postgres_hook_factory=FakePostgresHook,
            )
        )
        self.assertIn("FROM ga4.daily_analysis", calls[0][0])
        self.assertEqual(calls[0][1], ("2026-06-12",))


if __name__ == "__main__":
    unittest.main()

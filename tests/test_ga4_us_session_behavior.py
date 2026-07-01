from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "ga4"
sys.path.insert(0, str(MODULE_PATH))

from merino_ga4_jobs import us_session_behavior  # noqa: E402  # type: ignore[import-not-found]


class GA4USSessionBehaviorTest(unittest.TestCase):
    def test_daily_query_uses_us_session_entry_and_purchaser_only_averages(self) -> None:
        query = us_session_behavior.us_session_daily_query(date(2026, 6, 28))

        self.assertIn("FROM `merino-agent.analytics_370932876.events_20260628`", query)
        self.assertIn("entry_country AS country", query)
        self.assertIn("WHEN geo.country = 'United States' THEN 'US'", query)
        self.assertIn("WHEN geo.country = 'Australia' THEN 'AU'", query)
        self.assertIn("WHEN geo.country = 'Canada' THEN 'CA'", query)
        self.assertNotIn("WHERE s.entry_country = 'United States'", query)
        self.assertIn("WHERE key = 'ga_session_id'", query)
        self.assertIn("WHERE key = 'ga_session_number'", query)
        self.assertIn("COUNTIF(event_name = 'add_to_cart') AS add_to_cart_events", query)
        self.assertIn("COUNTIF(event_name = 'purchase') AS purchase_events", query)
        self.assertIn("AS entry_source", query)
        self.assertIn("AS entry_medium", query)
        self.assertIn("AS entry_channel_group", query)
        self.assertIn("AS facebook_session_count", query)
        self.assertIn("AS google_session_count", query)
        self.assertIn("AS referral_session_count", query)
        self.assertIn("AS direct_session_count", query)
        self.assertIn("COALESCE(ROUND(MIN(session_seconds), 6), 0) AS min_session_seconds", query)
        self.assertIn("COALESCE(ROUND(MAX(session_seconds), 6), 0) AS max_session_seconds", query)
        self.assertIn(
            "COALESCE(ROUND(AVG(IF(purchase_events > 0, session_seconds, NULL)), 6), 0) "
            "AS purchaser_avg_session_seconds",
            query,
        )
        self.assertIn(
            "COALESCE(ROUND(AVG(IF(purchase_events > 0, event_count, NULL)), 6), 0) "
            "AS purchaser_avg_events_per_session",
            query,
        )

    def test_state_and_hour_queries_group_by_expected_dimensions(self) -> None:
        state_query = us_session_behavior.us_session_daily_state_query("20260628")
        hour_query = us_session_behavior.us_session_daily_hour_query("20260628")
        state_hour_query = us_session_behavior.us_session_daily_state_hour_query("20260628")

        self.assertIn("entry_country AS country", state_query)
        self.assertIn("entry_state AS state", state_query)
        self.assertIn("GROUP BY 1, 2, 3, 4, 5, 6, 7, 8", state_query)
        self.assertIn("purchaser_avg_session_seconds", state_query)

        self.assertIn("entry_country AS country", hour_query)
        self.assertIn("AS pacific_hour", hour_query)
        self.assertIn("AS pacific_hour_start", hour_query)
        self.assertIn("GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9", hour_query)
        self.assertNotIn("purchaser_avg_session_seconds", hour_query)

        self.assertIn("entry_country AS country", state_hour_query)
        self.assertIn("entry_state AS state", state_hour_query)
        self.assertIn("AS pacific_hour", state_hour_query)
        self.assertIn("GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10", state_hour_query)
        self.assertNotIn("purchaser_avg_session_seconds", state_hour_query)

    def test_intraday_source_table_marks_rows_unfinalized(self) -> None:
        query = us_session_behavior.us_session_daily_query(
            "20260630",
            source_table="merino-agent.analytics_370932876.events_intraday_20260630",
            source_table_type="intraday",
        )

        self.assertIn("FROM `merino-agent.analytics_370932876.events_intraday_20260630`", query)
        self.assertIn("'intraday' AS source_table_type", query)
        self.assertIn("FALSE AS is_finalized", query)

    def test_rows_follow_postgres_column_order(self) -> None:
        row = {
            "report_date": date(2026, 6, 28),
            "country": "US",
            "state": "California",
            "pacific_hour": 13,
            "pacific_hour_start": "2026-06-28T13:00:00",
            "source_project_id": "merino-agent",
            "source_dataset_id": "analytics_370932876",
            "source_table": "merino-agent.analytics_370932876.events_20260628",
            "source_table_type": "finalized",
            "is_finalized": True,
            "session_count": 10,
            "user_count": 9,
            "event_count": 100,
            "avg_events_per_session": 10,
            "avg_session_seconds": 20,
            "min_session_seconds": 1,
            "max_session_seconds": 200,
            "median_session_seconds": 12,
            "p90_session_seconds": 90,
            "add_to_cart_session_count": 2,
            "add_to_cart_user_count": 2,
            "purchase_session_count": 1,
            "purchaser_count": 1,
            "returning_session_count": 3,
            "returning_user_count": 2,
            "facebook_session_count": 4,
            "google_session_count": 5,
            "referral_session_count": 6,
            "direct_session_count": 7,
            "purchaser_avg_session_seconds": 50,
            "purchaser_avg_events_per_session": 25,
        }

        self.assertEqual(
            us_session_behavior.us_session_daily_state_row(row),
            (
                date(2026, 6, 28),
                "US",
                "California",
                "merino-agent",
                "analytics_370932876",
                "merino-agent.analytics_370932876.events_20260628",
                "finalized",
                True,
                10,
                9,
                100,
                10,
                20,
                1,
                200,
                12,
                90,
                2,
                2,
                1,
                1,
                3,
                2,
                4,
                5,
                6,
                7,
                50,
                25,
            ),
        )
        self.assertEqual(
            us_session_behavior.us_session_daily_hour_row(row),
            (
                date(2026, 6, 28),
                "US",
                13,
                "2026-06-28T13:00:00",
                "merino-agent",
                "analytics_370932876",
                "merino-agent.analytics_370932876.events_20260628",
                "finalized",
                True,
                10,
                9,
                100,
                10,
                20,
                1,
                200,
                12,
                90,
                2,
                2,
                1,
                1,
                3,
                2,
                4,
                5,
                6,
                7,
            ),
        )

    def test_conflict_keys_match_schema_constraints(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        schema_dir = repo / "metabase_schema" / "schema"
        schema_sql = "\n".join(
            [
                (schema_dir / "ga4_us_session_behavior.sql").read_text(),
                (schema_dir / "ga4_us_session_behavior_channel_columns.sql").read_text(),
                (schema_dir / "ga4_us_session_behavior_country_columns.sql").read_text(),
            ]
        )

        self.assertIn("ON ga4.us_session_daily (report_date, country)", schema_sql)
        self.assertIn("ON ga4.us_session_daily_state (report_date, country, state)", schema_sql)
        self.assertIn("ON ga4.us_session_daily_hour (report_date, country, pacific_hour)", schema_sql)
        self.assertIn(
            "ON ga4.us_session_daily_state_hour (report_date, country, pacific_hour, state)",
            schema_sql,
        )
        self.assertEqual(us_session_behavior.US_SESSION_DAILY_CONFLICT_COLUMNS, ("report_date", "country"))
        self.assertEqual(
            us_session_behavior.US_SESSION_DAILY_STATE_CONFLICT_COLUMNS,
            ("report_date", "country", "state"),
        )
        self.assertEqual(
            us_session_behavior.US_SESSION_DAILY_HOUR_CONFLICT_COLUMNS,
            ("report_date", "country", "pacific_hour"),
        )
        self.assertEqual(
            us_session_behavior.US_SESSION_DAILY_STATE_HOUR_CONFLICT_COLUMNS,
            ("report_date", "country", "pacific_hour", "state"),
        )
        self.assertIn("ADD COLUMN IF NOT EXISTS facebook_session_count", schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS google_session_count", schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS referral_session_count", schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS direct_session_count", schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS country TEXT NOT NULL DEFAULT 'US'", schema_sql)


if __name__ == "__main__":
    unittest.main()

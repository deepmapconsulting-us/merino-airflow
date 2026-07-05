from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "ga4"
sys.path.insert(0, str(MODULE_PATH))

from merino_ga4_jobs import daily_purchaser_behavior  # noqa: E402  # type: ignore[import-not-found]


class GA4DailyPurchaserBehaviorTest(unittest.TestCase):
    def test_query_uses_finalized_table_and_stable_event_order(self) -> None:
        query = daily_purchaser_behavior.daily_purchaser_behavior_query(date(2026, 6, 12))

        self.assertIn("FROM `merino-agent.analytics_370932876.events_20260612`", query)
        self.assertIn("DATE '2026-06-12' AS report_date", query)
        self.assertIn("'finalized' AS source_table_type", query)
        self.assertIn("TRUE AS is_finalized", query)
        self.assertIn(
            "ORDER BY\n"
            "        event_timestamp,\n"
            "        event_bundle_sequence_id,\n"
            "        batch_page_id,\n"
            "        batch_ordering_id,\n"
            "        batch_event_index",
            query,
        )

    def test_query_can_use_intraday_source_table(self) -> None:
        query = daily_purchaser_behavior.daily_purchaser_behavior_query(
            "20260614",
            source_table="merino-agent.analytics_370932876.events_intraday_20260614",
            source_table_type="intraday",
        )

        self.assertIn("FROM `merino-agent.analytics_370932876.events_intraday_20260614`", query)
        self.assertIn("'intraday' AS source_table_type", query)
        self.assertIn("FALSE AS is_finalized", query)

    def test_query_filters_purchasing_sessions_and_includes_metrics(self) -> None:
        query = daily_purchaser_behavior.daily_purchaser_behavior_query("20260612")

        self.assertIn("WHERE purchase_event_count > 0", query)
        self.assertIn("device.category AS device_category", query)
        self.assertIn("device.browser AS device_browser", query)
        self.assertIn("session_traffic_source_last_click.cross_channel_campaign", query)
        self.assertIn("collected_traffic_source.manual_source", query)
        self.assertIn("AS traffic_placement", query)
        self.assertIn("AS is_meta_paid", query)
        self.assertIn("purchase_revenue_in_usd", query)
        self.assertIn("AS first_page_view_step", query)
        self.assertIn("AS first_view_item_step", query)
        self.assertIn("AS first_add_to_cart_step", query)
        self.assertIn("AS begin_checkout_step", query)
        self.assertIn("AS purchase_step", query)
        self.assertIn("AS session_start_at", query)

    def test_row_follows_postgres_column_order(self) -> None:
        row = {
            "report_date": date(2026, 6, 12),
            "source_project_id": "merino-agent",
            "source_dataset_id": "analytics_370932876",
            "source_table": "merino-agent.analytics_370932876.events_20260612",
            "source_table_type": "finalized",
            "is_finalized": True,
            "user_pseudo_id": "2093605261.1781456874",
            "ga_session_id": 1781456874,
            "session_start_at": "2026-06-14T10:07:54-07:00",
            "device_category": "desktop",
            "device_operating_system": "Windows",
            "device_browser": "Chrome",
            "traffic_channel_group": "Paid Social",
            "traffic_source": "facebook",
            "traffic_medium": "paid_social",
            "traffic_campaign": "Zipcode-赛文-061426",
            "traffic_placement": "facebook_reels",
            "is_meta_paid": True,
            "session_event_count": 108,
            "session_seconds": 870.0,
            "session_minutes": 14.5,
            "seconds_to_purchase": 870.0,
            "minutes_to_purchase": 14.5,
            "purchase_revenue_usd": 123.45,
            "transaction_id": "order-123",
            "purchase_event_count": 1,
            "first_page_view_step": 3,
            "first_view_item_step": 21,
            "first_add_to_cart_step": 30,
            "begin_checkout_step": 105,
            "purchase_step": 108,
            "landing_page": "https://example.com/",
        }

        self.assertEqual(
            daily_purchaser_behavior.daily_purchaser_behavior_row(row),
            (
                date(2026, 6, 12),
                "merino-agent",
                "analytics_370932876",
                "merino-agent.analytics_370932876.events_20260612",
                "finalized",
                True,
                "2093605261.1781456874",
                1781456874,
                "2026-06-14T10:07:54-07:00",
                "desktop",
                "Windows",
                "Chrome",
                "Paid Social",
                "facebook",
                "paid_social",
                "Zipcode-赛文-061426",
                "facebook_reels",
                True,
                108,
                870.0,
                14.5,
                870.0,
                14.5,
                123.45,
                "order-123",
                1,
                3,
                21,
                30,
                105,
                108,
                "https://example.com/",
            ),
        )

    def test_upsert_conflict_keys_match_schema_constraints(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        schema_dir = repo / "metabase_schema" / "schema" / "merino-analytics" / "ga4"
        schema_sql = (schema_dir / "ga4_daily_purchaser_behavior.sql").read_text()
        traffic_schema_sql = (
            schema_dir / "ga4_daily_purchaser_behavior_traffic_columns.sql"
        ).read_text()
        source_schema_sql = (schema_dir / "ga4_source_status_columns.sql").read_text()

        self.assertIn(
            "ON ga4.daily_purchaser_behavior (report_date, user_pseudo_id, ga_session_id)",
            schema_sql,
        )
        self.assertIn("ADD COLUMN IF NOT EXISTS traffic_channel_group TEXT", traffic_schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS traffic_placement TEXT", traffic_schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS is_meta_paid BOOLEAN NOT NULL DEFAULT false", traffic_schema_sql)
        self.assertIn("ON ga4.daily_purchaser_behavior (report_date DESC, is_meta_paid)", traffic_schema_sql)
        self.assertIn("ALTER TABLE ga4.daily_purchaser_behavior", source_schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS source_table_type TEXT NOT NULL DEFAULT 'finalized'", source_schema_sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS is_finalized BOOLEAN NOT NULL DEFAULT true", source_schema_sql)
        self.assertEqual(
            daily_purchaser_behavior.DAILY_PURCHASER_BEHAVIOR_CONFLICT_COLUMNS,
            ("report_date", "user_pseudo_id", "ga_session_id"),
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

        daily_purchaser_behavior.upsert_daily_purchaser_behavior(
            [
                {
                    "report_date": date(2026, 6, 12),
                    "source_project_id": "merino-agent",
                    "source_dataset_id": "analytics_370932876",
                    "source_table": "events_20260612",
                    "source_table_type": "finalized",
                    "is_finalized": True,
                    "user_pseudo_id": "2093605261.1781456874",
                    "ga_session_id": 1781456874,
                    "session_start_at": None,
                    "device_category": "desktop",
                    "device_operating_system": "Windows",
                    "device_browser": "Chrome",
                    "traffic_channel_group": "Paid Social",
                    "traffic_source": "facebook",
                    "traffic_medium": "paid_social",
                    "traffic_campaign": "Zipcode-赛文-061426",
                    "traffic_placement": "facebook_reels",
                    "is_meta_paid": True,
                    "session_event_count": 108,
                    "session_seconds": 870.0,
                    "session_minutes": 14.5,
                    "seconds_to_purchase": 870.0,
                    "minutes_to_purchase": 14.5,
                    "purchase_revenue_usd": 123.45,
                    "transaction_id": "order-123",
                    "purchase_event_count": 1,
                    "first_page_view_step": 3,
                    "first_view_item_step": 21,
                    "first_add_to_cart_step": 30,
                    "begin_checkout_step": 105,
                    "purchase_step": 108,
                    "landing_page": "https://example.com/",
                }
            ],
            postgres_hook_factory=FakePostgresHook,
        )

        sql, rows = calls[0]
        self.assertIn("INSERT INTO ga4.daily_purchaser_behavior AS target", sql)
        self.assertIn(
            "ON CONFLICT (report_date, user_pseudo_id, ga_session_id) DO UPDATE",
            sql,
        )
        self.assertIn("update_count = target.update_count + 1", sql)
        self.assertIn(
            "target.purchase_revenue_usd IS DISTINCT FROM EXCLUDED.purchase_revenue_usd",
            sql,
        )
        self.assertIn("target.is_meta_paid IS DISTINCT FROM EXCLUDED.is_meta_paid", sql)
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()

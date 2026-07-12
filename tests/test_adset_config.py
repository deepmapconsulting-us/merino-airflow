from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.adset_config import (  # noqa: E402  # type: ignore[import-not-found]
    active_adsets_from_flat,
    adset_config_row_from_graph,
    budget_hash,
    canonical_targeting,
    config_hash,
    extract_targeting_columns,
    insert_budget_version,
    insert_config_version,
    sync_adset_budget_versions,
    sync_adset_config_versions,
    sync_adset_targeting_daily_snapshots,
)
from merino_meta_jobs.object_property import flatten_config_snapshot  # noqa: E402  # type: ignore[import-not-found]


SAMPLE_SNAPSHOT = {
    "accounts": {
        "act_111": {
            "id": "act_111",
            "campaigns": [
                {
                    "id": "camp_1",
                    "name": "Summer Sale",
                    "status": "ACTIVE",
                    "adsets": [
                        {
                            "id": "adset_active",
                            "name": "US Broad",
                            "status": "ACTIVE",
                            "campaign_id": "camp_1",
                            "ads": [],
                        },
                        {
                            "id": "adset_paused",
                            "name": "Paused",
                            "status": "PAUSED",
                            "campaign_id": "camp_1",
                            "ads": [],
                        },
                    ],
                },
                {
                    "id": "camp_2",
                    "name": "Paused Campaign",
                    "status": "PAUSED",
                    "adsets": [
                        {
                            "id": "adset_in_paused_campaign",
                            "name": "Should Skip",
                            "status": "ACTIVE",
                            "campaign_id": "camp_2",
                            "ads": [],
                        }
                    ],
                },
            ],
        }
    }
}

TARGETING_A = {
    "age_min": 25,
    "age_max": 65,
    "genders": [2],
    "geo_locations": {"countries": ["US"]},
    "targeting_automation": {"advantage_audience": 1},
}

TARGETING_B = {
    "age_min": 18,
    "age_max": 65,
    "genders": [1, 2],
    "geo_locations": {"countries": ["US", "CA"]},
}

TARGETING_WITH_INTERESTS = {
    "age_min": 21,
    "age_max": 55,
    "flexible_spec": [
        {
            "interests": [{"id": "6003139266461", "name": "Hiking"}],
            "behaviors": [{"id": "6002714898572", "name": "Engaged Shoppers"}],
        }
    ],
    "custom_audiences": [{"id": "aud_1", "name": "Purchasers"}],
    "excluded_custom_audiences": [{"id": "aud_2", "name": "Recent buyers"}],
}


class ConfigHashTest(unittest.TestCase):
    def test_hash_stable_for_key_order(self) -> None:
        first = {"age_min": 25, "age_max": 65, "genders": [2]}
        second = {"genders": [2], "age_max": 65, "age_min": 25}
        self.assertEqual(config_hash(first), config_hash(second))

    def test_hash_changes_when_targeting_changes(self) -> None:
        self.assertNotEqual(config_hash(TARGETING_A), config_hash(TARGETING_B))

    def test_canonical_targeting_is_sorted_json(self) -> None:
        payload = json.loads(canonical_targeting({"b": 1, "a": 2}))
        self.assertEqual(list(payload.keys()), ["a", "b"])


class ExtractTargetingColumnsTest(unittest.TestCase):
    def test_extracts_age_gender_geo_advantage(self) -> None:
        extracted = extract_targeting_columns(TARGETING_A)
        self.assertEqual(extracted["age_min"], 25)
        self.assertEqual(extracted["age_max"], 65)
        self.assertEqual(extracted["genders"], [2])
        self.assertEqual(extracted["geo_countries"], ["US"])
        self.assertTrue(extracted["advantage_audience"])
        self.assertEqual(extracted["audience_region"], "us_pacific")
        self.assertEqual(extracted["audience_timezone"], "America/Los_Angeles")
        self.assertEqual(extracted["audience_timezone_offset_hours"], 0)
        self.assertEqual(extracted["audience_region_weights"], {"us_pacific:country_fallback": 1})

    def test_extracts_weighted_region_from_geo_regions(self) -> None:
        extracted = extract_targeting_columns(
            {
                "geo_locations": {
                    "regions": [
                        {"key": "3847", "name": "California", "country": "US"},
                        {"key": "3848", "name": "Colorado", "country": "US"},
                        {"key": "3855", "name": "Idaho", "country": "US"},
                        {"key": "3887", "name": "Utah", "country": "US"},
                    ]
                }
            }
        )

        self.assertEqual(extracted["audience_region"], "us_mountain")
        self.assertEqual(extracted["audience_timezone"], "America/Denver")
        self.assertEqual(extracted["audience_timezone_offset_hours"], 1)
        self.assertEqual(extracted["audience_region_weights"], {"us_pacific": 1, "us_mountain": 3})

    def test_extracts_australia_region_from_geo_regions(self) -> None:
        extracted = extract_targeting_columns(
            {
                "geo_locations": {
                    "regions": [
                        {"key": "1000", "name": "New South Wales", "country": "AU"},
                        {"key": "1001", "name": "Victoria", "country": "AU"},
                        {"key": "1002", "name": "Western Australia", "country": "AU"},
                    ]
                }
            }
        )

        self.assertEqual(extracted["audience_region"], "au_eastern")
        self.assertEqual(extracted["audience_timezone"], "Australia/Sydney")
        self.assertEqual(extracted["audience_timezone_offset_hours"], 17)
        self.assertEqual(extracted["audience_region_weights"], {"au_eastern": 2, "au_western": 1})

    def test_extract_handles_missing_targeting(self) -> None:
        extracted = extract_targeting_columns(None)
        self.assertIsNone(extracted["age_min"])
        self.assertIsNone(extracted["advantage_audience"])
        self.assertIsNone(extracted["audience_region"])
        self.assertEqual(extracted["audience_region_weights"], {})

    def test_extracts_interest_and_audience_json(self) -> None:
        extracted = extract_targeting_columns(TARGETING_WITH_INTERESTS)
        self.assertEqual(extracted["flexible_spec"], TARGETING_WITH_INTERESTS["flexible_spec"])
        self.assertEqual(
            extracted["interests"],
            [{"id": "6003139266461", "name": "Hiking"}],
        )
        self.assertEqual(
            extracted["behaviors"],
            [{"id": "6002714898572", "name": "Engaged Shoppers"}],
        )
        self.assertEqual(extracted["custom_audiences"], TARGETING_WITH_INTERESTS["custom_audiences"])
        self.assertEqual(
            extracted["excluded_custom_audiences"],
            TARGETING_WITH_INTERESTS["excluded_custom_audiences"],
        )


class ActiveAdsetFilterTest(unittest.TestCase):
    def test_active_adsets_from_flat_filters_hierarchy(self) -> None:
        flat = flatten_config_snapshot(SAMPLE_SNAPSHOT)
        active = active_adsets_from_flat(flat)
        adset_ids = {row["adset_id"] for row in active}
        self.assertEqual(adset_ids, {"adset_active"})


class AdsetConfigRowTest(unittest.TestCase):
    def test_row_from_graph_maps_fields(self) -> None:
        observed_at = datetime(2026, 5, 30, 18, 0, tzinfo=timezone.utc)
        row = adset_config_row_from_graph(
            {
                "id": "adset_active",
                "campaign_id": "camp_1",
                "status": "ACTIVE",
                "daily_budget": "2500",
                "lifetime_budget": "10000",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "billing_event": "IMPRESSIONS",
                "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
                "targeting": TARGETING_A,
            },
            source_account_id="act_111",
            observed_at=observed_at,
            config_snapshot_uri="gs://bucket/snapshot.json",
        )
        self.assertEqual(row["adset_id"], "adset_active")
        self.assertEqual(row["campaign_id"], "camp_1")
        self.assertEqual(row["age_min"], 25)
        self.assertTrue(row["advantage_audience"])
        self.assertEqual(row["config_hash"], config_hash(TARGETING_A))
        self.assertEqual(row["targeting_hash"], config_hash(TARGETING_A))
        self.assertEqual(row["audience_region"], "us_pacific")
        self.assertEqual(row["audience_timezone"], "America/Los_Angeles")
        self.assertEqual(row["daily_budget"], 2500)
        self.assertEqual(row["lifetime_budget"], 10000)
        self.assertEqual(row["optimization_goal"], "OFFSITE_CONVERSIONS")
        self.assertEqual(row["budget_hash"], budget_hash(row))


class InsertConfigVersionTest(unittest.TestCase):
    def _mock_conn(self, *, current_hash: str | None, next_version: int = 1) -> MagicMock:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        def execute_side_effect(sql: str, params=None) -> None:
            if "WHERE valid_to IS NULL" in sql and "SELECT adset_id, config_hash" in sql:
                if current_hash is None:
                    cursor.fetchall.return_value = []
                else:
                    cursor.fetchall.return_value = [("adset_1", current_hash)]
            elif "COALESCE(MAX(config_version)" in sql:
                cursor.fetchone.return_value = (next_version,)
            else:
                cursor.rowcount = 1

        cursor.execute.side_effect = execute_side_effect
        return conn

    def test_skips_insert_when_hash_unchanged(self) -> None:
        conn = self._mock_conn(current_hash=config_hash(TARGETING_A))
        current_hashes = {"adset_1": config_hash(TARGETING_A)}
        row = {
            "adset_id": "adset_1",
            "config_hash": config_hash(TARGETING_A),
            "observed_at": datetime.now(timezone.utc),
        }
        inserted = insert_config_version(conn, row, current_hashes=current_hashes)
        self.assertFalse(inserted)

    def test_inserts_when_hash_changed(self) -> None:
        conn = self._mock_conn(current_hash=config_hash(TARGETING_A), next_version=2)
        current_hashes = {"adset_1": config_hash(TARGETING_A)}
        row = {
            "adset_id": "adset_1",
            "campaign_id": "camp_1",
            "source_account_id": "act_111",
            "company": "merino",
            "platform": "meta",
            "source": "facebook",
            "observed_at": datetime(2026, 5, 30, 19, 0, tzinfo=timezone.utc),
            "observed_date": datetime(2026, 5, 30).date(),
            "valid_from": datetime(2026, 5, 30, 19, 0, tzinfo=timezone.utc),
            "valid_to": None,
            "config_hash": config_hash(TARGETING_B),
            "targeting": TARGETING_B,
            "age_min": 18,
            "age_max": 65,
            "genders": [1, 2],
            "advantage_audience": None,
            "geo_countries": ["US", "CA"],
            "config_snapshot_uri": "gs://bucket/snapshot.json",
        }
        inserted = insert_config_version(conn, row, current_hashes=current_hashes)
        self.assertTrue(inserted)
        self.assertEqual(current_hashes["adset_1"], config_hash(TARGETING_B))

    def test_sync_counts_insert_and_skip(self) -> None:
        conn = self._mock_conn(current_hash=None)
        cursor = conn.cursor.return_value.__enter__.return_value

        def execute_side_effect(sql: str, params=None) -> None:
            if "SELECT adset_id, config_hash" in sql:
                cursor.fetchall.return_value = []
            elif "COALESCE(MAX(config_version)" in sql:
                cursor.fetchone.return_value = (1,)
            else:
                cursor.rowcount = 1

        cursor.execute.side_effect = execute_side_effect

        observed_at = datetime(2026, 5, 30, 19, 0, tzinfo=timezone.utc)
        rows = [
            adset_config_row_from_graph(
                {"id": "adset_1", "campaign_id": "camp_1", "targeting": TARGETING_A},
                source_account_id="act_111",
                observed_at=observed_at,
            ),
            adset_config_row_from_graph(
                {"id": "adset_1", "campaign_id": "camp_1", "targeting": TARGETING_A},
                source_account_id="act_111",
                observed_at=observed_at,
            ),
        ]
        counts = sync_adset_config_versions(conn, rows)
        self.assertEqual(counts["fetched"], 2)
        self.assertEqual(counts["inserted"], 1)
        self.assertEqual(counts["skipped"], 1)


class TargetingDailySnapshotTest(unittest.TestCase):
    def test_sync_upserts_daily_partition_even_when_targeting_unchanged(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        observed_at = datetime(2026, 5, 30, 19, 0, tzinfo=timezone.utc)
        rows = [
            adset_config_row_from_graph(
                {"id": "adset_1", "campaign_id": "camp_1", "targeting": TARGETING_A},
                source_account_id="act_111",
                observed_at=observed_at,
            ),
            adset_config_row_from_graph(
                {"id": "adset_1", "campaign_id": "camp_1", "targeting": TARGETING_A},
                source_account_id="act_111",
                observed_at=observed_at,
            ),
        ]

        counts = sync_adset_targeting_daily_snapshots(conn, rows)

        self.assertEqual(counts["daily_upserted"], 2)
        self.assertEqual(cursor.execute.call_count, 2)
        sql = cursor.execute.call_args_list[0].args[0]
        self.assertIn("ON CONFLICT (observed_date, adset_id) DO UPDATE", sql)
        self.assertIn("update_count = marketing.meta_adset_targeting_daily_snapshot.update_count + 1", sql)


class BudgetVersionTest(unittest.TestCase):
    def _mock_conn(self, *, current_hash: str | None, next_version: int = 1) -> MagicMock:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        def execute_side_effect(sql: str, params=None) -> None:
            if "SELECT adset_id, budget_hash" in sql:
                if current_hash is None:
                    cursor.fetchall.return_value = []
                else:
                    cursor.fetchall.return_value = [("adset_1", current_hash)]
            elif "COALESCE(MAX(budget_version)" in sql:
                cursor.fetchone.return_value = (next_version,)
            else:
                cursor.rowcount = 1

        cursor.execute.side_effect = execute_side_effect
        return conn

    def test_skips_insert_when_budget_hash_unchanged(self) -> None:
        row = adset_config_row_from_graph(
            {
                "id": "adset_1",
                "campaign_id": "camp_1",
                "daily_budget": "2500",
                "lifetime_budget": "10000",
                "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
                "targeting": TARGETING_A,
            },
            source_account_id="act_111",
            observed_at=datetime(2026, 5, 30, 19, 0, tzinfo=timezone.utc),
        )
        conn = self._mock_conn(current_hash=row["budget_hash"])

        inserted = insert_budget_version(conn, row, current_hashes={"adset_1": row["budget_hash"]})

        self.assertFalse(inserted)

    def test_inserts_when_budget_hash_changed(self) -> None:
        row = adset_config_row_from_graph(
            {
                "id": "adset_1",
                "campaign_id": "camp_1",
                "daily_budget": "5000",
                "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
                "targeting": TARGETING_A,
            },
            source_account_id="act_111",
            observed_at=datetime(2026, 5, 30, 19, 0, tzinfo=timezone.utc),
        )
        conn = self._mock_conn(current_hash="old", next_version=2)
        current_hashes = {"adset_1": "old"}

        inserted = insert_budget_version(conn, row, current_hashes=current_hashes)

        self.assertTrue(inserted)
        self.assertEqual(current_hashes["adset_1"], row["budget_hash"])

    def test_sync_counts_budget_insert_and_skip(self) -> None:
        conn = self._mock_conn(current_hash=None)
        observed_at = datetime(2026, 5, 30, 19, 0, tzinfo=timezone.utc)
        rows = [
            adset_config_row_from_graph(
                {"id": "adset_1", "campaign_id": "camp_1", "daily_budget": "2500"},
                source_account_id="act_111",
                observed_at=observed_at,
            ),
            adset_config_row_from_graph(
                {"id": "adset_1", "campaign_id": "camp_1", "daily_budget": "2500"},
                source_account_id="act_111",
                observed_at=observed_at,
            ),
        ]

        counts = sync_adset_budget_versions(conn, rows)

        self.assertEqual(counts["budget_fetched"], 2)
        self.assertEqual(counts["budget_inserted"], 1)
        self.assertEqual(counts["budget_skipped"], 1)


if __name__ == "__main__":
    unittest.main()

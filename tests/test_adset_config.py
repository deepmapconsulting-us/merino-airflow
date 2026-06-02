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
    canonical_targeting,
    config_hash,
    extract_targeting_columns,
    insert_config_version,
    sync_adset_config_versions,
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

    def test_extract_handles_missing_targeting(self) -> None:
        extracted = extract_targeting_columns(None)
        self.assertIsNone(extracted["age_min"])
        self.assertIsNone(extracted["advantage_audience"])


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


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.account_snapshot import ADSET_LIST_FIELDS, _campaign_tree  # noqa: E402


class AccountSnapshotTest(unittest.TestCase):
    def test_adset_snapshot_includes_budget_and_targeting_summary(self) -> None:
        tree = _campaign_tree(
            ads=[],
            campaigns_by_id={
                "camp_1": {
                    "id": "camp_1",
                    "status": "ACTIVE",
                }
            },
            adsets_by_id={
                "adset_1": {
                    "id": "adset_1",
                    "campaign_id": "camp_1",
                    "status": "ACTIVE",
                    "daily_budget": "10000",
                    "targeting": {
                        "age_min": 30,
                        "age_max": 65,
                        "genders": [1, 2],
                        "geo_locations": {"countries": ["US"]},
                        "targeting_automation": {"advantage_audience": 1},
                    },
                }
            },
        )

        adset = tree[0]["adsets"][0]

        self.assertEqual(adset["daily_budget"], "10000")
        self.assertEqual(adset["targeting"]["age_min"], 30)
        self.assertEqual(
            adset["targeting_summary"],
            {
                "age_min": 30,
                "age_max": 65,
                "genders": [1, 2],
                "advantage_audience": True,
                "geo_countries": ["US"],
            },
        )

    def test_adset_list_fields_fetch_targeting_and_budget(self) -> None:
        self.assertIn("targeting", ADSET_LIST_FIELDS)
        self.assertIn("daily_budget", ADSET_LIST_FIELDS)
        self.assertIn("lifetime_budget", ADSET_LIST_FIELDS)


if __name__ == "__main__":
    unittest.main()

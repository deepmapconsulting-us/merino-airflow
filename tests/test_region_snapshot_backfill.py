from __future__ import annotations

import sys
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.region_snapshot_backfill import plan_region_backfill  # noqa: E402


class RegionSnapshotBackfillTest(unittest.TestCase):
    def test_entity_windows_use_created_at_not_schedule_fields(self) -> None:
        plan = plan_region_backfill(
            campaigns=[
                {
                    "campaign_id": "campaign_1",
                    "source_account_id": "4157857287789311",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "start_time": "2026-01-02T00:00:00+00:00",
                    "stop_time": "2026-01-03T23:59:59+00:00",
                }
            ],
            adsets=[
                {
                    "adset_id": "adset_1",
                    "campaign_id": "campaign_1",
                    "source_account_id": "4157857287789311",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "start_time": "2026-01-03T00:00:00+00:00",
                    "end_time": "2026-01-04T00:00:00+00:00",
                }
            ],
            ads=[
                {
                    "ad_id": "ad_1",
                    "adset_id": "adset_1",
                    "campaign_id": "campaign_1",
                    "source_account_id": "4157857287789311",
                    "created_at": "2026-01-03T00:00:00+00:00",
                    "creative_id": "creative_1",
                }
            ],
            existing={},
            start_date="2026-01-01",
            end_date="2026-01-05",
        )

        self.assertEqual(
            [batch["report_date"] for batch in plan["campaign_batches"]],
            ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
        )
        self.assertEqual(
            [batch["report_date"] for batch in plan["adset_batches"]],
            ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
        )
        self.assertEqual(
            [batch["report_date"] for batch in plan["ad_batches"]],
            ["2026-01-03", "2026-01-04", "2026-01-05"],
        )
        self.assertEqual(plan["ad_batches"][0]["campaign"]["adsets"][0]["ads"][0]["creative_id"], "creative_1")

    def test_missing_created_at_falls_back_to_requested_start_date(self) -> None:
        plan = plan_region_backfill(
            campaigns=[
                {
                    "campaign_id": "campaign_1",
                    "source_account_id": "act_1",
                    "created_at": None,
                }
            ],
            adsets=[],
            ads=[],
            existing={},
            start_date="2026-01-01",
            end_date="2026-01-02",
            levels=["campaign"],
        )

        self.assertEqual(
            [batch["report_date"] for batch in plan["campaign_batches"]],
            ["2026-01-01", "2026-01-02"],
        )

    def test_skip_existing_unless_force(self) -> None:
        campaigns = [
            {
                "campaign_id": "campaign_1",
                "source_account_id": "act_1",
                "created_at": "2026-01-01T00:00:00+00:00",
                "start_time": None,
                "stop_time": None,
            }
        ]
        existing = {"campaign": {("2026-01-01", "act_1", "campaign_1")}}

        skipped = plan_region_backfill(
            campaigns=campaigns,
            adsets=[],
            ads=[],
            existing=existing,
            start_date="2026-01-01",
            end_date="2026-01-01",
            levels=["campaign"],
            force=False,
        )
        forced = plan_region_backfill(
            campaigns=campaigns,
            adsets=[],
            ads=[],
            existing=existing,
            start_date="2026-01-01",
            end_date="2026-01-01",
            levels=["campaign"],
            force=True,
        )

        self.assertEqual(skipped["campaign_batches"], [])
        self.assertEqual(len(forced["campaign_batches"]), 1)

    def test_chunks_campaign_ids_by_date_and_account(self) -> None:
        campaigns = [
            {
                "campaign_id": f"campaign_{index}",
                "source_account_id": "act_1",
                "created_at": "2026-01-01T00:00:00+00:00",
            }
            for index in range(3)
        ]

        plan = plan_region_backfill(
            campaigns=campaigns,
            adsets=[],
            ads=[],
            existing={},
            start_date="2026-01-01",
            end_date="2026-01-01",
            levels=["campaign"],
            campaign_chunk_size=2,
        )

        self.assertEqual(len(plan["campaign_batches"]), 2)
        self.assertEqual(plan["campaign_batches"][0]["campaign_ids"], ["campaign_0", "campaign_1"])
        self.assertEqual(plan["campaign_batches"][1]["campaign_ids"], ["campaign_2"])


if __name__ == "__main__":
    unittest.main()

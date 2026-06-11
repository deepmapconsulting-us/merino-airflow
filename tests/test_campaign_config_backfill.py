from __future__ import annotations

import sys
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs import campaign_config_backfill as backfill  # noqa: E402


class CampaignConfigBackfillTest(unittest.TestCase):
    def test_parse_config_snapshot_object_name(self) -> None:
        ref = backfill.parse_config_snapshot_object_name(
            "facebook_campaign_config_update/2026-01-15/20260115T020000-0800/snapshot.json",
            prefix="facebook_campaign_config_update",
        )
        self.assertIsNotNone(ref)
        assert ref is not None
        self.assertEqual(ref.run_date, "2026-01-15")
        self.assertEqual(ref.run_datetime, "20260115T020000-0800")

    def test_scan_config_snapshots_builds_inventory(self) -> None:
        refs = backfill.iter_snapshot_refs_from_names(
            [
                "facebook_campaign_config_update/2026-01-01/20260101T020000-0800/snapshot.json",
                "facebook_campaign_config_update/2026-01-02/20260102T020000-0800/snapshot.json",
            ]
        )
        payloads = {
            refs[0].uri: {
                "generated_at": "2026-01-01T10:00:00+00:00",
                "accounts": {
                    "act_1": {
                        "id": "act_1",
                        "campaigns": [
                            {
                                "id": "campaign_1",
                                "status": "ACTIVE",
                                "created_at": "2025-12-01T00:00:00+0000",
                                "adsets": [
                                    {
                                        "id": "adset_1",
                                        "status": "ACTIVE",
                                        "created_at": "2025-12-02T00:00:00+0000",
                                        "ads": [{"id": "ad_1", "status": "ACTIVE"}],
                                    }
                                ],
                            }
                        ],
                    }
                },
            },
            refs[1].uri: {
                "generated_at": "2026-01-02T10:00:00+00:00",
                "accounts": {
                    "act_1": {
                        "id": "act_1",
                        "campaigns": [
                            {
                                "id": "campaign_1",
                                "status": "PAUSED",
                                "created_at": "2025-12-01T00:00:00+0000",
                                "adsets": [
                                    {
                                        "id": "adset_1",
                                        "status": "PAUSED",
                                        "created_at": "2025-12-02T00:00:00+0000",
                                        "ads": [{"id": "ad_1", "status": "PAUSED"}],
                                    },
                                    {
                                        "id": "adset_2",
                                        "status": "ACTIVE",
                                        "created_at": "2026-01-02T00:00:00+0000",
                                        "ads": [],
                                    },
                                ],
                            },
                            {
                                "id": "campaign_2",
                                "status": "ACTIVE",
                                "adsets": [],
                            },
                        ],
                    }
                },
            },
        }

        class FakeStorageClient:
            def list_blobs(self, bucket: str, prefix: str):  # noqa: ARG002
                return [type("Blob", (), {"name": ref.uri.removeprefix("gs://airflow-run-us-west2/")})() for ref in refs]

        def read_json(_client, uri: str) -> dict:
            return payloads[uri]

        report = backfill.scan_config_snapshots(FakeStorageClient(), read_json)

        self.assertEqual(report["snapshot_count"], 2)
        self.assertEqual(report["unique_campaigns"], 2)
        self.assertEqual(report["unique_adsets"], 2)
        self.assertEqual(report["unique_ads"], 1)
        self.assertEqual(report["snapshot_summaries"][0]["campaign_count"], 1)
        self.assertEqual(report["snapshot_summaries"][1]["campaign_count"], 2)
        self.assertEqual(report["snapshot_summaries"][1]["adset_count"], 2)

        campaigns = {row["object_id"]: row for row in report["inventory"]["campaigns"]}
        self.assertEqual(campaigns["campaign_1"]["snapshot_appearances"], 2)
        self.assertEqual(campaigns["campaign_1"]["first_active_observed_at"], "2026-01-01T10:00:00+00:00")
        self.assertEqual(campaigns["campaign_1"]["last_active_observed_at"], "2026-01-01T10:00:00+00:00")
        self.assertEqual(campaigns["campaign_1"]["last_status"], "PAUSED")
        self.assertIn("ACTIVE", campaigns["campaign_1"]["statuses_seen"])
        self.assertIn("PAUSED", campaigns["campaign_1"]["statuses_seen"])

        adsets = {row["object_id"]: row for row in report["inventory"]["adsets"]}
        self.assertEqual(adsets["adset_2"]["first_observed_at"], "2026-01-02T10:00:00+00:00")


if __name__ == "__main__":
    unittest.main()

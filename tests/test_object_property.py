from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.object_property import (  # noqa: E402  # type: ignore[import-not-found]
    ad_row_from_graph,
    adset_row_from_graph,
    campaign_row_from_graph,
    detail_rows_for_new_ids,
    flatten_config_snapshot,
    stub_rows_from_metrics,
)


SAMPLE_SNAPSHOT = {
    "accounts": {
        "act_111": {
            "id": "act_111",
            "campaigns": [
                {
                    "id": "camp_1",
                    "name": "Summer Sale",
                    "status": "ACTIVE",
                    "objective": "OUTCOME_SALES",
                    "created_at": "2026-01-01T00:00:00+0000",
                    "updated_at": "2026-02-01T00:00:00+0000",
                    "adsets": [
                        {
                            "id": "adset_1",
                            "name": "US Broad",
                            "status": "ACTIVE",
                            "campaign_id": "camp_1",
                            "created_at": "2026-01-02T00:00:00+0000",
                            "updated_at": "2026-02-02T00:00:00+0000",
                            "ads": [
                                {
                                    "id": "ad_1",
                                    "name": "Video A",
                                    "status": "ACTIVE",
                                    "adset_id": "adset_1",
                                    "campaign_id": "camp_1",
                                    "creative": {"id": "cr_1"},
                                    "created_at": "2026-01-03T00:00:00+0000",
                                    "updated_at": "2026-02-03T00:00:00+0000",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    }
}


class FlattenConfigSnapshotTest(unittest.TestCase):
    def test_flatten_config_snapshot_hierarchy_and_creative(self) -> None:
        flat = flatten_config_snapshot(SAMPLE_SNAPSHOT)
        self.assertEqual(len(flat["campaigns"]), 1)
        self.assertEqual(len(flat["adsets"]), 1)
        self.assertEqual(len(flat["ads"]), 1)
        self.assertEqual(flat["campaigns"][0]["campaign_id"], "camp_1")
        self.assertEqual(flat["campaigns"][0]["objective"], "OUTCOME_SALES")
        self.assertEqual(flat["adsets"][0]["adset_id"], "adset_1")
        self.assertEqual(flat["ads"][0]["ad_id"], "ad_1")
        self.assertEqual(flat["ads"][0]["creative_id"], "cr_1")
        self.assertEqual(flat["ads"][0]["source_account_id"], "act_111")


class RowBuilderTest(unittest.TestCase):
    def test_campaign_row_from_graph_maps_detail_fields(self) -> None:
        row = campaign_row_from_graph(
            {
                "id": "camp_1",
                "name": "Summer Sale",
                "status": "ACTIVE",
                "objective": "OUTCOME_SALES",
                "daily_budget": "10000",
                "lifetime_budget": None,
                "buying_type": "AUCTION",
                "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
                "start_time": "2026-01-01T08:00:00-0800",
                "created_time": "2026-01-01T00:00:00+0000",
                "updated_time": "2026-02-01T00:00:00+0000",
            },
            source_account_id="act_111",
            config_snapshot_uri="gs://bucket/snap.json",
        )
        self.assertEqual(row[0], "camp_1")
        self.assertEqual(row[1], "act_111")
        self.assertEqual(row[5], "Summer Sale")
        self.assertEqual(row[8], 10000)
        self.assertEqual(row[-1], "gs://bucket/snap.json")

    def test_adset_row_from_graph_maps_detail_fields(self) -> None:
        row = adset_row_from_graph(
            {
                "id": "adset_1",
                "campaign_id": "camp_1",
                "name": "US Broad",
                "status": "ACTIVE",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "billing_event": "IMPRESSIONS",
            },
            source_account_id="act_111",
        )
        self.assertEqual(row[0], "adset_1")
        self.assertEqual(row[1], "camp_1")
        self.assertEqual(row[6], "US Broad")

    def test_ad_row_from_graph_maps_creative_id(self) -> None:
        row = ad_row_from_graph(
            {
                "id": "ad_1",
                "adset_id": "adset_1",
                "campaign_id": "camp_1",
                "name": "Video A",
                "status": "ACTIVE",
                "creative": {"id": "cr_1"},
            },
            source_account_id="act_111",
        )
        self.assertEqual(row[0], "ad_1")
        self.assertEqual(row[3], "cr_1")


class DetailRowsForNewIdsTest(unittest.TestCase):
    @patch("merino_meta_jobs.object_property.fetch_object_detail")
    def test_detail_fetch_called_once_for_new_ad(self, mock_detail: MagicMock) -> None:
        mock_detail.return_value = {
            "id": "ad_new",
            "adset_id": "adset_1",
            "campaign_id": "camp_1",
            "name": "New Ad",
            "status": "ACTIVE",
            "creative": {"id": "cr_new"},
        }
        flat = flatten_config_snapshot(
            {
                "accounts": {
                    "act_111": {
                        "id": "act_111",
                        "campaigns": [
                            {
                                "id": "camp_1",
                                "adsets": [
                                    {
                                        "id": "adset_1",
                                        "campaign_id": "camp_1",
                                        "ads": [{"id": "ad_new", "adset_id": "adset_1", "campaign_id": "camp_1"}],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )
        client = MagicMock()
        existing = {"campaigns": {"camp_1"}, "adsets": {"adset_1"}, "ads": set()}
        rows = detail_rows_for_new_ids(client, flat, existing, config_snapshot_uri="gs://x")
        mock_detail.assert_called_once_with(client, "ad", "ad_new")
        self.assertEqual(len(rows["ads"]), 1)
        self.assertEqual(rows["ads"][0][0], "ad_new")
        self.assertEqual(rows["ads"][0][3], "cr_new")


class StubRowsFromMetricsTest(unittest.TestCase):
    def test_stub_rows_from_metrics_executes_three_inserts(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.rowcount = 2
        conn.cursor.return_value.__enter__.return_value = cursor
        counts = stub_rows_from_metrics(conn)
        self.assertEqual(cursor.execute.call_count, 3)
        sql = " ".join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertIn("meta_ad_hourly_metric", sql)
        self.assertIn("(unknown)", sql)
        self.assertEqual(counts["campaigns"], 2)
        self.assertEqual(counts["adsets"], 2)
        self.assertEqual(counts["ads"], 2)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs import traffic  # noqa: E402  # type: ignore[import-not-found]


class DailyTrafficSnapshotTest(unittest.TestCase):
    def test_grouped_daily_snapshot_calls_use_id_filters(self) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []

        class FakeMetaGraphClient:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token

            def get_all(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
                calls.append((endpoint, params))
                return [{"campaign_id": "campaign_1"}]

        original_client = traffic.MetaGraphClient
        traffic.MetaGraphClient = FakeMetaGraphClient  # type: ignore[assignment]
        try:
            traffic.campaign_daily_snapshot(
                "token",
                "4157857287789311",
                ["campaign_1", "campaign_2"],
                "2026-05-20",
            )
            traffic.adset_daily_snapshot(
                "token",
                "act_4157857287789311",
                "campaign_1",
                ["adset_1", "adset_2"],
                "2026-05-20",
            )
            traffic.ad_daily_snapshot(
                "token",
                "act_4157857287789311",
                "campaign_1",
                ["ad_1", "ad_2"],
                "2026-05-20",
            )
        finally:
            traffic.MetaGraphClient = original_client

        self.assertEqual(calls[0][0], "act_4157857287789311/insights")
        self.assertEqual(calls[0][1]["level"], "campaign")
        self.assertEqual(
            calls[0][1]["filtering"],
            [{"field": "campaign.id", "operator": "IN", "value": ["campaign_1", "campaign_2"]}],
        )
        self.assertEqual(calls[1][0], "campaign_1/insights")
        self.assertEqual(calls[1][1]["level"], "adset")
        self.assertEqual(
            calls[1][1]["filtering"],
            [{"field": "adset.id", "operator": "IN", "value": ["adset_1", "adset_2"]}],
        )
        self.assertEqual(calls[2][0], "campaign_1/insights")
        self.assertEqual(calls[2][1]["level"], "ad")
        self.assertEqual(
            calls[2][1]["filtering"],
            [{"field": "ad.id", "operator": "IN", "value": ["ad_1", "ad_2"]}],
        )

    def test_insight_metric_values_extracts_scalars_and_json_payloads(self) -> None:
        values = traffic.insight_metric_values(
            {
                "spend": "12.34",
                "impressions": "100",
                "clicks": "8",
                "unique_clicks": "7",
                "ctr": "8.0",
                "actions": [
                    {"action_type": "link_click", "value": "5"},
                    {"action_type": "purchase", "value": "2"},
                    {"action_type": "landing_page_view", "value": "3"},
                ],
                "action_values": [{"action_type": "purchase", "value": "88.00"}],
                "cost_per_action_type": [{"action_type": "purchase", "value": "6.17"}],
                "conversions": [{"action_type": "purchase", "value": "2"}],
                "video_avg_time_watched_actions": [{"action_type": "video_view", "value": "4"}],
            }
        )

        self.assertEqual(values["spend"], 12.34)
        self.assertEqual(values["impressions"], 100)
        self.assertEqual(values["clicks"], 8)
        self.assertEqual(values["unique_clicks"], 7)
        self.assertEqual(values["link_clicks"], 5)
        self.assertEqual(values["landing_page_views"], 3)
        self.assertEqual(values["results"], 2)
        self.assertEqual(values["cost_per_result"], 6.17)
        self.assertEqual(values["actions"], {"landing_page_view": 3, "link_click": 5, "purchase": 2})
        self.assertEqual(values["action_values"], {"purchase": 88})
        self.assertEqual(values["cost_per_action_type"], {"purchase": 6.17})
        self.assertEqual(values["conversions"], {"purchase": 2})
        self.assertEqual(values["video_avg_time_watched_actions"], {"video_view": 4})

    def test_schema_and_dag_use_daily_snapshot_upsert_keys(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        schema_sql = (repo.parents[0] / "metabase_schema" / "schema" / "meta_traffic_snapshot.sql").read_text()
        dag_py = (repo / "dags" / "meta_traffic_snapshot.py").read_text()

        self.assertNotIn("snapshot_run_id,\n        source_account_id", schema_sql)
        self.assertIn("report_date,\n        source_account_id", schema_sql)
        self.assertIn("ON CONFLICT ({conflict_target}) DO UPDATE", dag_py)
        self.assertIn("update_count = target.update_count + 1", dag_py)
        self.assertIn('f"target.{column} IS DISTINCT FROM EXCLUDED.{column}"', dag_py)


if __name__ == "__main__":
    unittest.main()

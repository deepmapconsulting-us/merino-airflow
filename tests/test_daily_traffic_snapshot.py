from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))
DAGS_PATH = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_PATH))

from meta_status import ACTIVE_STATUS, HYBRID_STATUS, NOT_ACTIVE_STATUS, ConfigStatusMap, status_from_checks  # noqa: E402
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

    def test_campaign_region_daily_snapshot_uses_region_breakdown(self) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []

        class FakeMetaGraphClient:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token

            def get_all(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
                calls.append((endpoint, params))
                return [
                    {
                        "campaign_id": "campaign_1",
                        "region": "California",
                    }
                ]

        original_client = traffic.MetaGraphClient
        traffic.MetaGraphClient = FakeMetaGraphClient  # type: ignore[assignment]
        try:
            traffic.campaign_region_daily_snapshot(
                "token",
                "4157857287789311",
                ["campaign_1", "campaign_2"],
                "2026-05-20",
            )
        finally:
            traffic.MetaGraphClient = original_client

        self.assertEqual(calls[0][0], "act_4157857287789311/insights")
        self.assertEqual(calls[0][1]["level"], "campaign")
        self.assertEqual(calls[0][1]["breakdowns"], "region")
        self.assertEqual(
            calls[0][1]["filtering"],
            [{"field": "campaign.id", "operator": "IN", "value": ["campaign_1", "campaign_2"]}],
        )

    def test_region_insights_require_region_dimension(self) -> None:
        insights = [
            {"campaign_id": "campaign_1", "region": "California"},
            {"campaign_id": "campaign_1"},
            {"campaign_id": "campaign_2", "region": "Texas"},
        ]
        filtered = [
            insight
            for insight in insights
            if insight.get("campaign_id") and insight.get("region")
        ]
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]["region"], "California")

    def test_adset_region_daily_snapshot_uses_region_breakdown(self) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []

        class FakeMetaGraphClient:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token

            def get_all(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
                calls.append((endpoint, params))
                return [
                    {
                        "adset_id": "adset_1",
                        "region": "California",
                    }
                ]

        original_client = traffic.MetaGraphClient
        traffic.MetaGraphClient = FakeMetaGraphClient  # type: ignore[assignment]
        try:
            traffic.adset_region_daily_snapshot(
                "token",
                "act_4157857287789311",
                "campaign_1",
                ["adset_1", "adset_2"],
                "2026-05-20",
            )
        finally:
            traffic.MetaGraphClient = original_client

        self.assertEqual(calls[0][0], "campaign_1/insights")
        self.assertEqual(calls[0][1]["level"], "adset")
        self.assertEqual(calls[0][1]["breakdowns"], "region")
        self.assertEqual(
            calls[0][1]["filtering"],
            [{"field": "adset.id", "operator": "IN", "value": ["adset_1", "adset_2"]}],
        )

    def test_ad_region_daily_snapshot_uses_region_breakdown(self) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []

        class FakeMetaGraphClient:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token

            def get_all(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
                calls.append((endpoint, params))
                return [
                    {
                        "ad_id": "ad_1",
                        "region": "Texas",
                    }
                ]

        original_client = traffic.MetaGraphClient
        traffic.MetaGraphClient = FakeMetaGraphClient  # type: ignore[assignment]
        try:
            traffic.ad_region_daily_snapshot(
                "token",
                "act_4157857287789311",
                "campaign_1",
                ["ad_1", "ad_2"],
                "2026-05-20",
            )
        finally:
            traffic.MetaGraphClient = original_client

        self.assertEqual(calls[0][0], "campaign_1/insights")
        self.assertEqual(calls[0][1]["level"], "ad")
        self.assertEqual(calls[0][1]["breakdowns"], "region")
        self.assertEqual(
            calls[0][1]["filtering"],
            [{"field": "ad.id", "operator": "IN", "value": ["ad_1", "ad_2"]}],
        )

    def test_ad_gender_age_daily_snapshot_uses_age_gender_breakdown(self) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []

        class FakeMetaGraphClient:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token

            def get_all(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
                calls.append((endpoint, params))
                return [
                    {
                        "ad_id": "ad_1",
                        "age": "25-34",
                        "gender": "female",
                    }
                ]

        original_client = traffic.MetaGraphClient
        traffic.MetaGraphClient = FakeMetaGraphClient  # type: ignore[assignment]
        try:
            traffic.ad_gender_age_daily_snapshot(
                "token",
                "act_4157857287789311",
                "campaign_1",
                ["ad_1", "ad_2"],
                "2026-05-20",
            )
        finally:
            traffic.MetaGraphClient = original_client

        self.assertEqual(calls[0][0], "campaign_1/insights")
        self.assertEqual(calls[0][1]["level"], "ad")
        self.assertEqual(calls[0][1]["breakdowns"], "age,gender")
        self.assertEqual(
            calls[0][1]["filtering"],
            [{"field": "ad.id", "operator": "IN", "value": ["ad_1", "ad_2"]}],
        )

    def test_gender_age_insights_require_breakdown_dimensions(self) -> None:
        adset_by_ad_id = {"ad_1": {"id": "adset_1"}}
        insights = [
            {"ad_id": "ad_1", "age": "25-34", "gender": "female"},
            {"ad_id": "ad_1", "age": "25-34"},
            {"ad_id": "ad_2", "age": "18-24", "gender": "male"},
        ]
        filtered = [
            insight
            for insight in insights
            if insight.get("ad_id") in adset_by_ad_id
            and insight.get("age")
            and insight.get("gender")
        ]
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["gender"], "female")

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
        gender_age_sql = (
            repo.parents[0] / "metabase_schema" / "schema" / "meta_ad_gender_age_daily_snapshot.sql"
        ).read_text()
        region_sql = (
            repo.parents[0] / "metabase_schema" / "schema" / "meta_campaign_region_daily_snapshot.sql"
        ).read_text()
        adset_region_sql = (
            repo.parents[0] / "metabase_schema" / "schema" / "meta_adset_region_daily_snapshot.sql"
        ).read_text()
        ad_region_sql = (
            repo.parents[0] / "metabase_schema" / "schema" / "meta_ad_region_daily_snapshot.sql"
        ).read_text()
        dag_py = (repo / "dags" / "meta_traffic_snapshot.py").read_text()

        self.assertNotIn("snapshot_run_id,\n        source_account_id", schema_sql)
        self.assertIn("report_date,\n        source_account_id", schema_sql)
        self.assertIn("marketing.meta_ad_gender_age_daily_snapshot", gender_age_sql)
        self.assertIn("age,\n        gender,", gender_age_sql)
        self.assertIn("meta_ad_gender_age_daily_snapshot_unique_idx", gender_age_sql)
        self.assertIn("marketing.meta_campaign_region_daily_snapshot", region_sql)
        self.assertIn("region TEXT NOT NULL", region_sql)
        self.assertIn("meta_campaign_region_daily_snapshot_unique_idx", region_sql)
        self.assertIn("marketing.meta_adset_region_daily_snapshot", adset_region_sql)
        self.assertIn("meta_adset_region_daily_snapshot_unique_idx", adset_region_sql)
        self.assertIn("marketing.meta_ad_region_daily_snapshot", ad_region_sql)
        self.assertIn("meta_ad_region_daily_snapshot_unique_idx", ad_region_sql)
        self.assertIn("AD_GENDER_AGE_DAILY_TABLE", dag_py)
        self.assertIn("CAMPAIGN_REGION_DAILY_TABLE", dag_py)
        self.assertIn("ADSET_REGION_DAILY_TABLE", dag_py)
        self.assertIn("AD_REGION_DAILY_TABLE", dag_py)
        self.assertIn("pull_ad_gender_age_snapshots", dag_py)
        self.assertIn("pull_campaign_region_snapshots", dag_py)
        self.assertIn("pull_adset_region_snapshots", dag_py)
        self.assertIn("pull_ad_region_snapshots", dag_py)
        self.assertIn("ON CONFLICT ({conflict_target}) DO UPDATE", dag_py)
        self.assertIn("update_count = target.update_count + 1", dag_py)
        self.assertIn('f"target.{column} IS DISTINCT FROM EXCLUDED.{column}"', dag_py)

    def test_daily_active_status_can_be_hybrid(self) -> None:
        active_snapshot = {
            "accounts": {
                "act_1": {
                    "campaigns": [
                        {
                            "id": "campaign_1",
                            "status": "ACTIVE",
                            "adsets": [
                                {
                                    "id": "adset_1",
                                    "status": "ACTIVE",
                                    "ads": [{"id": "ad_1", "status": "ACTIVE"}],
                                }
                            ],
                        }
                    ]
                }
            }
        }
        inactive_snapshot = {
            "accounts": {
                "act_1": {
                    "campaigns": [
                        {
                            "id": "campaign_1",
                            "status": "PAUSED",
                            "adsets": [
                                {
                                    "id": "adset_1",
                                    "status": "ACTIVE",
                                    "ads": [{"id": "ad_1", "status": "ACTIVE"}],
                                }
                            ],
                        }
                    ]
                }
            }
        }

        active = ConfigStatusMap.from_snapshot(active_snapshot)
        inactive = ConfigStatusMap.from_snapshot(inactive_snapshot)

        self.assertEqual(status_from_checks([active.ad_status("campaign_1", "adset_1", "ad_1")]), ACTIVE_STATUS)
        self.assertEqual(status_from_checks([inactive.ad_status("campaign_1", "adset_1", "ad_1")]), NOT_ACTIVE_STATUS)
        self.assertEqual(
            status_from_checks(
                [
                    active.ad_status("campaign_1", "adset_1", "ad_1"),
                    inactive.ad_status("campaign_1", "adset_1", "ad_1"),
                ]
            ),
            HYBRID_STATUS,
        )


if __name__ == "__main__":
    unittest.main()

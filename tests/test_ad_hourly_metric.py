from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))
DAGS_PATH = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_PATH))

import meta_status  # noqa: E402
from meta_status import ACTIVE_STATUS, NOT_ACTIVE_STATUS, HourlyStatusResolver, load_config_snapshot  # noqa: E402
from merino_meta_jobs import traffic  # noqa: E402  # type: ignore[import-not-found]


class AdHourlyMetricTest(unittest.TestCase):
    def test_ad_hourly_snapshot_uses_hourly_breakdown_and_ad_filter(self) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []

        class FakeMetaGraphClient:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token

            def get_all(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
                calls.append((endpoint, params))
                return [{"ad_id": "ad_1"}]

        original_client = traffic.MetaGraphClient
        traffic.MetaGraphClient = FakeMetaGraphClient  # type: ignore[assignment]
        try:
            snapshot = traffic.ad_hourly_snapshot(
                "token",
                "4157857287789311",
                "campaign_1",
                ["ad_1", "ad_2", "ad_3", "ad_4", "ad_5"],
                "2026-05-20",
                page_limit=500,
            )
        finally:
            traffic.MetaGraphClient = original_client

        self.assertEqual(snapshot["account_id"], "act_4157857287789311")
        self.assertEqual(snapshot["campaign_id"], "campaign_1")
        self.assertEqual(calls[0][0], "campaign_1/insights")
        self.assertEqual(calls[0][1]["level"], "ad")
        self.assertEqual(calls[0][1]["breakdowns"], "hourly_stats_aggregated_by_advertiser_time_zone")
        self.assertEqual(
            calls[0][1]["filtering"],
            [
                {
                    "field": "ad.id",
                    "operator": "IN",
                    "value": ["ad_1", "ad_2", "ad_3", "ad_4", "ad_5"],
                }
            ],
        )

    def test_hourly_schema_and_dag_use_metric_hour_ad_key(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        schema_sql = (repo.parents[0] / "metabase_schema" / "schema" / "meta_hourly_metric.sql").read_text()
        dag_py = (repo / "dags" / "meta_ad_hourly_metric.py").read_text()

        self.assertNotIn("source_account_id TEXT", schema_sql)
        self.assertNotIn("company TEXT", schema_sql)
        self.assertIn("meta_ad_hourly_metric_metric_hour_ad_id_unique_idx", schema_sql)
        self.assertIn("metric_hour,\n        ad_id", schema_sql)
        self.assertIn('AD_HOURLY_CONFLICT_COLUMNS = ("metric_hour", "ad_id")', dag_py)
        self.assertIn("CREATE UNIQUE INDEX IF NOT EXISTS meta_ad_hourly_metric_metric_hour_ad_id_unique_idx", dag_py)
        self.assertIn('schedule="0 */2 * * *"', dag_py)
        self.assertIn("AD_BATCH_SIZE = 5", dag_py)
        self.assertIn("LOOKBACK_HOURS = 12", dag_py)
        self.assertIn("campaign_config_logical_date", dag_py)
        config_dag_py = (repo / "dags" / "facebook_campaign_config_update.py").read_text()
        self.assertIn('schedule="0 */2 * * *"', config_dag_py)
        property_dag_py = (repo / "dags" / "meta_object_property_sync.py").read_text()
        self.assertIn('schedule="0 */2 * * *"', property_dag_py)
        meta_gcs_py = (repo / "dags" / "meta_gcs.py").read_text()
        self.assertIn("REPORT_PARTITION_HOURS = 2", meta_gcs_py)
        self.assertIn("def campaign_config_logical_date", meta_gcs_py)
        self.assertIn("ON CONFLICT ({conflict_target}) DO UPDATE", dag_py)
        self.assertIn("update_count = target.update_count + 1", dag_py)

    def test_hourly_active_status_uses_same_two_hour_bucket(self) -> None:
        calls: list[tuple[str, int]] = []

        def fake_load_config_snapshot(storage_client, prefix: str, run_date: str, hour: int, redis_client=None):
            calls.append((run_date, hour))
            return {
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

        original_load = meta_status.load_config_snapshot
        meta_status.load_config_snapshot = fake_load_config_snapshot
        try:
            resolver = HourlyStatusResolver(None, "facebook_campaign_config_update", redis_client=object())
            self.assertEqual(
                resolver.ad_status("2026-05-23T13:15:00-07:00", "campaign_1", "adset_1", "ad_1"),
                ACTIVE_STATUS,
            )
            self.assertEqual(
                resolver.ad_status("2026-05-23T13:15:00-07:00", "campaign_1", "adset_1", "missing_ad"),
                NOT_ACTIVE_STATUS,
            )
        finally:
            meta_status.load_config_snapshot = original_load

        self.assertEqual(calls, [("2026-05-23", 12)])

    def test_config_loader_uses_closest_gcs_snapshot_on_exact_miss(self) -> None:
        class FakeRedis:
            def __init__(self) -> None:
                self.values: dict[str, str] = {}

            def get(self, key: str):
                return self.values.get(key)

            def set(self, key: str, value: str, ex: int) -> None:
                self.values[key] = value

        class FakeBlob:
            def __init__(self, object_name: str) -> None:
                self.object_name = object_name

            def download_as_text(self) -> str:
                if "/20260523T120000-0700/" in self.object_name:
                    return '{"accounts":{"act_1":{"campaigns":[]}}}'
                raise FileNotFoundError(self.object_name)

        class FakeBucket:
            def blob(self, object_name: str) -> FakeBlob:
                return FakeBlob(object_name)

        class FakeStorage:
            def bucket(self, bucket_name: str) -> FakeBucket:
                return FakeBucket()

        snapshot = load_config_snapshot(
            FakeStorage(),
            "facebook_campaign_config_update",
            "2026-05-23",
            8,
            FakeRedis(),
        )

        self.assertEqual(snapshot, {"accounts": {"act_1": {"campaigns": []}}})


if __name__ == "__main__":
    unittest.main()

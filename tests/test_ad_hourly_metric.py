from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

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
        self.assertIn("report_partition_datetime", dag_py)
        self.assertIn("ON CONFLICT ({conflict_target}) DO UPDATE", dag_py)
        self.assertIn("update_count = target.update_count + 1", dag_py)


if __name__ == "__main__":
    unittest.main()

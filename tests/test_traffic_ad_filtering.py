from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs import traffic  # noqa: E402


class AdTrafficFilteringTest(unittest.TestCase):
    def test_ad_traffic_snapshot_filters_by_configured_ad_ids(self) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []

        class FakeMetaGraphClient:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token

            def get_all(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
                calls.append((endpoint, params))
                return [{"ad_id": "ad_3", "adset_id": "adset_1"}]

        original_client = traffic.MetaGraphClient
        traffic.MetaGraphClient = FakeMetaGraphClient  # type: ignore[assignment]
        try:
            snapshot = traffic.ad_traffic_snapshot(
                "token",
                "4157857287789311",
                "adset_1",
                "2026-05-19",
                ad_ids=["ad_1", "ad_2", "ad_3", "ad_4", "ad_5"],
                page_limit=500,
            )
        finally:
            traffic.MetaGraphClient = original_client

        self.assertEqual(snapshot["account_id"], "act_4157857287789311")
        self.assertEqual(snapshot["adset_id"], "adset_1")
        self.assertEqual(snapshot["insights"], [{"ad_id": "ad_3", "adset_id": "adset_1"}])
        self.assertEqual(calls[0][0], "adset_1/insights")
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

    def test_ad_traffic_snapshot_skips_api_when_config_has_no_ads(self) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []

        class FakeMetaGraphClient:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token

            def get_all(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
                calls.append((endpoint, params))
                return []

        original_client = traffic.MetaGraphClient
        traffic.MetaGraphClient = FakeMetaGraphClient  # type: ignore[assignment]
        try:
            snapshot = traffic.ad_traffic_snapshot(
                "token",
                "act_4157857287789311",
                "adset_1",
                "2026-05-19",
                ad_ids=[],
            )
        finally:
            traffic.MetaGraphClient = original_client

        self.assertEqual(calls, [])
        self.assertEqual(snapshot["insights"], [])

    def test_configured_ad_guard_matches_only_snapshot_ad_ids(self) -> None:
        adset = {
            "id": "adset_1",
            "ads": [
                {"id": "ad_1"},
                {"id": "ad_2"},
                {"id": "ad_3"},
                {"id": "ad_4"},
                {"id": "ad_5"},
            ],
        }

        self.assertEqual(traffic.ad_ids_from_config(adset), ["ad_1", "ad_2", "ad_3", "ad_4", "ad_5"])
        self.assertTrue(traffic.insight_ad_is_configured({"ad_id": "ad_3"}, adset))
        self.assertFalse(traffic.insight_ad_is_configured({"ad_id": "ad_6"}, adset))
        self.assertFalse(traffic.insight_ad_is_configured({}, adset))


if __name__ == "__main__":
    unittest.main()

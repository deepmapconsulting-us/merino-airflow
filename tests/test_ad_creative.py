from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from merino_meta_jobs.ad_creative import (
    ads_for_creative_registry_from_snapshot,
    creative_video_ids,
    fetch_and_sync_ad_creatives,
    has_image_creative,
)


class AdCreativeTest(unittest.TestCase):
    def test_creative_video_ids_from_object_story_spec(self) -> None:
        creative = {
            "object_story_spec": {
                "video_data": {"video_id": "1325362342895307"},
            }
        }
        self.assertEqual(creative_video_ids(creative), ["1325362342895307"])

    def test_has_image_creative_without_video(self) -> None:
        creative = {
            "object_type": "PHOTO",
            "image_url": "https://example.com/photo.jpg",
        }
        self.assertTrue(has_image_creative(creative, has_video=False))

    def test_ads_for_creative_registry_from_snapshot_filters_inactive(self) -> None:
        snapshot = {
            "accounts": {
                "act_1": {
                    "id": "act_1",
                    "campaigns": [
                        {
                            "id": "c1",
                            "status": "ACTIVE",
                            "adsets": [
                                {
                                    "id": "a1",
                                    "status": "ACTIVE",
                                    "ads": [
                                        {
                                            "id": "ad1",
                                            "status": "ACTIVE",
                                            "creative": {"id": "cr1"},
                                        },
                                        {
                                            "id": "ad2",
                                            "status": "PAUSED",
                                            "updated_at": "2020-01-01T00:00:00+0000",
                                            "creative": {"id": "cr2"},
                                        },
                                    ],
                                }
                            ],
                        }
                    ],
                }
            }
        }
        ads = ads_for_creative_registry_from_snapshot(
            snapshot,
            lookup_window_days=3,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
        )
        self.assertEqual([row["ad_id"] for row in ads], ["ad1"])

    def test_fetch_and_sync_ad_creatives_upserts_rows(self) -> None:
        client = MagicMock()
        client.get_all.return_value = [
            {
                "id": "cr1",
                "object_type": "VIDEO",
                "object_story_spec": {"video_data": {"video_id": "v1"}},
            }
        ]
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor

        result = fetch_and_sync_ad_creatives(
            client,
            conn,
            [
                {
                    "ad_id": "ad1",
                    "adset_id": "as1",
                    "campaign_id": "c1",
                    "source_account_id": "act_1",
                    "creative_id": "cr1",
                }
            ],
            config_snapshot_uri="gs://bucket/snapshot.json",
        )

        self.assertEqual(result["ads"], 1)
        self.assertEqual(result["creatives_fetched"], 1)
        self.assertEqual(result["rows_upserted"], 1)
        client.get_all.assert_called_once()
        cursor.executemany.assert_called_once()


if __name__ == "__main__":
    unittest.main()

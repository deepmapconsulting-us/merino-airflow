from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs import traffic  # noqa: E402  # type: ignore[import-not-found]
from merino_meta_jobs.media_analysis import (  # noqa: E402  # type: ignore[import-not-found]
    analysis_targets_from_download,
    creative_media_analysis,
    download_ad_creative_assets,
    media_analysis_headers,
)


class MediaAnalysisAdsFilterTest(unittest.TestCase):
    def test_media_analysis_ads_from_adset_filters_active_with_creative(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)
        adset = {
            "id": "adset_1",
            "ads": [
                {"id": "ad_active", "status": "ACTIVE", "creative_id": "cr_1"},
                {"id": "ad_no_creative", "status": "ACTIVE"},
                {"id": "ad_paused_old", "status": "PAUSED", "creative_id": "cr_2", "updated_at": "2020-01-01T00:00:00+0000"},
                {
                    "id": "ad_recent",
                    "status": "PAUSED",
                    "creative_id": "cr_3",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            ],
        }
        ads = traffic.media_analysis_ads_from_adset(adset, cutoff=cutoff)
        ids = {ad["id"] for ad in ads}
        self.assertEqual(ids, {"ad_active", "ad_recent"})
        self.assertEqual(ads[0]["creative_id"], "cr_1")


class MediaAnalysisClientTest(unittest.TestCase):
    def test_media_analysis_headers(self) -> None:
        headers = media_analysis_headers("meta-token", "gateway-token")
        self.assertEqual(headers["Authorization"], "Bearer meta-token")
        self.assertEqual(headers["X-MCP-Gateway-Token"], "gateway-token")

    @patch("requests.post")
    def test_download_ad_creative_assets_posts_expected_body(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"ad_id": "123", "video_ids": ["456"], "videos": [], "cache_hits": []},
        )
        download_ad_creative_assets(
            "123",
            meta_token="meta",
            gateway_token="gw",
            base_url="http://mcp.test",
            get_video_frame_in_sec=5,
            split_frame_by_sec=1.0,
            force_refresh=True,
        )
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "http://mcp.test/api/v1/download-ad-creative-assets")
        self.assertEqual(kwargs["json"]["ad_id"], "123")
        self.assertEqual(kwargs["json"]["get_video_frame_in_sec"], 5)
        self.assertEqual(kwargs["json"]["split_frame_by_sec"], 1.0)
        self.assertTrue(kwargs["json"]["force_refresh"])
        self.assertEqual(kwargs["headers"]["X-MCP-Gateway-Token"], "gw")

    @patch("requests.post")
    def test_creative_media_analysis_posts_storage_and_ids(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"video_analysis": {}, "from_cache": False},
        )
        storage = {"local_dir": "/data", "frames": []}
        creative_media_analysis(
            storage,
            ad_id="123",
            video_id="456",
            meta_token="meta",
            gateway_token="gw",
            base_url="http://mcp.test",
            force_refresh=False,
            max_frames=10,
        )
        body = mock_post.call_args.kwargs["json"]
        self.assertEqual(body["storage"], storage)
        self.assertEqual(body["ad_id"], "123")
        self.assertEqual(body["video_id"], "456")
        self.assertEqual(body["max_frames"], 10)


class AnalysisTargetsTest(unittest.TestCase):
    def test_analysis_targets_from_download_two_videos(self) -> None:
        download_payload = {
            "ad_id": "111",
            "videos": [
                {
                    "video_id": "v1",
                    "storage": {"local_dir": "/a", "frames": ["f1.jpg"]},
                },
                {
                    "creative_video_id": "v2",
                    "storage": {"local_dir": "/a", "frames": ["f2.jpg"]},
                },
                {"video_id": "v3"},
            ],
        }
        targets = analysis_targets_from_download(download_payload)
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[0]["video_id"], "v1")
        self.assertEqual(targets[1]["video_id"], "v2")
        self.assertEqual(targets[0]["ad_id"], "111")
        self.assertIn("frames", targets[0]["storage"])


if __name__ == "__main__":
    unittest.main()

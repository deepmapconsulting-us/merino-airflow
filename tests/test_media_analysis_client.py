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
    DEFAULT_BASE_URL,
    analysis_targets_from_download,
    creative_media_analysis,
    download_ad_creative_assets,
    media_analysis_base_url,
    media_analysis_headers,
    upsert_creative_media_analysis,
)


class MediaAnalysisAdsFilterTest(unittest.TestCase):
    def test_traffic_accounts_from_config_preserves_adset_targeting(self) -> None:
        snapshot = {
            "accounts": {
                "act_1": {
                    "id": "act_1",
                    "campaigns": [
                        {
                            "id": "camp_1",
                            "status": "ACTIVE",
                            "adsets": [
                                {
                                    "id": "adset_1",
                                    "status": "ACTIVE",
                                    "campaign_id": "camp_1",
                                    "targeting": {"age_min": 30, "age_max": 65, "genders": [1, 2]},
                                    "targeting_summary": {
                                        "age_min": 30,
                                        "age_max": 65,
                                        "genders": [1, 2],
                                        "advantage_audience": None,
                                        "geo_countries": ["US"],
                                    },
                                    "ads": [{"id": "ad_1", "status": "ACTIVE", "creative": {"id": "cr_1"}}],
                                }
                            ],
                        }
                    ],
                }
            }
        }

        accounts = traffic.traffic_accounts_from_config(snapshot, now=datetime.now(timezone.utc))
        adset = accounts[0]["campaigns"][0]["adsets"][0]

        self.assertEqual(adset["targeting"]["age_min"], 30)
        self.assertEqual(adset["targeting_summary"]["genders"], [1, 2])

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
    def test_media_analysis_base_url_defaults_to_in_cluster_service(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            with patch("merino_meta_jobs.media_analysis._variable_get", return_value=""):
                self.assertEqual(
                    media_analysis_base_url(),
                    "http://media-analysis-mcp.merino-mcp.svc.cluster.local:8080",
                )
        self.assertEqual(DEFAULT_BASE_URL, "http://media-analysis-mcp.merino-mcp.svc.cluster.local:8080")

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


class CreativeMediaAnalysisPersistenceTest(unittest.TestCase):
    def test_upsert_creative_media_analysis_writes_snapshot_and_traffic(self) -> None:
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (42,)
        partition_datetime = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)

        snapshot_id = upsert_creative_media_analysis(
            conn,
            campaign_id="camp_1",
            adset_id="adset_1",
            ad_id="ad_1",
            video_id="video_1",
            partition_datetime=partition_datetime,
            analysis={
                "freeform_video_summary": "summary",
                "video_analysis_schema_name": "VideoAnalysisDynamic",
                "video_analysis": {"theme": "demo"},
                "audio_analysis": {"music": "upbeat"},
            },
        )

        self.assertEqual(snapshot_id, 42)
        self.assertEqual(cursor.execute.call_count, 2)
        snapshot_sql, snapshot_params = cursor.execute.call_args_list[0].args
        traffic_sql, traffic_params = cursor.execute.call_args_list[1].args
        self.assertIn("marketing.creative_media_analysis_snapshot", snapshot_sql)
        self.assertIn("ON CONFLICT (campaign_id, adset_id, ad_id, video_id) DO UPDATE", snapshot_sql)
        self.assertIn("RETURNING id", snapshot_sql)
        self.assertIn('"theme": "demo"', snapshot_params[4])
        self.assertEqual(snapshot_params[5], "summary")
        self.assertEqual(snapshot_params[6], "VideoAnalysisDynamic")
        self.assertIn("marketing.creative_media_analysis_traffic", traffic_sql)
        self.assertIn(
            "ON CONFLICT (partition_datetime, campaign_id, adset_id, ad_id, video_id) DO UPDATE",
            traffic_sql,
        )
        self.assertEqual(traffic_params[0], 42)
        self.assertEqual(traffic_params[1], partition_datetime)


if __name__ == "__main__":
    unittest.main()

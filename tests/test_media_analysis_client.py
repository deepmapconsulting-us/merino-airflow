from __future__ import annotations

import sys
import unittest
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs import traffic  # noqa: E402  # type: ignore[import-not-found]
from merino_meta_jobs.media_analysis import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_BASE_URL,
    analysis_targets_from_download,
    build_video_preview_url,
    creative_media_analysis,
    creative_media_analysis_skip_status,
    download_ad_creative_assets,
    media_analysis_analysis_cache_key,
    media_analysis_base_url,
    media_analysis_cache_matches,
    media_analysis_files_cache_key,
    media_analysis_config_for_ad,
    media_analysis_headers,
    parse_media_analysis_config,
    image_gcs_uri_from_download,
    translate_chinese_schema_prompt,
    translate_creative_media_analysis_to_chinese,
    update_chinese_creative_media_analysis_snapshot,
    upsert_creative_media_analysis,
    video_gcs_uri_from_download,
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
        self.assertFalse(body["log_generation_input"])
        self.assertNotIn("api_key", body)
        self.assertNotIn("openai_api_key", body)

    @patch("requests.post")
    def test_creative_media_analysis_posts_optional_config(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"video_analysis": {}, "from_cache": False},
        )
        config = {"image_type": "contents", "contents_end": 3}

        creative_media_analysis(
            {"local_dir": "/data", "contents": ["a.jpg"]},
            ad_id="123",
            video_id="456",
            meta_token="meta",
            gateway_token="gw",
            base_url="http://mcp.test",
            config=config,
            log_generation_input=True,
        )

        body = mock_post.call_args.kwargs["json"]
        self.assertEqual(body["config"], config)
        self.assertTrue(body["log_generation_input"])

    def test_parse_media_analysis_config_single_and_list_payloads(self) -> None:
        single = parse_media_analysis_config(
            '{"ad_id":"ad_1","config":{"image_type":"frames","frame_sample_end_sec":3}}'
        )
        multiple = parse_media_analysis_config(
            '[{"ad_id":"ad_2","config":{"image_type":"contents","contents_end":2}}]'
        )

        self.assertEqual(media_analysis_config_for_ad("ad_1", single)["frame_sample_end_sec"], 3)
        self.assertEqual(media_analysis_config_for_ad("ad_2", multiple)["image_type"], "contents")
        self.assertEqual(media_analysis_config_for_ad("missing", multiple), {})

    @patch("requests.post")
    def test_translate_chinese_schema_prompt_posts_expected_body(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "created": False,
                "existing": True,
                "target_prompt_name": "media_analysis_mcp/创意媒体分析快照结构",
            },
        )

        payload = translate_chinese_schema_prompt(
            gateway_token="gw",
            base_url="http://mcp.test",
            model="gpt-5.5",
            input_content="Representative creative analysis rows.",
            force=True,
            dry_run=False,
        )

        self.assertTrue(payload["existing"])
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "http://mcp.test/api/v1/translate/chinese-schema-prompt")
        self.assertEqual(kwargs["headers"]["X-MCP-Gateway-Token"], "gw")
        self.assertNotIn("Authorization", kwargs["headers"])
        self.assertEqual(kwargs["json"]["source_prompt_name"], "media_analysis_mcp/video_analysis_schema")
        self.assertEqual(kwargs["json"]["target_prompt_name"], "media_analysis_mcp/创意媒体分析快照结构")
        self.assertEqual(kwargs["json"]["translation_prompt_name"], "media_analysis_mcp/translate_schema_to_chineese")
        self.assertEqual(kwargs["json"]["input_content"], "Representative creative analysis rows.")
        self.assertEqual(kwargs["json"]["model"], "gpt-5.5")
        self.assertTrue(kwargs["json"]["force"])
        self.assertFalse(kwargs["json"]["dry_run"])

    @patch("requests.post")
    def test_translate_chinese_schema_prompt_omits_empty_input_content(
        self,
        mock_post: MagicMock,
    ) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"created": False, "existing": True},
        )

        translate_chinese_schema_prompt(
            gateway_token="gw",
            base_url="http://mcp.test",
        )

        _, kwargs = mock_post.call_args
        self.assertNotIn("input_content", kwargs["json"])

    @patch("requests.post")
    def test_translate_creative_media_analysis_to_chinese_posts_expected_body(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "translated_analysis": {"主题": "产品展示", "视频自由摘要": "中文摘要"},
                "field_count": 2,
            },
        )

        payload = translate_creative_media_analysis_to_chinese(
            analysis={"video_analysis": {"theme": "product display"}},
            freeform_video_summary="A product is shown.",
            gateway_token="gw",
            base_url="http://mcp.test",
            model="gpt-5.5",
        )

        self.assertEqual(payload["translated_analysis"]["主题"], "产品展示")
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "http://mcp.test/api/v1/translate/chinese-analysis")
        self.assertEqual(kwargs["headers"]["X-MCP-Gateway-Token"], "gw")
        self.assertEqual(kwargs["json"]["analysis"]["video_analysis"]["theme"], "product display")
        self.assertEqual(kwargs["json"]["freeform_video_summary"], "A product is shown.")
        self.assertEqual(kwargs["json"]["schema_prompt_name"], "media_analysis_mcp/创意媒体分析快照结构")
        self.assertEqual(kwargs["json"]["translation_prompt_name"], "media_analysis_mcp/translate_schema_to_chineese")
        self.assertEqual(kwargs["json"]["model"], "gpt-5.5")

    @patch("requests.post")
    def test_translate_creative_media_analysis_to_chinese_forwards_langfuse_trace(
        self, mock_post: MagicMock
    ) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"translated_analysis": {"主题": "产品展示", "视频自由摘要": "中文摘要"}},
        )

        translate_creative_media_analysis_to_chinese(
            analysis={"video_analysis": {"theme": "product display"}},
            freeform_video_summary="A product is shown.",
            gateway_token="gw",
            base_url="http://mcp.test",
            langfuse_trace_id="trace-123",
            langfuse_parent_observation_id="span-456",
            langfuse_trace_name="media-analysis.ad-1-analysis",
        )

        body = mock_post.call_args.kwargs["json"]
        self.assertEqual(body["langfuse_trace_id"], "trace-123")
        self.assertEqual(body["langfuse_parent_observation_id"], "span-456")
        self.assertEqual(body["langfuse_trace_name"], "media-analysis.ad-1-analysis")
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
                {
                    "video_id": "v4",
                    "storage": {"local_dir": "/a", "frames": [], "frames_dir": ""},
                },
            ],
        }
        targets = analysis_targets_from_download(download_payload)
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[0]["video_id"], "v1")
        self.assertEqual(targets[1]["video_id"], "v2")
        self.assertEqual(targets[0]["ad_id"], "111")
        self.assertIn("frames", targets[0]["storage"])

    def test_analysis_targets_include_creative_id(self) -> None:
        download_payload = {
            "ad_id": "111",
            "creative_id": "cr_top",
            "videos": [
                {
                    "video_id": "v1",
                    "creative_id": "cr_video",
                    "storage": {"frames": ["f1.jpg"]},
                }
            ],
        }
        targets = analysis_targets_from_download(download_payload)
        self.assertEqual(targets[0]["creative_id"], "cr_video")

    def test_analysis_targets_from_download_contents_mode(self) -> None:
        download_payload = {
            "ad_id": "111",
            "videos": [
                {
                    "video_id": "v1",
                    "storage": {"contents": ["content_000.jpg"]},
                },
                {
                    "video_id": "v2",
                    "storage": {"frames": ["f1.jpg"]},
                },
            ],
        }
        targets = analysis_targets_from_download(
            download_payload,
            config={"image_type": "contents"},
        )
        self.assertEqual([target["video_id"] for target in targets], ["v1"])

    def test_analysis_targets_from_download_image_bundle(self) -> None:
        download_payload = {
            "ad_id": "120239306002680157",
            "creative_id": "2360925424372934",
            "videos": [],
            "images": [
                {
                    "creative_id": "2360925424372934",
                    "image_asset_id": "2360925424372934",
                    "storage": {
                        "media_type": "image",
                        "images": ["images/image_000.jpg"],
                    },
                }
            ],
        }
        targets = analysis_targets_from_download(download_payload)
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["media_type"], "image")
        self.assertEqual(targets[0]["image_asset_id"], "2360925424372934")
        self.assertEqual(targets[0]["video_id"], "")

    def test_analysis_targets_from_download_image_bundle_without_videos_key(self) -> None:
        download_payload = {
            "ad_id": "120239306002680157",
            "creative_id": "2360925424372934",
            "images": [
                {
                    "image_asset_id": "2360925424372934",
                    "storage": {
                        "media_type": "image",
                        "images": ["images/image_000.jpg"],
                    },
                }
            ],
        }
        targets = analysis_targets_from_download(download_payload)
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["media_type"], "image")

    @patch("requests.post")
    def test_creative_media_analysis_posts_image_asset_id(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"video_analysis": {}, "from_cache": False},
        )
        storage = {
            "local_dir": "/data",
            "media_type": "image",
            "images": ["images/image_000.jpg"],
        }
        creative_media_analysis(
            storage,
            ad_id="120239306002680157",
            image_asset_id="2360925424372934",
            meta_token="meta",
            gateway_token="gw",
            base_url="http://mcp.test",
            audio_analysis=False,
            log_generation_input=True,
        )
        body = mock_post.call_args.kwargs["json"]
        self.assertEqual(body["image_asset_id"], "2360925424372934")
        self.assertFalse(body["audio_analysis"])
        self.assertTrue(body["log_generation_input"])
        self.assertNotIn("video_id", body)

    @patch("requests.post")
    def test_creative_media_analysis_returns_skipped_payload(self, mock_post: MagicMock) -> None:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "skipped": True,
                "warning": "No frame images in storage.frames or storage.frames_dir",
                "ad_id": "123",
                "video_id": "456",
            },
        )
        payload = creative_media_analysis(
            {"local_dir": "/data", "frames": []},
            ad_id="123",
            video_id="456",
            meta_token="meta",
            gateway_token="gw",
            base_url="http://mcp.test",
        )
        self.assertTrue(payload.get("skipped"))
        self.assertIn("frame images", payload.get("warning", ""))


class MediaAnalysisRedisSkipTest(unittest.TestCase):
    def test_cache_keys_match_mcp_prefix(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(
                media_analysis_files_cache_key("ad_1", "video_1"),
                "meta:meta_media_analysis:files:ad_1:video_1",
            )
            self.assertEqual(
                media_analysis_analysis_cache_key("ad_1", "video_1"),
                "meta:meta_media_analysis:analysis:ad_1:video_1",
            )

    def test_media_analysis_cache_matches_config_and_audio_requirement(self) -> None:
        payload = {
            "ad_id": "ad_1",
            "video_id": "video_1",
            "freeform_video_summary": "summary",
            "video_analysis": {"hook": "demo"},
            "audio_analysis": {"music": "upbeat"},
            "media_config": {"image_type": "contents"},
        }

        self.assertTrue(
            media_analysis_cache_matches(
                payload,
                ad_id="ad_1",
                media_id="video_1",
                audio_analysis=True,
                media_config={"image_type": "contents"},
            )
        )
        self.assertFalse(
            media_analysis_cache_matches(
                payload,
                ad_id="ad_1",
                media_id="video_1",
                audio_analysis=True,
                media_config={"image_type": "frames"},
            )
        )
        self.assertFalse(
            media_analysis_cache_matches(
                {**payload, "audio_analysis": None},
                ad_id="ad_1",
                media_id="video_1",
                audio_analysis=True,
            )
        )

    def test_skip_status_requires_redis_cache_and_current_traffic_row(self) -> None:
        redis_client = MagicMock()
        redis_client.get.side_effect = lambda key: json.dumps(
            {
                "meta:meta_media_analysis:files:ad_1:video_1": {
                    "ad_id": "ad_1",
                    "video_id": "video_1",
                },
                "meta:meta_media_analysis:analysis:ad_1:video_1": {
                    "ad_id": "ad_1",
                    "video_id": "video_1",
                    "freeform_video_summary": "summary",
                    "video_analysis": {"hook": "demo"},
                    "audio_analysis": {"music": "upbeat"},
                    "media_config": {},
                },
            }.get(key)
        )

        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.side_effect = [
            [(["video_1"],)],
            [("video_1",)],
        ]

        status = creative_media_analysis_skip_status(
            conn,
            ad_id="ad_1",
            partition_datetime=datetime(2026, 6, 1, 12, tzinfo=timezone.utc),
            audio_analysis=True,
            redis_client=redis_client,
        )

        self.assertTrue(status["skip"])
        self.assertEqual(status["reason"], "redis_cache_and_traffic_ready")
        self.assertEqual(status["video_ids"], ["video_1"])

    def test_skip_status_runs_when_current_traffic_row_is_missing(self) -> None:
        redis_client = MagicMock()
        redis_client.get.side_effect = lambda key: json.dumps(
            {
                "meta:meta_media_analysis:files:ad_1:video_1": {
                    "ad_id": "ad_1",
                    "video_id": "video_1",
                },
                "meta:meta_media_analysis:analysis:ad_1:video_1": {
                    "ad_id": "ad_1",
                    "video_id": "video_1",
                    "freeform_video_summary": "summary",
                    "video_analysis": {"hook": "demo"},
                    "audio_analysis": {"music": "upbeat"},
                    "media_config": {},
                },
            }.get(key)
        )

        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.side_effect = [
            [(["video_1"],)],
            [],
        ]

        status = creative_media_analysis_skip_status(
            conn,
            ad_id="ad_1",
            partition_datetime=datetime(2026, 6, 1, 12, tzinfo=timezone.utc),
            audio_analysis=True,
            redis_client=redis_client,
        )

        self.assertFalse(status["skip"])
        self.assertEqual(status["reason"], "traffic_snapshot_missing")
        self.assertEqual(status["missing_traffic"], ["video_1"])


class ImageGcsUriFromDownloadTest(unittest.TestCase):
    def test_image_gcs_uri_picks_first_image(self) -> None:
        payload = {
            "storage_prefix": "gs://meta_analysis/campaign_1/adset_1/ad_1/creative_1",
            "gcs_files": [
                "gs://meta_analysis/campaign_1/adset_1/ad_1/creative_1/images/image_000.jpg",
            ],
            "images": [
                {
                    "image_asset_id": "2360925424372934",
                    "storage": {
                        "images": ["images/image_000.jpg"],
                        "gcs_files": [
                            "gs://meta_analysis/campaign_1/adset_1/ad_1/creative_1/images/image_000.jpg",
                        ],
                    },
                }
            ],
        }
        uri = image_gcs_uri_from_download(
            payload,
            ad_id="120239306002680157",
            image_asset_id="2360925424372934",
        )
        self.assertEqual(
            uri,
            "gs://meta_analysis/campaign_1/adset_1/ad_1/creative_1/images/image_000.jpg",
        )


class VideoGcsUriFromDownloadTest(unittest.TestCase):
    def test_video_gcs_uri_picks_mp4_for_video_id(self) -> None:
        payload = {
            "storage_prefix": "gs://meta_analysis/campaign_1/adset_1/ad_1/creative_1",
            "gcs_files": [
                "gs://meta_analysis/campaign_1/adset_1/ad_1/creative_1/video_99/frame.jpg",
                "gs://meta_analysis/campaign_1/adset_1/ad_1/creative_1/video_42/ad_1_video_42_title.mp4",
            ],
            "videos": [
                {
                    "video_id": "42",
                    "storage": {
                        "gcs_files": [
                            "gs://meta_analysis/campaign_1/adset_1/ad_1/creative_1/video_42/ad_1_video_42_title.mp4",
                        ]
                    },
                }
            ],
        }
        uri = video_gcs_uri_from_download(payload, ad_id="1", video_id="42")
        self.assertEqual(
            uri,
            "gs://meta_analysis/campaign_1/adset_1/ad_1/creative_1/video_42/ad_1_video_42_title.mp4",
        )

    def test_build_video_preview_url_encodes_uri(self) -> None:
        gcs_uri = "gs://meta_analysis/foo/bar.mp4"
        url = build_video_preview_url(gcs_uri)
        self.assertIn("/api/v1/media/preview?uri=", url)
        self.assertIn("gs%3A%2F%2Fmeta_analysis%2Ffoo%2Fbar.mp4", url)


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
            creative_id="cr_1",
            video_id="video_1",
            partition_datetime=partition_datetime,
            analysis={
                "freeform_video_summary": "summary",
                "video_analysis_schema_name": "VideoAnalysisDynamic",
                "primary_text": "Primary copy",
                "headline": "Headline copy",
                "description": "Description copy",
                "video_analysis": {"theme": "demo"},
                "audio_analysis": {"music": "upbeat"},
            },
            video_gcs_uri="gs://meta_analysis/campaign/ad.mp4",
            video_preview_url="https://media-analysis-mcp.merino-aiagent.com/api/v1/media/preview?uri=gs%3A%2F%2F",
        )

        self.assertEqual(snapshot_id, 42)
        self.assertEqual(cursor.execute.call_count, 2)
        snapshot_sql, snapshot_params = cursor.execute.call_args_list[0].args
        traffic_sql, traffic_params = cursor.execute.call_args_list[1].args
        self.assertIn("marketing.creative_media_analysis_snapshot", snapshot_sql)
        self.assertIn("creative_id", snapshot_sql)
        self.assertIn("ON CONFLICT (campaign_id, adset_id, ad_id, media_type, video_id, image_asset_id) DO UPDATE", snapshot_sql)
        self.assertIn("RETURNING id", snapshot_sql)
        self.assertEqual(snapshot_params[3], "cr_1")
        self.assertEqual(snapshot_params[4], "video")
        self.assertEqual(snapshot_params[5], "video_1")
        self.assertEqual(snapshot_params[6], "")
        self.assertIn('"theme": "demo"', snapshot_params[7])
        self.assertIn('"primary_text": "Primary copy"', snapshot_params[7])
        self.assertIn('"headline": "Headline copy"', snapshot_params[7])
        self.assertIn('"description": "Description copy"', snapshot_params[7])
        self.assertEqual(snapshot_params[8], "summary")
        self.assertEqual(snapshot_params[9], "VideoAnalysisDynamic")
        self.assertEqual(snapshot_params[10], "gs://meta_analysis/campaign/ad.mp4")
        self.assertIn("media/preview", snapshot_params[11])
        self.assertIn("video_gcs_uri", snapshot_sql)
        self.assertIn("video_preview_url", snapshot_sql)
        self.assertIn("image_gcs_uri", snapshot_sql)
        self.assertIn("image_preview_url", snapshot_sql)
        self.assertIn("marketing.creative_media_analysis_traffic", traffic_sql)
        self.assertIn("creative_id", traffic_sql)
        self.assertIn(
            "ON CONFLICT (partition_datetime, campaign_id, adset_id, ad_id, media_type, video_id, image_asset_id) DO UPDATE",
            traffic_sql,
        )
        self.assertEqual(traffic_params[0], 42)
        self.assertEqual(traffic_params[5], "cr_1")
        self.assertEqual(traffic_params[6], "video")
        self.assertEqual(traffic_params[7], "video_1")
        self.assertEqual(traffic_params[1], partition_datetime)

    def test_update_chinese_creative_media_analysis_snapshot_updates_analysis_by_id(self) -> None:
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value

        update_chinese_creative_media_analysis_snapshot(
            conn,
            snapshot_id=42,
            translated_analysis={"主题": "产品展示", "视频自由摘要": "中文摘要"},
        )

        sql, params = cursor.execute.call_args.args
        self.assertIn('marketing."创意媒体分析快照"', sql)
        self.assertIn('SET "分析结果" = %s::jsonb', sql)
        self.assertIn("WHERE id = %s", sql)
        self.assertIn('"主题": "产品展示"', params[0])
        self.assertEqual(params[1], 42)


if __name__ == "__main__":
    unittest.main()

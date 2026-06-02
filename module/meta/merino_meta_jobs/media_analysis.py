"""HTTP client for media-analysis-mcp FastAPI (download + creative analysis)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

DOWNLOAD_PATH = "/api/v1/download-ad-creative-assets"
ANALYSIS_PATH = "/api/v1/creative-media-analysis"
# In-cluster service (Airflow workers run in GKE; same namespace routing as ingress bridge).
DEFAULT_BASE_URL = "http://media-analysis-mcp.merino-mcp.svc.cluster.local:8080"
PUBLIC_BASE_URL = "https://media-analysis-mcp.merino-aiagent.com"
MEDIA_ANALYSIS_URL_VARIABLE = "media_analysis_url"
MEDIA_ANALYSIS_URL_ENV = "MEDIA_ANALYSIS_URL"
MCP_GATEWAY_TOKEN_VARIABLE = "meta_mcp_gateway_token"
MCP_GATEWAY_TOKEN_ENV = "META_MCP_GATEWAY_TOKEN"
DOWNLOAD_TIMEOUT_SEC = 300
ANALYSIS_TIMEOUT_SEC = 600
CREATIVE_MEDIA_ANALYSIS_SNAPSHOT_TABLE = "marketing.creative_media_analysis_snapshot"
CREATIVE_MEDIA_ANALYSIS_TRAFFIC_TABLE = "marketing.creative_media_analysis_traffic"


def _variable_get(key: str, fallback: str = "") -> str:
    try:
        from airflow.sdk import Variable  # type: ignore[import-not-found]

        return str(Variable.get(key))
    except Exception:
        return fallback


def media_analysis_base_url() -> str:
    url = _variable_get(MEDIA_ANALYSIS_URL_VARIABLE, os.environ.get(MEDIA_ANALYSIS_URL_ENV, "")).strip()
    return url or DEFAULT_BASE_URL


def mcp_gateway_token() -> str:
    token = _variable_get(MCP_GATEWAY_TOKEN_VARIABLE, os.environ.get(MCP_GATEWAY_TOKEN_ENV, "")).strip()
    if not token:
        raise RuntimeError(
            f"MCP gateway token missing. Set Airflow Variable {MCP_GATEWAY_TOKEN_VARIABLE!r} "
            f"(GSM secret airflow-variables-{MCP_GATEWAY_TOKEN_VARIABLE}) "
            f"or env {MCP_GATEWAY_TOKEN_ENV}."
        )
    return token


def media_analysis_headers(meta_token: str, gateway_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {meta_token}",
        "X-MCP-Gateway-Token": gateway_token,
        "Content-Type": "application/json",
    }


def _post_json(
    base_url: str,
    path: str,
    headers: dict[str, str],
    body: dict[str, Any],
    *,
    timeout_sec: int,
) -> dict[str, Any]:
    import requests

    url = f"{base_url.rstrip('/')}{path}"
    response = requests.post(url, json=body, headers=headers, timeout=timeout_sec)
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code} POST {path}: {response.text[:2000]}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object from POST {path}")
    if payload.get("error") and set(payload.keys()) <= {"error"}:
        raise RuntimeError(str(payload["error"]))
    return payload


def download_ad_creative_assets(
    ad_id: str,
    *,
    meta_token: str,
    gateway_token: str,
    base_url: str | None = None,
    get_video_frame_in_sec: int = 3,
    split_frame_by_sec: float = 1.0,
    force_refresh: bool = False,
    bucket_location: str = "meta_analysis",
    save_to_gcs: bool = False,
) -> dict[str, Any]:
    body = {
        "ad_id": ad_id,
        "bucket_location": bucket_location,
        "get_video_frame_in_sec": get_video_frame_in_sec,
        "split_frame_by_sec": split_frame_by_sec,
        "force_refresh": force_refresh,
        "save_to_gcs": save_to_gcs,
    }
    return _post_json(
        base_url or media_analysis_base_url(),
        DOWNLOAD_PATH,
        media_analysis_headers(meta_token, gateway_token),
        body,
        timeout_sec=DOWNLOAD_TIMEOUT_SEC,
    )


def creative_media_analysis(
    storage: dict[str, Any],
    *,
    ad_id: str,
    video_id: str,
    meta_token: str,
    gateway_token: str,
    base_url: str | None = None,
    force_refresh: bool = False,
    max_frames: int = 20,
    audio_analysis: bool = True,
    extras: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "storage": storage,
        "ad_id": ad_id,
        "video_id": video_id,
        "force_refresh": force_refresh,
        "max_frames": max_frames,
        "audio_analysis": audio_analysis,
    }
    if extras is not None:
        body["extras"] = extras
    return _post_json(
        base_url or media_analysis_base_url(),
        ANALYSIS_PATH,
        media_analysis_headers(meta_token, gateway_token),
        body,
        timeout_sec=ANALYSIS_TIMEOUT_SEC,
    )


def analysis_targets_from_download(download_payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Build per-video analysis targets from a download API response."""
    ad_id = str(download_payload.get("ad_id") or "")
    videos = download_payload.get("videos")
    if not isinstance(videos, list):
        return []

    targets: list[dict[str, Any]] = []
    for video in videos:
        if not isinstance(video, dict):
            continue
        storage = video.get("storage")
        if not isinstance(storage, dict):
            continue
        video_id = str(video.get("video_id") or video.get("creative_video_id") or "")
        if not video_id:
            continue
        targets.append({"ad_id": ad_id, "video_id": video_id, "storage": storage})
    return targets


def upsert_creative_media_analysis(
    conn: Any,
    *,
    campaign_id: str,
    adset_id: str,
    ad_id: str,
    video_id: str,
    partition_datetime: datetime | str,
    analysis: dict[str, Any],
) -> int:
    """Persist final creative media analysis and partition linkage."""
    analysis_json = json.dumps(analysis, ensure_ascii=False, sort_keys=True)
    freeform_video_summary = str(analysis.get("freeform_video_summary") or "")
    video_analysis_schema_name = str(analysis.get("video_analysis_schema_name") or "")
    partition_value = _partition_datetime(partition_datetime)

    snapshot_sql = f"""
        INSERT INTO {CREATIVE_MEDIA_ANALYSIS_SNAPSHOT_TABLE} AS target (
            campaign_id,
            adset_id,
            ad_id,
            video_id,
            analysis,
            freeform_video_summary,
            video_analysis_schema_name
        )
        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
        ON CONFLICT (campaign_id, adset_id, ad_id, video_id) DO UPDATE
        SET
            analysis = EXCLUDED.analysis,
            freeform_video_summary = EXCLUDED.freeform_video_summary,
            video_analysis_schema_name = EXCLUDED.video_analysis_schema_name,
            updated_at = now(),
            update_count = target.update_count + 1
        WHERE target.analysis IS DISTINCT FROM EXCLUDED.analysis
           OR target.freeform_video_summary IS DISTINCT FROM EXCLUDED.freeform_video_summary
           OR target.video_analysis_schema_name IS DISTINCT FROM EXCLUDED.video_analysis_schema_name
        RETURNING id
    """
    traffic_sql = f"""
        INSERT INTO {CREATIVE_MEDIA_ANALYSIS_TRAFFIC_TABLE} AS target (
            analysis_snapshot_id,
            partition_datetime,
            campaign_id,
            adset_id,
            ad_id,
            video_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (partition_datetime, campaign_id, adset_id, ad_id, video_id) DO UPDATE
        SET
            analysis_snapshot_id = EXCLUDED.analysis_snapshot_id,
            updated_at = now(),
            update_count = target.update_count + 1
        WHERE target.analysis_snapshot_id IS DISTINCT FROM EXCLUDED.analysis_snapshot_id
    """
    with conn.cursor() as cursor:
        cursor.execute(
            snapshot_sql,
            (
                campaign_id,
                adset_id,
                ad_id,
                video_id,
                analysis_json,
                freeform_video_summary or None,
                video_analysis_schema_name or None,
            ),
        )
        row = cursor.fetchone()
        if row is None:
            cursor.execute(
                f"""
                SELECT id
                FROM {CREATIVE_MEDIA_ANALYSIS_SNAPSHOT_TABLE}
                WHERE campaign_id = %s
                  AND adset_id = %s
                  AND ad_id = %s
                  AND video_id = %s
                """,
                (campaign_id, adset_id, ad_id, video_id),
            )
            row = cursor.fetchone()
        if row is None:
            raise RuntimeError(
                f"Could not resolve creative media analysis snapshot id for ad_id={ad_id} video_id={video_id}"
            )

        snapshot_id = int(row[0])
        cursor.execute(
            traffic_sql,
            (
                snapshot_id,
                partition_value,
                campaign_id,
                adset_id,
                ad_id,
                video_id,
            ),
        )
    return snapshot_id


def _partition_datetime(value: datetime | str) -> datetime | str:
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value

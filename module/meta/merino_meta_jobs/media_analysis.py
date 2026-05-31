"""HTTP client for media-analysis-mcp FastAPI (download + creative analysis)."""

from __future__ import annotations

import os
from typing import Any

DOWNLOAD_PATH = "/api/v1/download-ad-creative-assets"
ANALYSIS_PATH = "/api/v1/creative-media-analysis"
DEFAULT_BASE_URL = "https://media-analysis-mcp.merino-aiagent.com"
MEDIA_ANALYSIS_URL_VARIABLE = "media_analysis_url"
MEDIA_ANALYSIS_URL_ENV = "MEDIA_ANALYSIS_URL"
MCP_GATEWAY_TOKEN_VARIABLE = "mcp_gateway_token"
MCP_GATEWAY_TOKEN_ENV = "MCP_GATEWAY_TOKEN"
DOWNLOAD_TIMEOUT_SEC = 300
ANALYSIS_TIMEOUT_SEC = 600


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

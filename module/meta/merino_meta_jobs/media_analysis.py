"""HTTP client for media-analysis-mcp FastAPI (download + creative analysis)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

DOWNLOAD_PATH = "/api/v1/download-ad-creative-assets"
ANALYSIS_PATH = "/api/v1/creative-media-analysis"
TRANSLATE_CHINESE_SCHEMA_PATH = "/api/v1/translate/chinese-schema-prompt"
TRANSLATE_CHINESE_ANALYSIS_PATH = "/api/v1/translate/chinese-analysis"
# In-cluster service (Airflow workers run in GKE; same namespace routing as ingress bridge).
DEFAULT_BASE_URL = "http://media-analysis-mcp.merino-mcp.svc.cluster.local:8080"
PUBLIC_BASE_URL = "https://media-analysis-mcp.merino-aiagent.com"
MEDIA_PREVIEW_BASE_URL_ENV = "MEDIA_PREVIEW_BASE_URL"
MEDIA_ANALYSIS_URL_ENV = "MEDIA_ANALYSIS_URL"
MCP_GATEWAY_TOKEN_VARIABLE = "meta_mcp_gateway_token"
MCP_GATEWAY_TOKEN_ENV = "META_MCP_GATEWAY_TOKEN"
MEDIA_ANALYSIS_CONFIG_ENV = "MEDIA_ANALYSIS_CONFIG"
DOWNLOAD_TIMEOUT_SEC = 300
ANALYSIS_TIMEOUT_SEC = 600
CREATIVE_MEDIA_ANALYSIS_SNAPSHOT_TABLE = "marketing.creative_media_analysis_snapshot"
CREATIVE_MEDIA_ANALYSIS_TRAFFIC_TABLE = "marketing.creative_media_analysis_traffic"
CHINESE_CREATIVE_MEDIA_ANALYSIS_SNAPSHOT_TABLE = 'marketing."创意媒体分析快照"'
AD_CREATIVE_REGISTRY_TABLE = "marketing.meta_ad_creative"
MEDIA_ANALYSIS_REDIS_CONN_ID = "merino_redis"
MEDIA_ANALYSIS_REDIS_META_PREFIX_ENV = "MEDIA_ANALYSIS_REDIS_META_PREFIX"
MEDIA_ANALYSIS_REDIS_META_PREFIX_DEFAULT = "meta:meta_media_analysis"


def _variable_get(key: str, fallback: str = "") -> str:
    try:
        from airflow.sdk import Variable  # type: ignore[import-not-found]

        return str(Variable.get(key))
    except Exception:
        return fallback


def media_analysis_base_url() -> str:
    url = os.environ.get(MEDIA_ANALYSIS_URL_ENV, "").strip()
    return url or DEFAULT_BASE_URL


def media_analysis_redis_meta_prefix() -> str:
    return (
        os.environ.get(MEDIA_ANALYSIS_REDIS_META_PREFIX_ENV, "").strip()
        or MEDIA_ANALYSIS_REDIS_META_PREFIX_DEFAULT
    )


def media_analysis_files_cache_key(ad_id: str, media_id: str) -> str:
    return f"{media_analysis_redis_meta_prefix()}:files:{ad_id}:{media_id}"


def media_analysis_analysis_cache_key(ad_id: str, media_id: str) -> str:
    return f"{media_analysis_redis_meta_prefix()}:analysis:{ad_id}:{media_id}"


def media_analysis_redis_client() -> Any | None:
    try:
        from airflow.providers.redis.hooks.redis import RedisHook  # type: ignore[import-not-found]
    except Exception:
        return None

    try:
        return RedisHook(redis_conn_id=MEDIA_ANALYSIS_REDIS_CONN_ID).get_conn()
    except Exception:
        return None


def media_preview_base_url() -> str:
    return os.environ.get(MEDIA_PREVIEW_BASE_URL_ENV, PUBLIC_BASE_URL).strip().rstrip("/") or PUBLIC_BASE_URL


def build_video_preview_url(gcs_uri: str) -> str:
    base = media_preview_base_url()
    return f"{base}/api/v1/media/preview?uri={quote(gcs_uri, safe='')}"


def _gcs_files_for_video(download_payload: dict[str, Any], video_id: str) -> list[str]:
    seen: set[str] = set()
    uris: list[str] = []

    def add(items: Any) -> None:
        if not isinstance(items, list):
            return
        for item in items:
            if isinstance(item, str) and item.startswith("gs://") and item not in seen:
                seen.add(item)
                uris.append(item)

    add(download_payload.get("gcs_files"))
    videos = download_payload.get("videos")
    if not isinstance(videos, list):
        return uris

    video_marker = f"/video_{video_id}/"
    for video in videos:
        if not isinstance(video, dict):
            continue
        vid = str(video.get("video_id") or video.get("creative_video_id") or "")
        if vid != video_id:
            continue
        storage = video.get("storage")
        if isinstance(storage, dict):
            add(storage.get("gcs_files"))
        saved_file = str(video.get("saved_file") or "")
        if saved_file:
            prefix = str(download_payload.get("storage_prefix") or "").rstrip("/")
            if prefix.startswith("gs://"):
                add([f"{prefix}/{saved_file.lstrip('/')}"])
    return [uri for uri in uris if video_marker in uri or f"video_{video_id}_" in uri]


def video_gcs_uri_from_download(
    download_payload: dict[str, Any],
    *,
    ad_id: str,
    video_id: str,
) -> str | None:
    """Return the gs:// URI for this video's .mp4 from a download API response."""
    del ad_id  # reserved for future path conventions
    for uri in _gcs_files_for_video(download_payload, video_id):
        if uri.lower().endswith(".mp4"):
            return uri
    return None


def mcp_gateway_token() -> str:
    token = os.environ.get(MCP_GATEWAY_TOKEN_ENV, "").strip()
    if not token:
        token = _variable_get(MCP_GATEWAY_TOKEN_VARIABLE).strip()
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


def mcp_gateway_headers(gateway_token: str) -> dict[str, str]:
    return {
        "X-MCP-Gateway-Token": gateway_token,
        "Content-Type": "application/json",
    }


def parse_media_analysis_config(raw: str) -> dict[str, dict[str, Any]]:
    """Parse ad-specific media analysis config from an environment value."""
    if not raw.strip():
        return {}

    payload = json.loads(raw)
    entries: list[Any]
    if isinstance(payload, list):
        entries = payload
    elif isinstance(payload, dict) and "ad_id" in payload:
        entries = [payload]
    elif isinstance(payload, dict):
        return {
            str(ad_id): dict(config)
            for ad_id, config in payload.items()
            if isinstance(config, dict)
        }
    else:
        raise ValueError("MEDIA_ANALYSIS_CONFIG must be a JSON object or list")

    configs: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        ad_id = str(entry.get("ad_id") or "").strip()
        config = entry.get("config")
        if ad_id and isinstance(config, dict):
            configs[ad_id] = dict(config)
    return configs


def media_analysis_config_by_ad() -> dict[str, dict[str, Any]]:
    raw = os.environ.get(MEDIA_ANALYSIS_CONFIG_ENV, "")
    return parse_media_analysis_config(raw)


def media_analysis_config_for_ad(ad_id: str, configs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return dict(configs.get(str(ad_id), {}))


def _sanitize_analysis_body(body: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(body)
    sanitized.pop("api_key", None)
    sanitized.pop("openai_api_key", None)
    return sanitized


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
    response = requests.post(url, json=_sanitize_analysis_body(body), headers=headers, timeout=timeout_sec)
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code} POST {path}: {response.text[:2000]}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object from POST {path}")
    if payload.get("skipped"):
        return payload
    if payload.get("error") and set(payload.keys()) <= {"error"}:
        raise RuntimeError(str(payload["error"]))
    return payload


def storage_has_analyzable_visuals(
    storage: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
) -> bool:
    """True when download storage metadata includes frames, contents, or images for analysis."""
    if str(storage.get("media_type") or "") == "image":
        images = storage.get("images")
        if isinstance(images, list) and images:
            return True
        return bool(str(storage.get("images_dir") or "").strip())

    image_type = str((config or {}).get("image_type") or "frames")
    if image_type == "contents":
        contents = storage.get("contents")
        if isinstance(contents, list) and contents:
            return True
        return bool(str(storage.get("contents_dir") or "").strip())
    frames = storage.get("frames")
    if isinstance(frames, list) and frames:
        return True
    return bool(str(storage.get("frames_dir") or "").strip())


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
    video_id: str = "",
    image_asset_id: str = "",
    meta_token: str,
    gateway_token: str,
    base_url: str | None = None,
    force_refresh: bool = False,
    max_frames: int = 20,
    audio_analysis: bool = True,
    log_generation_input: bool = False,
    extras: dict[str, Any] | str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "storage": storage,
        "ad_id": ad_id,
        "force_refresh": force_refresh,
        "max_frames": max_frames,
        "audio_analysis": audio_analysis,
        "log_generation_input": log_generation_input,
    }
    if video_id:
        body["video_id"] = video_id
    if image_asset_id:
        body["image_asset_id"] = image_asset_id
    if extras is not None:
        body["extras"] = extras
    if config:
        body["config"] = config
    return _post_json(
        base_url or media_analysis_base_url(),
        ANALYSIS_PATH,
        media_analysis_headers(meta_token, gateway_token),
        body,
        timeout_sec=ANALYSIS_TIMEOUT_SEC,
    )


def translate_chinese_schema_prompt(
    *,
    gateway_token: str,
    base_url: str | None = None,
    source_prompt_name: str = "media_analysis_mcp/video_analysis_schema",
    target_prompt_name: str = "media_analysis_mcp/创意媒体分析快照结构",
    translation_prompt_name: str = "media_analysis_mcp/translate_schema_to_chineese",
    input_content: str = "",
    model: str = "",
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    body = {
        "source_prompt_name": source_prompt_name,
        "target_prompt_name": target_prompt_name,
        "translation_prompt_name": translation_prompt_name,
        "model": model,
        "force": force,
        "dry_run": dry_run,
    }
    if input_content:
        body["input_content"] = input_content
    return _post_json(
        base_url or media_analysis_base_url(),
        TRANSLATE_CHINESE_SCHEMA_PATH,
        mcp_gateway_headers(gateway_token),
        body,
        timeout_sec=ANALYSIS_TIMEOUT_SEC,
    )


def translate_creative_media_analysis_to_chinese(
    *,
    analysis: dict[str, Any],
    freeform_video_summary: str,
    gateway_token: str,
    base_url: str | None = None,
    schema_prompt_name: str = "media_analysis_mcp/创意媒体分析快照结构",
    translation_prompt_name: str = "media_analysis_mcp/translate_analysis_to_chinese",
    model: str = "",
    langfuse_trace_id: str = "",
    langfuse_parent_observation_id: str = "",
    langfuse_trace_name: str = "",
) -> dict[str, Any]:
    body = {
        "analysis": analysis,
        "freeform_video_summary": freeform_video_summary,
        "schema_prompt_name": schema_prompt_name,
        "translation_prompt_name": translation_prompt_name,
        "model": model,
    }
    if langfuse_trace_id:
        body["langfuse_trace_id"] = langfuse_trace_id
    if langfuse_parent_observation_id:
        body["langfuse_parent_observation_id"] = langfuse_parent_observation_id
    if langfuse_trace_name:
        body["langfuse_trace_name"] = langfuse_trace_name
    return _post_json(
        base_url or media_analysis_base_url(),
        TRANSLATE_CHINESE_ANALYSIS_PATH,
        mcp_gateway_headers(gateway_token),
        body,
        timeout_sec=ANALYSIS_TIMEOUT_SEC,
    )


def update_chinese_creative_media_analysis_snapshot(
    conn: Any,
    *,
    snapshot_id: int,
    translated_analysis: dict[str, Any],
    translated_freeform_video_summary: str,
) -> None:
    analysis_json = json.dumps(translated_analysis, ensure_ascii=False, sort_keys=True)
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {CHINESE_CREATIVE_MEDIA_ANALYSIS_SNAPSHOT_TABLE}
            SET
                "分析结果" = %s::jsonb,
                "视频自由摘要" = %s
            WHERE id = %s
            """,
            (analysis_json, translated_freeform_video_summary or None, snapshot_id),
        )


def redis_json_payload(redis_client: Any, key: str) -> dict[str, Any] | None:
    raw = redis_client.get(key)
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        payload = json.loads(str(raw))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def media_analysis_files_cache_matches(payload: dict[str, Any] | None, *, ad_id: str, media_id: str) -> bool:
    if not payload:
        return False
    return str(payload.get("ad_id") or "") == ad_id and str(payload.get("video_id") or "") == media_id


def media_analysis_cache_matches(
    payload: dict[str, Any] | None,
    *,
    ad_id: str,
    media_id: str,
    audio_analysis: bool,
    media_config: dict[str, Any] | None = None,
) -> bool:
    if not payload:
        return False
    if str(payload.get("ad_id") or "") != ad_id or str(payload.get("video_id") or "") != media_id:
        return False
    if not isinstance(payload.get("freeform_video_summary"), str):
        return False
    if not isinstance(payload.get("video_analysis"), dict):
        return False
    if audio_analysis and not isinstance(payload.get("audio_analysis"), dict):
        return False
    cached_config = payload.get("media_config")
    if not isinstance(cached_config, dict):
        cached_config = {}
    return cached_config == (media_config or {})


def media_analysis_registry_video_ids(conn: Any, ad_id: str) -> list[str]:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT video_ids
            FROM {AD_CREATIVE_REGISTRY_TABLE}
            WHERE ad_id = %s
              AND has_video = true
            """,
            (ad_id,),
        )
        rows = cursor.fetchall()

    video_ids: list[str] = []
    for row in rows:
        raw_video_ids = row[0] if row else None
        if isinstance(raw_video_ids, str):
            try:
                raw_video_ids = json.loads(raw_video_ids)
            except json.JSONDecodeError:
                raw_video_ids = []
        if not isinstance(raw_video_ids, list):
            continue
        for video_id in raw_video_ids:
            value = str(video_id or "").strip()
            if value:
                video_ids.append(value)
    return sorted(set(video_ids))


def recorded_media_analysis_video_ids(
    conn: Any,
    *,
    ad_id: str,
    partition_datetime: datetime | str,
    video_ids: list[str],
) -> set[str]:
    if not video_ids:
        return set()
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT video_id
            FROM {CREATIVE_MEDIA_ANALYSIS_TRAFFIC_TABLE}
            WHERE ad_id = %s
              AND media_type = 'video'
              AND partition_datetime = %s
              AND video_id = ANY(%s)
            """,
            (ad_id, _partition_datetime(partition_datetime), video_ids),
        )
        rows = cursor.fetchall()
    return {str(row[0]) for row in rows if row and row[0]}


def creative_media_analysis_skip_status(
    conn: Any,
    *,
    ad_id: str,
    partition_datetime: datetime | str,
    audio_analysis: bool,
    media_config: dict[str, Any] | None = None,
    redis_client: Any | None = None,
) -> dict[str, Any]:
    video_ids = media_analysis_registry_video_ids(conn, ad_id)
    if not video_ids:
        return {"skip": False, "reason": "no_registry_video_ids", "video_ids": []}

    client = redis_client if redis_client is not None else media_analysis_redis_client()
    if client is None:
        return {"skip": False, "reason": "redis_unavailable", "video_ids": video_ids}

    missing_files: list[str] = []
    missing_analysis: list[str] = []
    redis_keys: list[str] = []
    for video_id in video_ids:
        files_key = media_analysis_files_cache_key(ad_id, video_id)
        analysis_key = media_analysis_analysis_cache_key(ad_id, video_id)
        redis_keys.extend([files_key, analysis_key])
        if not media_analysis_files_cache_matches(
            redis_json_payload(client, files_key),
            ad_id=ad_id,
            media_id=video_id,
        ):
            missing_files.append(video_id)
        if not media_analysis_cache_matches(
            redis_json_payload(client, analysis_key),
            ad_id=ad_id,
            media_id=video_id,
            audio_analysis=audio_analysis,
            media_config=media_config,
        ):
            missing_analysis.append(video_id)

    if missing_files or missing_analysis:
        return {
            "skip": False,
            "reason": "redis_cache_missing",
            "video_ids": video_ids,
            "missing_files": missing_files,
            "missing_analysis": missing_analysis,
            "redis_keys": redis_keys,
        }

    recorded_video_ids = recorded_media_analysis_video_ids(
        conn,
        ad_id=ad_id,
        partition_datetime=partition_datetime,
        video_ids=video_ids,
    )
    missing_traffic = [video_id for video_id in video_ids if video_id not in recorded_video_ids]
    if missing_traffic:
        return {
            "skip": False,
            "reason": "traffic_snapshot_missing",
            "video_ids": video_ids,
            "missing_traffic": missing_traffic,
            "redis_keys": redis_keys,
        }

    return {
        "skip": True,
        "reason": "redis_cache_and_traffic_ready",
        "video_ids": video_ids,
        "redis_keys": redis_keys,
    }


def analysis_targets_from_download(
    download_payload: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Build per-media analysis targets from a download API response."""
    ad_id = str(download_payload.get("ad_id") or "")
    videos = download_payload.get("videos")
    targets: list[dict[str, Any]] = []
    if isinstance(videos, list):
        for video in videos:
            if not isinstance(video, dict):
                continue
            storage = video.get("storage")
            if not isinstance(storage, dict):
                continue
            if not storage_has_analyzable_visuals(storage, config=config):
                continue
            video_id = str(video.get("video_id") or video.get("creative_video_id") or "")
            if not video_id:
                continue
            creative_id = str(video.get("creative_id") or download_payload.get("creative_id") or "")
            targets.append(
                {
                    "ad_id": ad_id,
                    "video_id": video_id,
                    "creative_id": creative_id,
                    "storage": storage,
                    "media_type": "video",
                }
            )

    image_rows = download_payload.get("images")
    if isinstance(image_rows, list):
        for image in image_rows:
            if not isinstance(image, dict):
                continue
            storage = image.get("storage")
            if not isinstance(storage, dict):
                continue
            if not storage_has_analyzable_visuals(storage, config=config):
                continue
            image_asset_id = str(
                image.get("image_asset_id")
                or storage.get("image_asset_id")
                or image.get("creative_id")
                or download_payload.get("creative_id")
                or ""
            )
            if not image_asset_id:
                continue
            creative_id = str(image.get("creative_id") or download_payload.get("creative_id") or "")
            targets.append(
                {
                    "ad_id": ad_id,
                    "video_id": "",
                    "image_asset_id": image_asset_id,
                    "creative_id": creative_id,
                    "storage": storage,
                    "media_type": "image",
                }
            )
    return targets


def image_gcs_uri_from_download(
    download_payload: dict[str, Any],
    *,
    ad_id: str,
    image_asset_id: str,
) -> str | None:
    """Return the gs:// URI for the first downloaded image from a download API response."""
    del ad_id
    seen: set[str] = set()
    uris: list[str] = []

    def add(items: Any) -> None:
        if not isinstance(items, list):
            return
        for item in items:
            if isinstance(item, str) and item.startswith("gs://") and item not in seen:
                seen.add(item)
                uris.append(item)

    add(download_payload.get("gcs_files"))
    images = download_payload.get("images")
    if not isinstance(images, list):
        return None

    for image in images:
        if not isinstance(image, dict):
            continue
        asset_id = str(image.get("image_asset_id") or "")
        if asset_id != image_asset_id:
            continue
        storage = image.get("storage")
        if isinstance(storage, dict):
            add(storage.get("gcs_files"))
        image_rels = []
        if isinstance(storage, dict):
            image_rels = storage.get("images") if isinstance(storage.get("images"), list) else []
        prefix = str(download_payload.get("storage_prefix") or "").rstrip("/")
        if prefix.startswith("gs://") and image_rels:
            add([f"{prefix}/{str(image_rels[0]).lstrip('/')}"])
        for uri in uris:
            if "/images/" in uri:
                return uri
    for uri in uris:
        if "/images/" in uri:
            return uri
    return uris[0] if uris else None


def upsert_creative_media_analysis(
    conn: Any,
    *,
    campaign_id: str,
    adset_id: str,
    ad_id: str,
    creative_id: str,
    video_id: str,
    partition_datetime: datetime | str,
    analysis: dict[str, Any],
    media_type: str = "video",
    image_asset_id: str = "",
    video_gcs_uri: str | None = None,
    video_preview_url: str | None = None,
    image_gcs_uri: str | None = None,
    image_preview_url: str | None = None,
) -> int:
    """Persist final creative media analysis and partition linkage."""
    analysis_payload = dict(analysis)
    if media_type == "image" and "media_type" not in analysis_payload:
        analysis_payload["media_type"] = "image"
    for key in ("primary_text", "headline", "description"):
        value = str(analysis_payload.get(key) or "")
        if value:
            analysis_payload[key] = value
    analysis_json = json.dumps(analysis_payload, ensure_ascii=False, sort_keys=True)
    freeform_video_summary = str(analysis.get("freeform_video_summary") or "")
    video_analysis_schema_name = str(analysis.get("video_analysis_schema_name") or "")
    partition_value = _partition_datetime(partition_datetime)

    snapshot_sql = f"""
        INSERT INTO {CREATIVE_MEDIA_ANALYSIS_SNAPSHOT_TABLE} AS target (
            campaign_id,
            adset_id,
            ad_id,
            creative_id,
            media_type,
            video_id,
            image_asset_id,
            analysis,
            freeform_video_summary,
            video_analysis_schema_name,
            video_gcs_uri,
            video_preview_url,
            image_gcs_uri,
            image_preview_url
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (campaign_id, adset_id, ad_id, media_type, video_id, image_asset_id) DO UPDATE
        SET
            creative_id = EXCLUDED.creative_id,
            analysis = EXCLUDED.analysis,
            freeform_video_summary = EXCLUDED.freeform_video_summary,
            video_analysis_schema_name = EXCLUDED.video_analysis_schema_name,
            video_gcs_uri = COALESCE(EXCLUDED.video_gcs_uri, target.video_gcs_uri),
            video_preview_url = COALESCE(EXCLUDED.video_preview_url, target.video_preview_url),
            image_gcs_uri = COALESCE(EXCLUDED.image_gcs_uri, target.image_gcs_uri),
            image_preview_url = COALESCE(EXCLUDED.image_preview_url, target.image_preview_url),
            updated_at = now(),
            update_count = target.update_count + 1
        WHERE target.creative_id IS DISTINCT FROM EXCLUDED.creative_id
           OR target.analysis IS DISTINCT FROM EXCLUDED.analysis
           OR target.freeform_video_summary IS DISTINCT FROM EXCLUDED.freeform_video_summary
           OR target.video_analysis_schema_name IS DISTINCT FROM EXCLUDED.video_analysis_schema_name
           OR target.video_gcs_uri IS DISTINCT FROM COALESCE(EXCLUDED.video_gcs_uri, target.video_gcs_uri)
           OR target.video_preview_url IS DISTINCT FROM COALESCE(EXCLUDED.video_preview_url, target.video_preview_url)
           OR target.image_gcs_uri IS DISTINCT FROM COALESCE(EXCLUDED.image_gcs_uri, target.image_gcs_uri)
           OR target.image_preview_url IS DISTINCT FROM COALESCE(EXCLUDED.image_preview_url, target.image_preview_url)
        RETURNING id
    """
    traffic_sql = f"""
        INSERT INTO {CREATIVE_MEDIA_ANALYSIS_TRAFFIC_TABLE} AS target (
            analysis_snapshot_id,
            partition_datetime,
            campaign_id,
            adset_id,
            ad_id,
            creative_id,
            media_type,
            video_id,
            image_asset_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (partition_datetime, campaign_id, adset_id, ad_id, media_type, video_id, image_asset_id) DO UPDATE
        SET
            analysis_snapshot_id = EXCLUDED.analysis_snapshot_id,
            creative_id = EXCLUDED.creative_id,
            updated_at = now(),
            update_count = target.update_count + 1
        WHERE target.analysis_snapshot_id IS DISTINCT FROM EXCLUDED.analysis_snapshot_id
           OR target.creative_id IS DISTINCT FROM EXCLUDED.creative_id
    """
    with conn.cursor() as cursor:
        cursor.execute(
            snapshot_sql,
            (
                campaign_id,
                adset_id,
                ad_id,
                creative_id,
                media_type,
                video_id,
                image_asset_id,
                analysis_json,
                freeform_video_summary or None,
                video_analysis_schema_name or None,
                video_gcs_uri or None,
                video_preview_url or None,
                image_gcs_uri or None,
                image_preview_url or None,
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
                  AND media_type = %s
                  AND video_id = %s
                  AND image_asset_id = %s
                """,
                (campaign_id, adset_id, ad_id, media_type, video_id, image_asset_id),
            )
            row = cursor.fetchone()
        if row is None:
            raise RuntimeError(
                "Could not resolve creative media analysis snapshot id for "
                f"ad_id={ad_id} media_type={media_type} video_id={video_id} "
                f"image_asset_id={image_asset_id}"
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
                creative_id,
                media_type,
                video_id,
                image_asset_id,
            ),
        )
    return snapshot_id


def _partition_datetime(value: datetime | str) -> datetime | str:
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value

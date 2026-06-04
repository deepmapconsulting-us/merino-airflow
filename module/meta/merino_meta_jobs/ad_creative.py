"""Sync Meta ad creative media registry into marketing.meta_ad_creative."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from merino_meta_jobs.facebook_graph import MetaGraphClient, ensure_act_prefix
from merino_meta_jobs.traffic import (
    DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS,
    _object_should_import,
    account_ids_from_text,
    traffic_accounts_from_config,
)

COMPANY = "merino"
PLATFORM = "meta"
SOURCE = "facebook"

AD_CREATIVE_TABLE = "marketing.meta_ad_creative"

CREATIVE_FIELDS = (
    "id,name,status,thumbnail_url,image_url,image_hash,object_story_spec,object_type,"
    "body,title,asset_feed_spec{images,videos,bodies,titles,descriptions}"
)

INSERT_COLUMNS = (
    "ad_id",
    "creative_id",
    "adset_id",
    "campaign_id",
    "source_account_id",
    "company",
    "platform",
    "source",
    "object_type",
    "has_video",
    "has_image",
    "is_primary",
    "video_ids",
    "config_snapshot_uri",
)

CHANGE_COLUMNS = (
    "object_type",
    "has_video",
    "has_image",
    "is_primary",
    "video_ids",
    "config_snapshot_uri",
)


def ads_for_creative_registry_from_snapshot(
    snapshot: dict[str, Any],
    *,
    active_accounts_value: str | None = None,
    lookup_window_days: int | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Active or recently updated ads with a creative_id from the campaign config snapshot."""
    if lookup_window_days is None:
        lookup_window_days = DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=lookup_window_days)
    active_accounts = {
        ensure_act_prefix(account_id)
        for account_id in account_ids_from_text(active_accounts_value or "")
    }

    selected: list[dict[str, Any]] = []
    for account in traffic_accounts_from_config(
        snapshot,
        active_accounts_value=active_accounts_value,
        lookup_window_days=lookup_window_days,
        now=now,
    ):
        account_id = ensure_act_prefix(str(account.get("id") or ""))
        if active_accounts and account_id not in active_accounts:
            continue
        for campaign in account.get("campaigns", []):
            campaign_id = str(campaign.get("id") or "")
            if not campaign_id:
                continue
            for adset in campaign.get("adsets", []):
                adset_id = str(adset.get("id") or "")
                if not adset_id:
                    continue
                for ad in adset.get("ads", []):
                    if not isinstance(ad, dict) or not ad.get("id"):
                        continue
                    creative_id = ad.get("creative_id")
                    if not creative_id:
                        continue
                    if not _object_should_import(ad, cutoff):
                        continue
                    selected.append(
                        {
                            "ad_id": str(ad["id"]),
                            "adset_id": adset_id,
                            "campaign_id": campaign_id,
                            "source_account_id": account_id,
                            "creative_id": str(creative_id),
                            "status": ad.get("status"),
                        }
                    )
    return selected


def fetch_and_sync_ad_creatives(
    client: MetaGraphClient,
    conn,
    ads: list[dict[str, Any]],
    *,
    config_snapshot_uri: str = "",
) -> dict[str, int]:
    rows: list[tuple[Any, ...]] = []
    fetched_creatives = 0
    for ad in ads:
        ad_id = str(ad.get("ad_id") or "")
        if not ad_id:
            continue
        primary_creative_id = str(ad.get("creative_id") or "")
        creative_rows = client.get_all(
            f"{ad_id}/adcreatives",
            {"fields": CREATIVE_FIELDS, "limit": 50},
        )
        if not creative_rows and primary_creative_id:
            creative_payload = client.get(primary_creative_id, {"fields": CREATIVE_FIELDS})
            if creative_payload.get("id"):
                creative_rows = [creative_payload]

        seen_creative_ids: set[str] = set()
        for creative in creative_rows:
            creative_id = str(creative.get("id") or "")
            if not creative_id or creative_id in seen_creative_ids:
                continue
            seen_creative_ids.add(creative_id)
            fetched_creatives += 1
            rows.append(
                _registry_row(
                    ad,
                    creative,
                    is_primary=creative_id == primary_creative_id,
                    config_snapshot_uri=config_snapshot_uri,
                )
            )

    upserted = upsert_ad_creative_rows(conn, rows)
    return {
        "ads": len(ads),
        "creatives_fetched": fetched_creatives,
        "rows_upserted": upserted,
    }


def upsert_ad_creative_rows(conn, rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0

    columns = ", ".join(INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    update_columns = [column for column in INSERT_COLUMNS if column not in {"ad_id", "creative_id"}]
    assignments = ",\n            ".join(
        [
            "last_synced_at = now()",
            "update_count = target.update_count + 1",
            *[f"{column} = EXCLUDED.{column}" for column in update_columns],
        ]
    )
    changed = "\n            OR ".join(
        f"target.{column} IS DISTINCT FROM EXCLUDED.{column}" for column in CHANGE_COLUMNS
    )
    sql = f"""
        INSERT INTO {AD_CREATIVE_TABLE} AS target ({columns})
        VALUES ({placeholders})
        ON CONFLICT (ad_id, creative_id) DO UPDATE
        SET {assignments}
        WHERE {changed}
    """
    with conn.cursor() as cursor:
        cursor.executemany(sql, rows)
    return len(rows)


def _registry_row(
    ad: dict[str, Any],
    creative: dict[str, Any],
    *,
    is_primary: bool,
    config_snapshot_uri: str,
) -> tuple[Any, ...]:
    video_ids = creative_video_ids(creative)
    has_video = bool(video_ids)
    has_image = has_image_creative(creative, has_video=has_video)
    row = {
        "ad_id": str(ad.get("ad_id") or ""),
        "creative_id": str(creative.get("id") or ""),
        "adset_id": str(ad.get("adset_id") or ""),
        "campaign_id": str(ad.get("campaign_id") or ""),
        "source_account_id": str(ad.get("source_account_id") or ""),
        "company": COMPANY,
        "platform": PLATFORM,
        "source": SOURCE,
        "object_type": str(creative.get("object_type") or "") or None,
        "has_video": has_video,
        "has_image": has_image,
        "is_primary": is_primary,
        "video_ids": video_ids,
        "config_snapshot_uri": config_snapshot_uri or None,
    }
    return tuple(_serialize_value(row.get(column)) for column in INSERT_COLUMNS)


def creative_video_ids(creative: dict[str, Any] | None) -> list[str]:
    if not isinstance(creative, dict):
        return []

    video_ids: list[str] = []
    object_story_spec = creative.get("object_story_spec")
    if isinstance(object_story_spec, dict):
        video_data = object_story_spec.get("video_data")
        if isinstance(video_data, dict):
            video_id = str(video_data.get("video_id") or "").strip()
            if video_id:
                video_ids.append(video_id)

    asset_feed_spec = creative.get("asset_feed_spec")
    if isinstance(asset_feed_spec, dict):
        videos = asset_feed_spec.get("videos")
        if isinstance(videos, list):
            for row in videos:
                if not isinstance(row, dict):
                    continue
                video_id = str(row.get("video_id") or "").strip()
                if video_id:
                    video_ids.append(video_id)

    return sorted(set(video_ids))


def has_image_creative(creative: dict[str, Any] | None, *, has_video: bool) -> bool:
    if not isinstance(creative, dict):
        return False
    if creative_image_urls(creative) or creative_image_hashes(creative):
        return True
    object_type = str(creative.get("object_type") or "").upper()
    return object_type in {"PHOTO", "SHARE"} and not has_video


def creative_image_urls(creative: dict[str, Any] | None) -> list[str]:
    if not isinstance(creative, dict):
        return []

    urls: list[str] = []
    for key in ("image_url", "thumbnail_url"):
        value = creative.get(key)
        if value:
            urls.append(str(value))

    image_urls_for_viewing = creative.get("image_urls_for_viewing")
    if isinstance(image_urls_for_viewing, list):
        urls.extend(str(url) for url in image_urls_for_viewing if url)

    object_story_spec = creative.get("object_story_spec")
    if isinstance(object_story_spec, dict):
        link_data = object_story_spec.get("link_data")
        if isinstance(link_data, dict):
            for key in ("picture", "image_url"):
                value = link_data.get(key)
                if value:
                    urls.append(str(value))

    asset_feed_spec = creative.get("asset_feed_spec")
    if isinstance(asset_feed_spec, dict):
        images = asset_feed_spec.get("images")
        if isinstance(images, list):
            for image in images:
                if isinstance(image, dict) and image.get("url"):
                    urls.append(str(image["url"]))

    return sorted(set(urls))


def creative_image_hashes(creative: dict[str, Any] | None) -> list[str]:
    if not isinstance(creative, dict):
        return []

    hashes: list[str] = []
    if creative.get("image_hash"):
        hashes.append(str(creative["image_hash"]))

    object_story_spec = creative.get("object_story_spec")
    if isinstance(object_story_spec, dict):
        link_data = object_story_spec.get("link_data")
        if isinstance(link_data, dict) and link_data.get("image_hash"):
            hashes.append(str(link_data["image_hash"]))

    asset_feed_spec = creative.get("asset_feed_spec")
    if isinstance(asset_feed_spec, dict):
        images = asset_feed_spec.get("images")
        if isinstance(images, list):
            for image in images:
                if isinstance(image, dict) and image.get("hash"):
                    hashes.append(str(image["hash"]))

    return sorted(set(hashes))


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value

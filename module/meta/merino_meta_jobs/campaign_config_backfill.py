"""Scan historical facebook_campaign_config_update GCS snapshots.

Each snapshot is a point-in-time inventory of Meta campaigns, adsets, and ads
(active, paused, archived, etc.). This module walks every snapshot.json under
the config prefix and builds:

- per-snapshot counts (how many objects existed at that moment)
- a union inventory with first/last seen timestamps and active observation windows

Dimension tables ``marketing.meta_campaign`` / ``meta_adset`` / ``meta_ad`` only
store the latest merged state from ``meta_object_property_sync``; they do not
retain historical status timelines. Use this scan (or a future history table fed
by it) for backfill planning.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

from merino_meta_jobs.facebook_graph import ensure_act_prefix
from merino_meta_jobs.object_property import flatten_config_snapshot

DEFAULT_CONFIG_GCS_BUCKET = "airflow-run-us-west2"
DEFAULT_CONFIG_GCS_PREFIX = "facebook_campaign_config_update"

_OBJECT_ID_KEYS = {
    "campaign": "campaign_id",
    "adset": "adset_id",
    "ad": "ad_id",
}


@dataclass(frozen=True)
class ConfigSnapshotRef:
    uri: str
    run_date: str
    run_datetime: str


@dataclass
class SnapshotSummary:
    snapshot_uri: str
    observed_at: str
    run_date: str
    run_datetime: str
    campaign_count: int
    adset_count: int
    ad_count: int
    active_campaign_count: int
    active_adset_count: int
    active_ad_count: int


@dataclass
class ObjectInventoryRow:
    object_level: str
    object_id: str
    source_account_id: str
    campaign_id: str | None
    adset_id: str | None
    name: str | None
    created_at: str | None
    snapshot_appearances: int
    first_observed_at: str
    last_observed_at: str
    first_active_observed_at: str | None
    last_active_observed_at: str | None
    statuses_seen: list[str]
    last_status: str | None


def gcs_uri(bucket: str, object_name: str) -> str:
    return f"gs://{bucket}/{object_name}"


def parse_config_snapshot_object_name(
    object_name: str,
    *,
    prefix: str,
    bucket: str = DEFAULT_CONFIG_GCS_BUCKET,
) -> ConfigSnapshotRef | None:
    """Parse ``{prefix}/{date}/{datetime}/snapshot.json``."""
    suffix = "/snapshot.json"
    if not object_name.endswith(suffix):
        return None
    body = object_name[: -len(suffix)]
    if not body.startswith(f"{prefix}/"):
        return None
    parts = body.split("/")
    if len(parts) != 3:
        return None
    _prefix, run_date, run_datetime = parts
    if _prefix != prefix:
        return None
    return ConfigSnapshotRef(
        uri=gcs_uri(bucket, object_name),
        run_date=run_date,
        run_datetime=run_datetime,
    )


def list_config_snapshot_refs(
    storage_client: Any,
    *,
    bucket: str = DEFAULT_CONFIG_GCS_BUCKET,
    prefix: str = DEFAULT_CONFIG_GCS_PREFIX,
) -> list[ConfigSnapshotRef]:
    refs: list[ConfigSnapshotRef] = []
    for blob in storage_client.list_blobs(bucket, prefix=f"{prefix}/"):
        object_name = str(getattr(blob, "name", "") or "")
        ref = parse_config_snapshot_object_name(object_name, prefix=prefix, bucket=bucket)
        if ref is not None:
            refs.append(ref)
    return sorted(refs, key=lambda item: (item.run_date, item.run_datetime))


def _observed_at(snapshot: dict[str, Any], ref: ConfigSnapshotRef) -> str:
    generated_at = snapshot.get("generated_at")
    if isinstance(generated_at, str) and generated_at.strip():
        return generated_at
    return ref.run_datetime


def _is_active(status: Any) -> bool:
    return str(status or "").upper() == "ACTIVE"


def _inventory_key(level: str, row: dict[str, Any]) -> tuple[str, str, str]:
    object_id = str(row[_OBJECT_ID_KEYS[level]])
    account_id = ensure_act_prefix(str(row["source_account_id"]))
    return level, object_id, account_id


def _merge_inventory_row(
    inventory: dict[tuple[str, str, str], ObjectInventoryRow],
    *,
    level: str,
    row: dict[str, Any],
    observed_at: str,
) -> None:
    key = _inventory_key(level, row)
    status = str(row.get("status") or "") or None
    campaign_id = str(row.get("campaign_id") or "") or None
    adset_id = str(row.get("adset_id") or "") or None if level in {"adset", "ad"} else None
    created_at = row.get("created_at")
    if isinstance(created_at, str):
        created_at = created_at or None
    else:
        created_at = None

    existing = inventory.get(key)
    if existing is None:
        inventory[key] = ObjectInventoryRow(
            object_level=level,
            object_id=key[1],
            source_account_id=key[2],
            campaign_id=campaign_id,
            adset_id=adset_id,
            name=row.get("name"),
            created_at=created_at,
            snapshot_appearances=1,
            first_observed_at=observed_at,
            last_observed_at=observed_at,
            first_active_observed_at=observed_at if _is_active(status) else None,
            last_active_observed_at=observed_at if _is_active(status) else None,
            statuses_seen=[status] if status else [],
            last_status=status,
        )
        return

    existing.snapshot_appearances += 1
    existing.last_observed_at = observed_at
    if row.get("name"):
        existing.name = row.get("name")
    if created_at and not existing.created_at:
        existing.created_at = created_at
    if status and status not in existing.statuses_seen:
        existing.statuses_seen.append(status)
    existing.last_status = status
    if _is_active(status):
        if existing.first_active_observed_at is None:
            existing.first_active_observed_at = observed_at
        existing.last_active_observed_at = observed_at


def summarize_flat_snapshot(
    flat: dict[str, list[dict[str, Any]]],
    *,
    snapshot_uri: str,
    observed_at: str,
    run_date: str,
    run_datetime: str,
) -> SnapshotSummary:
    campaigns = flat.get("campaigns") or []
    adsets = flat.get("adsets") or []
    ads = flat.get("ads") or []
    return SnapshotSummary(
        snapshot_uri=snapshot_uri,
        observed_at=observed_at,
        run_date=run_date,
        run_datetime=run_datetime,
        campaign_count=len(campaigns),
        adset_count=len(adsets),
        ad_count=len(ads),
        active_campaign_count=sum(1 for row in campaigns if _is_active(row.get("status"))),
        active_adset_count=sum(1 for row in adsets if _is_active(row.get("status"))),
        active_ad_count=sum(1 for row in ads if _is_active(row.get("status"))),
    )


def scan_config_snapshots(
    storage_client: Any,
    read_json: Any,
    *,
    bucket: str = DEFAULT_CONFIG_GCS_BUCKET,
    prefix: str = DEFAULT_CONFIG_GCS_PREFIX,
    max_snapshots: int | None = None,
) -> dict[str, Any]:
    """Read each historical config snapshot and build summary + inventory."""
    refs = list_config_snapshot_refs(storage_client, bucket=bucket, prefix=prefix)
    if max_snapshots is not None:
        refs = refs[:max_snapshots]

    snapshot_summaries: list[SnapshotSummary] = []
    inventory: dict[tuple[str, str, str], ObjectInventoryRow] = {}

    for ref in refs:
        snapshot = read_json(storage_client, ref.uri)
        if not isinstance(snapshot, dict):
            continue
        observed_at = _observed_at(snapshot, ref)
        flat = flatten_config_snapshot(snapshot)
        snapshot_summaries.append(
            summarize_flat_snapshot(
                flat,
                snapshot_uri=ref.uri,
                observed_at=observed_at,
                run_date=ref.run_date,
                run_datetime=ref.run_datetime,
            )
        )
        for level, rows in (
            ("campaign", flat.get("campaigns") or []),
            ("adset", flat.get("adsets") or []),
            ("ad", flat.get("ads") or []),
        ):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                _merge_inventory_row(inventory, level=level, row=row, observed_at=observed_at)

    inventory_rows = sorted(
        inventory.values(),
        key=lambda row: (row.object_level, row.source_account_id, row.object_id),
    )
    return build_scan_report(snapshot_summaries, inventory_rows)


def build_scan_report(
    snapshot_summaries: list[SnapshotSummary],
    inventory_rows: list[ObjectInventoryRow],
) -> dict[str, Any]:
    campaigns = [row for row in inventory_rows if row.object_level == "campaign"]
    adsets = [row for row in inventory_rows if row.object_level == "adset"]
    ads = [row for row in inventory_rows if row.object_level == "ad"]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_count": len(snapshot_summaries),
        "unique_campaigns": len(campaigns),
        "unique_adsets": len(adsets),
        "unique_ads": len(ads),
        "snapshot_summaries": [asdict(item) for item in snapshot_summaries],
        "inventory": {
            "campaigns": [asdict(item) for item in campaigns],
            "adsets": [asdict(item) for item in adsets],
            "ads": [asdict(item) for item in ads],
        },
    }


def iter_snapshot_refs_from_names(
    object_names: Iterator[str],
    *,
    prefix: str = DEFAULT_CONFIG_GCS_PREFIX,
) -> list[ConfigSnapshotRef]:
    refs: list[ConfigSnapshotRef] = []
    for object_name in object_names:
        ref = parse_config_snapshot_object_name(object_name, prefix=prefix)
        if ref is not None:
            refs.append(ref)
    return sorted(refs, key=lambda item: (item.run_date, item.run_datetime))

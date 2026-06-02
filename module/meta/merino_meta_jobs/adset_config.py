"""Sync Meta ad set targeting config into marketing.meta_adset_config (SCD Type 2)."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from merino_meta_jobs.facebook_graph import MetaGraphClient

COMPANY = "merino"
PLATFORM = "meta"
SOURCE = "facebook"
REPORT_TIMEZONE = "America/Los_Angeles"

ADSET_CONFIG_FIELDS = "id,name,campaign_id,status,targeting"
ADSET_CONFIG_TABLE = "marketing.meta_adset_config"

INSERT_COLUMNS = (
    "adset_id",
    "campaign_id",
    "source_account_id",
    "company",
    "platform",
    "source",
    "observed_at",
    "observed_date",
    "valid_from",
    "valid_to",
    "config_version",
    "config_hash",
    "targeting",
    "age_min",
    "age_max",
    "genders",
    "advantage_audience",
    "geo_countries",
    "config_snapshot_uri",
)


def active_adsets_from_flat(flat: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Return ad sets whose campaign and adset status are ACTIVE."""
    active_campaign_ids = {
        str(row["campaign_id"])
        for row in flat.get("campaigns", [])
        if row.get("campaign_id") and _is_active(row.get("status"))
    }
    return [
        row
        for row in flat.get("adsets", [])
        if row.get("adset_id")
        and str(row.get("campaign_id") or "") in active_campaign_ids
        and _is_active(row.get("status"))
    ]


def fetch_adset_targeting(client: MetaGraphClient, adset_id: str) -> dict[str, Any]:
    payload = client.get(adset_id, {"fields": ADSET_CONFIG_FIELDS})
    return payload if isinstance(payload, dict) else {}


def canonical_targeting(targeting: Any) -> str:
    normalized = targeting if isinstance(targeting, dict) else {}
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def config_hash(targeting: Any) -> str:
    digest = hashlib.sha256(canonical_targeting(targeting).encode("utf-8"))
    return digest.hexdigest()


def extract_targeting_columns(targeting: Any) -> dict[str, Any]:
    if not isinstance(targeting, dict):
        targeting = {}

    automation = targeting.get("targeting_automation")
    advantage_audience = None
    if isinstance(automation, dict) and "advantage_audience" in automation:
        advantage_audience = bool(int(automation["advantage_audience"]))

    geo = targeting.get("geo_locations")
    geo_countries = None
    if isinstance(geo, dict):
        countries = geo.get("countries")
        if isinstance(countries, list):
            geo_countries = countries

    genders = targeting.get("genders")
    if genders is not None and not isinstance(genders, list):
        genders = None

    return {
        "age_min": _int_or_none(targeting.get("age_min")),
        "age_max": _int_or_none(targeting.get("age_max")),
        "genders": genders,
        "advantage_audience": advantage_audience,
        "geo_countries": geo_countries,
    }


def adset_config_row_from_graph(
    graph_row: dict[str, Any],
    *,
    source_account_id: str,
    observed_at: datetime,
    config_snapshot_uri: str = "",
) -> dict[str, Any]:
    targeting = graph_row.get("targeting") if isinstance(graph_row.get("targeting"), dict) else {}
    extracted = extract_targeting_columns(targeting)
    observed_date = observed_at.astimezone(ZoneInfo(REPORT_TIMEZONE)).date()
    return {
        "adset_id": str(graph_row.get("id") or ""),
        "campaign_id": str(graph_row.get("campaign_id") or ""),
        "source_account_id": source_account_id,
        "company": COMPANY,
        "platform": PLATFORM,
        "source": SOURCE,
        "observed_at": observed_at,
        "observed_date": observed_date,
        "valid_from": observed_at,
        "valid_to": None,
        "config_hash": config_hash(targeting),
        "targeting": targeting,
        "config_snapshot_uri": config_snapshot_uri or None,
        **extracted,
    }


def load_current_hashes(conn) -> dict[str, str]:
    sql = f"""
        SELECT adset_id, config_hash
        FROM {ADSET_CONFIG_TABLE}
        WHERE valid_to IS NULL
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return {str(row[0]): str(row[1]) for row in cursor.fetchall()}


def next_config_version(conn, adset_id: str) -> int:
    sql = f"""
        SELECT COALESCE(MAX(config_version), 0) + 1
        FROM {ADSET_CONFIG_TABLE}
        WHERE adset_id = %s
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (adset_id,))
        row = cursor.fetchone()
    return int(row[0]) if row else 1


def close_open_version(conn, adset_id: str, valid_to: datetime) -> None:
    sql = f"""
        UPDATE {ADSET_CONFIG_TABLE}
        SET valid_to = %s
        WHERE adset_id = %s AND valid_to IS NULL
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (valid_to, adset_id))


def insert_config_version(
    conn,
    row: dict[str, Any],
    *,
    current_hashes: dict[str, str],
) -> bool:
    """Insert a new config version when hash changed. Returns True if inserted."""
    adset_id = row["adset_id"]
    config_hash_value = row["config_hash"]
    if current_hashes.get(adset_id) == config_hash_value:
        return False

    observed_at = row["observed_at"]
    if not isinstance(observed_at, datetime):
        observed_at = datetime.now(timezone.utc)
    elif observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)

    row = {**row, "observed_at": observed_at, "valid_from": observed_at}
    row["config_version"] = next_config_version(conn, adset_id)

    close_open_version(conn, adset_id, observed_at)

    columns = ", ".join(INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    values = tuple(_serialize_value(row.get(column)) for column in INSERT_COLUMNS)
    sql = f"""
        INSERT INTO {ADSET_CONFIG_TABLE} ({columns})
        VALUES ({placeholders})
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, values)

    current_hashes[adset_id] = config_hash_value
    return True


def sync_adset_config_versions(
    conn,
    rows: list[dict[str, Any]],
) -> dict[str, int]:
    current_hashes = load_current_hashes(conn)
    inserted = 0
    skipped = 0
    for row in rows:
        if insert_config_version(conn, row, current_hashes=current_hashes):
            inserted += 1
        else:
            skipped += 1
    return {"inserted": inserted, "skipped": skipped, "fetched": len(rows)}


def fetch_active_adset_configs(
    client: MetaGraphClient,
    active_adsets: list[dict[str, Any]],
    *,
    observed_at: datetime | None = None,
    config_snapshot_uri: str = "",
) -> list[dict[str, Any]]:
    observed_at = observed_at or datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    for adset in active_adsets:
        adset_id = str(adset.get("adset_id") or "")
        if not adset_id:
            continue
        graph_row = fetch_adset_targeting(client, adset_id)
        if not graph_row.get("id"):
            continue
        rows.append(
            adset_config_row_from_graph(
                graph_row,
                source_account_id=str(adset.get("source_account_id") or ""),
                observed_at=observed_at,
                config_snapshot_uri=config_snapshot_uri,
            )
        )
    return rows


def _is_active(status: Any) -> bool:
    return str(status or "").upper() == "ACTIVE"


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value

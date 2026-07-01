"""Sync Meta ad set targeting and budget config into marketing history tables."""

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

ADSET_CONFIG_FIELDS = (
    "id,name,campaign_id,status,daily_budget,lifetime_budget,optimization_goal,"
    "billing_event,bid_strategy,targeting"
)
ADSET_CONFIG_TABLE = "marketing.meta_adset_config"
ADSET_TARGETING_DAILY_TABLE = "marketing.meta_adset_targeting_daily_snapshot"
ADSET_BUDGET_HISTORY_TABLE = "marketing.meta_adset_budget_history"

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

TARGETING_DAILY_COLUMNS = (
    "observed_date",
    "observed_at",
    "adset_id",
    "campaign_id",
    "source_account_id",
    "company",
    "platform",
    "source",
    "targeting",
    "targeting_hash",
    "age_min",
    "age_max",
    "genders",
    "advantage_audience",
    "geo_countries",
    "flexible_spec",
    "interests",
    "behaviors",
    "custom_audiences",
    "excluded_custom_audiences",
    "config_snapshot_uri",
)

BUDGET_COLUMNS = (
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
    "budget_version",
    "budget_hash",
    "daily_budget",
    "lifetime_budget",
    "bid_strategy",
    "optimization_goal",
    "billing_event",
    "status",
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


def canonical_budget(row: dict[str, Any]) -> str:
    payload = {
        "daily_budget": _int_or_none(row.get("daily_budget")),
        "lifetime_budget": _int_or_none(row.get("lifetime_budget")),
        "bid_strategy": _clean_text(row.get("bid_strategy")),
        "optimization_goal": _clean_text(row.get("optimization_goal")),
        "billing_event": _clean_text(row.get("billing_event")),
        "status": _clean_text(row.get("status")),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def budget_hash(row: dict[str, Any]) -> str:
    digest = hashlib.sha256(canonical_budget(row).encode("utf-8"))
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
        "flexible_spec": _json_list_or_none(targeting.get("flexible_spec")),
        "interests": _targeting_items(targeting, "interests"),
        "behaviors": _targeting_items(targeting, "behaviors"),
        "custom_audiences": _json_list_or_none(targeting.get("custom_audiences")),
        "excluded_custom_audiences": _json_list_or_none(targeting.get("excluded_custom_audiences")),
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
        "targeting_hash": config_hash(targeting),
        "targeting": targeting,
        "daily_budget": _int_or_none(graph_row.get("daily_budget")),
        "lifetime_budget": _int_or_none(graph_row.get("lifetime_budget")),
        "bid_strategy": _clean_text(graph_row.get("bid_strategy")),
        "optimization_goal": _clean_text(graph_row.get("optimization_goal")),
        "billing_event": _clean_text(graph_row.get("billing_event")),
        "status": _clean_text(graph_row.get("status")),
        "budget_hash": budget_hash(graph_row),
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


def sync_adset_targeting_daily_snapshots(
    conn,
    rows: list[dict[str, Any]],
) -> dict[str, int]:
    columns = ", ".join(TARGETING_DAILY_COLUMNS)
    placeholders = ", ".join(["%s"] * len(TARGETING_DAILY_COLUMNS))
    update_columns = [
        column
        for column in TARGETING_DAILY_COLUMNS
        if column not in {"observed_date", "adset_id"}
    ]
    assignments = ",\n            ".join(
        [f"{column} = EXCLUDED.{column}" for column in update_columns]
        + ["record_updated_at = now()", "update_count = marketing.meta_adset_targeting_daily_snapshot.update_count + 1"]
    )
    sql = f"""
        INSERT INTO {ADSET_TARGETING_DAILY_TABLE} ({columns})
        VALUES ({placeholders})
        ON CONFLICT (observed_date, adset_id) DO UPDATE SET
            {assignments}
    """
    with conn.cursor() as cursor:
        for row in rows:
            cursor.execute(sql, tuple(_serialize_value(row.get(column)) for column in TARGETING_DAILY_COLUMNS))
    return {"daily_upserted": len(rows)}


def load_current_budget_hashes(conn) -> dict[str, str]:
    sql = f"""
        SELECT adset_id, budget_hash
        FROM {ADSET_BUDGET_HISTORY_TABLE}
        WHERE valid_to IS NULL
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return {str(row[0]): str(row[1]) for row in cursor.fetchall()}


def next_budget_version(conn, adset_id: str) -> int:
    sql = f"""
        SELECT COALESCE(MAX(budget_version), 0) + 1
        FROM {ADSET_BUDGET_HISTORY_TABLE}
        WHERE adset_id = %s
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (adset_id,))
        row = cursor.fetchone()
    return int(row[0]) if row else 1


def close_open_budget_version(conn, adset_id: str, valid_to: datetime) -> None:
    sql = f"""
        UPDATE {ADSET_BUDGET_HISTORY_TABLE}
        SET valid_to = %s
        WHERE adset_id = %s AND valid_to IS NULL
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (valid_to, adset_id))


def insert_budget_version(
    conn,
    row: dict[str, Any],
    *,
    current_hashes: dict[str, str],
) -> bool:
    adset_id = row["adset_id"]
    budget_hash_value = row["budget_hash"]
    if current_hashes.get(adset_id) == budget_hash_value:
        return False

    observed_at = row["observed_at"]
    if not isinstance(observed_at, datetime):
        observed_at = datetime.now(timezone.utc)
    elif observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)

    row = {**row, "observed_at": observed_at, "valid_from": observed_at, "valid_to": None}
    row["budget_version"] = next_budget_version(conn, adset_id)

    close_open_budget_version(conn, adset_id, observed_at)

    columns = ", ".join(BUDGET_COLUMNS)
    placeholders = ", ".join(["%s"] * len(BUDGET_COLUMNS))
    values = tuple(_serialize_value(row.get(column)) for column in BUDGET_COLUMNS)
    sql = f"""
        INSERT INTO {ADSET_BUDGET_HISTORY_TABLE} ({columns})
        VALUES ({placeholders})
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, values)

    current_hashes[adset_id] = budget_hash_value
    return True


def sync_adset_budget_versions(
    conn,
    rows: list[dict[str, Any]],
) -> dict[str, int]:
    current_hashes = load_current_budget_hashes(conn)
    inserted = 0
    skipped = 0
    for row in rows:
        if insert_budget_version(conn, row, current_hashes=current_hashes):
            inserted += 1
        else:
            skipped += 1
    return {"budget_inserted": inserted, "budget_skipped": skipped, "budget_fetched": len(rows)}


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


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_list_or_none(value: Any) -> list[Any] | None:
    return value if isinstance(value, list) else None


def _targeting_items(targeting: dict[str, Any], key: str) -> list[Any] | None:
    direct = _json_list_or_none(targeting.get(key))
    if direct:
        return direct

    items: list[Any] = []
    flexible_spec = targeting.get("flexible_spec")
    if isinstance(flexible_spec, list):
        for entry in flexible_spec:
            if not isinstance(entry, dict):
                continue
            values = entry.get(key)
            if isinstance(values, list):
                items.extend(values)
    return items or None


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value

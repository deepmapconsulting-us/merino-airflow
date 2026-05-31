"""Sync Meta campaign / adset / ad properties into marketing dimension tables."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from merino_meta_jobs.facebook_graph import MetaGraphClient, ensure_act_prefix

COMPANY = "merino"
PLATFORM = "meta"
SOURCE = "facebook"

CAMPAIGN_DETAIL_FIELDS = (
    "id,name,objective,status,daily_budget,lifetime_budget,buying_type,"
    "start_time,stop_time,created_time,updated_time,bid_strategy"
)
ADSET_DETAIL_FIELDS = (
    "id,name,campaign_id,status,daily_budget,lifetime_budget,optimization_goal,"
    "billing_event,start_time,end_time,created_time,updated_time,bid_strategy"
)
AD_DETAIL_FIELDS = (
    "id,name,adset_id,campaign_id,status,creative,created_time,updated_time"
)

CAMPAIGN_LIST_FIELDS = "id,name,status,objective,created_time,updated_time"
ADSET_LIST_FIELDS = "id,name,status,campaign_id,created_time,updated_time"
AD_LIST_FIELDS = "id,name,status,adset_id,campaign_id,created_time,updated_time,creative{id}"

CAMPAIGN_TABLE = "marketing.meta_campaign"
ADSET_TABLE = "marketing.meta_adset"
AD_TABLE = "marketing.meta_ad"

CAMPAIGN_INSERT_COLUMNS = (
    "campaign_id",
    "source_account_id",
    "company",
    "platform",
    "source",
    "name",
    "status",
    "objective",
    "daily_budget",
    "lifetime_budget",
    "buying_type",
    "bid_strategy",
    "start_time",
    "stop_time",
    "created_at",
    "updated_at",
    "config_snapshot_uri",
)
ADSET_INSERT_COLUMNS = (
    "adset_id",
    "campaign_id",
    "source_account_id",
    "company",
    "platform",
    "source",
    "name",
    "status",
    "daily_budget",
    "lifetime_budget",
    "optimization_goal",
    "billing_event",
    "bid_strategy",
    "start_time",
    "end_time",
    "created_at",
    "updated_at",
    "config_snapshot_uri",
)
AD_INSERT_COLUMNS = (
    "ad_id",
    "adset_id",
    "campaign_id",
    "creative_id",
    "source_account_id",
    "company",
    "platform",
    "source",
    "name",
    "status",
    "created_at",
    "updated_at",
    "config_snapshot_uri",
)

CAMPAIGN_CHANGE_COLUMNS = ("name", "status", "objective")
ADSET_CHANGE_COLUMNS = ("name", "status", "campaign_id")
AD_CHANGE_COLUMNS = ("name", "status", "creative_id", "adset_id", "campaign_id")


def flatten_config_snapshot(snapshot: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    campaigns: list[dict[str, Any]] = []
    adsets: list[dict[str, Any]] = []
    ads: list[dict[str, Any]] = []

    accounts = snapshot.get("accounts")
    if not isinstance(accounts, dict):
        return {"campaigns": campaigns, "adsets": adsets, "ads": ads}

    for account_id, account in accounts.items():
        if not isinstance(account, dict):
            continue
        source_account_id = ensure_act_prefix(str(account.get("id") or account_id))
        for campaign in account.get("campaigns", []):
            if not isinstance(campaign, dict) or not campaign.get("id"):
                continue
            campaign_id = str(campaign["id"])
            campaigns.append(
                {
                    "campaign_id": campaign_id,
                    "source_account_id": source_account_id,
                    "name": campaign.get("name"),
                    "status": campaign.get("status"),
                    "objective": campaign.get("objective"),
                    "created_at": campaign.get("created_at"),
                    "updated_at": campaign.get("updated_at"),
                }
            )
            for adset in campaign.get("adsets", []):
                if not isinstance(adset, dict) or not adset.get("id"):
                    continue
                adset_id = str(adset["id"])
                adsets.append(
                    {
                        "adset_id": adset_id,
                        "campaign_id": str(adset.get("campaign_id") or campaign_id),
                        "source_account_id": source_account_id,
                        "name": adset.get("name"),
                        "status": adset.get("status"),
                        "created_at": adset.get("created_at"),
                        "updated_at": adset.get("updated_at"),
                    }
                )
                for ad in adset.get("ads", []):
                    if not isinstance(ad, dict) or not ad.get("id"):
                        continue
                    creative = ad.get("creative") if isinstance(ad.get("creative"), dict) else {}
                    ads.append(
                        {
                            "ad_id": str(ad["id"]),
                            "adset_id": adset_id,
                            "campaign_id": str(ad.get("campaign_id") or campaign_id),
                            "source_account_id": source_account_id,
                            "name": ad.get("name"),
                            "status": ad.get("status"),
                            "creative_id": str(creative["id"]) if creative.get("id") else None,
                            "created_at": ad.get("created_at"),
                            "updated_at": ad.get("updated_at"),
                        }
                    )

    return {"campaigns": campaigns, "adsets": adsets, "ads": ads}


def fetch_object_detail(client: MetaGraphClient, level: str, object_id: str) -> dict[str, Any]:
    fields = {
        "campaign": CAMPAIGN_DETAIL_FIELDS,
        "adset": ADSET_DETAIL_FIELDS,
        "ad": AD_DETAIL_FIELDS,
    }.get(level)
    if not fields:
        raise ValueError(f"Unknown object level: {level}")
    payload = client.get(object_id, {"fields": fields})
    if not isinstance(payload, dict):
        return {}
    return payload


def campaign_row_from_graph(
    row: dict[str, Any],
    *,
    source_account_id: str,
    config_snapshot_uri: str = "",
) -> tuple[Any, ...]:
    return (
        str(row.get("id") or row.get("campaign_id") or ""),
        source_account_id,
        COMPANY,
        PLATFORM,
        SOURCE,
        row.get("name"),
        row.get("status"),
        row.get("objective"),
        _int_or_none(row.get("daily_budget")),
        _int_or_none(row.get("lifetime_budget")),
        row.get("buying_type"),
        row.get("bid_strategy"),
        _parse_time(row.get("start_time")),
        _parse_time(row.get("stop_time")),
        _parse_time(row.get("created_time") or row.get("created_at")),
        _parse_time(row.get("updated_time") or row.get("updated_at")),
        config_snapshot_uri or None,
    )


def adset_row_from_graph(
    row: dict[str, Any],
    *,
    source_account_id: str,
    config_snapshot_uri: str = "",
) -> tuple[Any, ...]:
    return (
        str(row.get("id") or row.get("adset_id") or ""),
        str(row.get("campaign_id") or ""),
        source_account_id,
        COMPANY,
        PLATFORM,
        SOURCE,
        row.get("name"),
        row.get("status"),
        _int_or_none(row.get("daily_budget")),
        _int_or_none(row.get("lifetime_budget")),
        row.get("optimization_goal"),
        row.get("billing_event"),
        row.get("bid_strategy"),
        _parse_time(row.get("start_time")),
        _parse_time(row.get("end_time")),
        _parse_time(row.get("created_time") or row.get("created_at")),
        _parse_time(row.get("updated_time") or row.get("updated_at")),
        config_snapshot_uri or None,
    )


def ad_row_from_graph(
    row: dict[str, Any],
    *,
    source_account_id: str,
    config_snapshot_uri: str = "",
) -> tuple[Any, ...]:
    creative = row.get("creative") if isinstance(row.get("creative"), dict) else {}
    creative_id = row.get("creative_id")
    if not creative_id and creative.get("id"):
        creative_id = str(creative["id"])
    return (
        str(row.get("id") or row.get("ad_id") or ""),
        str(row.get("adset_id") or ""),
        str(row.get("campaign_id") or ""),
        str(creative_id) if creative_id else None,
        source_account_id,
        COMPANY,
        PLATFORM,
        SOURCE,
        row.get("name"),
        row.get("status"),
        _parse_time(row.get("created_time") or row.get("created_at")),
        _parse_time(row.get("updated_time") or row.get("updated_at")),
        config_snapshot_uri or None,
    )


def full_init_rows(
    client: MetaGraphClient,
    account_id: str,
    *,
    page_limit: int = 500,
    config_snapshot_uri: str = "",
) -> dict[str, list[tuple[Any, ...]]]:
    account_id = ensure_act_prefix(account_id)
    list_params = {"fields": CAMPAIGN_LIST_FIELDS, "limit": page_limit}
    campaign_rows: list[tuple[Any, ...]] = []
    adset_rows: list[tuple[Any, ...]] = []
    ad_rows: list[tuple[Any, ...]] = []

    for listed in client.get_all(f"{account_id}/campaigns", list_params):
        detail = fetch_object_detail(client, "campaign", str(listed["id"]))
        campaign_rows.append(
            campaign_row_from_graph(
                detail,
                source_account_id=account_id,
                config_snapshot_uri=config_snapshot_uri,
            )
        )

    for listed in client.get_all(
        f"{account_id}/adsets",
        {"fields": ADSET_LIST_FIELDS, "limit": page_limit},
    ):
        detail = fetch_object_detail(client, "adset", str(listed["id"]))
        adset_rows.append(
            adset_row_from_graph(
                detail,
                source_account_id=account_id,
                config_snapshot_uri=config_snapshot_uri,
            )
        )

    for listed in client.get_all(
        f"{account_id}/ads",
        {"fields": AD_LIST_FIELDS, "limit": page_limit},
    ):
        detail = fetch_object_detail(client, "ad", str(listed["id"]))
        ad_rows.append(
            ad_row_from_graph(
                detail,
                source_account_id=account_id,
                config_snapshot_uri=config_snapshot_uri,
            )
        )

    return {"campaigns": campaign_rows, "adsets": adset_rows, "ads": ad_rows}


def incremental_rows_from_snapshot(
    flat: dict[str, list[dict[str, Any]]],
    *,
    config_snapshot_uri: str,
) -> dict[str, list[tuple[Any, ...]]]:
    campaigns = [
        campaign_row_from_graph(row, source_account_id=row["source_account_id"], config_snapshot_uri=config_snapshot_uri)
        for row in flat["campaigns"]
        if row.get("campaign_id")
    ]
    adsets = [
        adset_row_from_graph(row, source_account_id=row["source_account_id"], config_snapshot_uri=config_snapshot_uri)
        for row in flat["adsets"]
        if row.get("adset_id")
    ]
    ads = [
        ad_row_from_graph(row, source_account_id=row["source_account_id"], config_snapshot_uri=config_snapshot_uri)
        for row in flat["ads"]
        if row.get("ad_id")
    ]
    return {"campaigns": campaigns, "adsets": adsets, "ads": ads}


def detail_rows_for_new_ids(
    client: MetaGraphClient,
    flat: dict[str, list[dict[str, Any]]],
    existing: dict[str, set[str]],
    *,
    config_snapshot_uri: str,
) -> dict[str, list[tuple[Any, ...]]]:
    campaigns: list[tuple[Any, ...]] = []
    adsets: list[tuple[Any, ...]] = []
    ads: list[tuple[Any, ...]] = []

    for row in flat["campaigns"]:
        campaign_id = str(row.get("campaign_id") or "")
        if not campaign_id or campaign_id in existing["campaigns"]:
            continue
        detail = fetch_object_detail(client, "campaign", campaign_id)
        campaigns.append(
            campaign_row_from_graph(
                detail,
                source_account_id=row["source_account_id"],
                config_snapshot_uri=config_snapshot_uri,
            )
        )

    for row in flat["adsets"]:
        adset_id = str(row.get("adset_id") or "")
        if not adset_id or adset_id in existing["adsets"]:
            continue
        detail = fetch_object_detail(client, "adset", adset_id)
        adsets.append(
            adset_row_from_graph(
                detail,
                source_account_id=row["source_account_id"],
                config_snapshot_uri=config_snapshot_uri,
            )
        )

    for row in flat["ads"]:
        ad_id = str(row.get("ad_id") or "")
        if not ad_id or ad_id in existing["ads"]:
            continue
        detail = fetch_object_detail(client, "ad", ad_id)
        ads.append(
            ad_row_from_graph(
                detail,
                source_account_id=row["source_account_id"],
                config_snapshot_uri=config_snapshot_uri,
            )
        )

    return {"campaigns": campaigns, "adsets": adsets, "ads": ads}


def load_existing_ids(conn) -> dict[str, set[str]]:
    existing = {"campaigns": set(), "adsets": set(), "ads": set()}
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT campaign_id FROM {CAMPAIGN_TABLE}")
        existing["campaigns"] = {str(row[0]) for row in cursor.fetchall()}
        cursor.execute(f"SELECT adset_id FROM {ADSET_TABLE}")
        existing["adsets"] = {str(row[0]) for row in cursor.fetchall()}
        cursor.execute(f"SELECT ad_id FROM {AD_TABLE}")
        existing["ads"] = {str(row[0]) for row in cursor.fetchall()}
    return existing


def upsert_property_rows(
    conn,
    table_name: str,
    column_names: tuple[str, ...],
    pk_column: str,
    change_columns: tuple[str, ...],
    rows: list[tuple[Any, ...]],
) -> int:
    if not rows:
        return 0

    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    update_columns = [column for column in column_names if column not in {pk_column}]
    assignments = ",\n            ".join(
        [
            "last_synced_at = now()",
            "update_count = target.update_count + 1",
            *[f"{column} = EXCLUDED.{column}" for column in update_columns],
        ]
    )
    changed = "\n            OR ".join(
        f"target.{column} IS DISTINCT FROM EXCLUDED.{column}" for column in change_columns
    )
    sql = f"""
        INSERT INTO {table_name} AS target ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({pk_column}) DO UPDATE
        SET {assignments}
        WHERE {changed}
    """
    with conn.cursor() as cursor:
        cursor.executemany(sql, rows)
    return len(rows)


def stub_rows_from_metrics(conn) -> dict[str, int]:
    counts = {"campaigns": 0, "adsets": 0, "ads": 0}
    stub_sql = {
        "campaigns": f"""
            INSERT INTO {CAMPAIGN_TABLE} (
                campaign_id, source_account_id, company, platform, source, name, status
            )
            SELECT DISTINCT campaign_id, 'unknown', '{COMPANY}', '{PLATFORM}', '{SOURCE}', '(unknown)', 'UNKNOWN'
            FROM marketing.meta_ad_hourly_metric
            WHERE campaign_id IS NOT NULL AND campaign_id <> ''
            ON CONFLICT (campaign_id) DO NOTHING
        """,
        "adsets": f"""
            INSERT INTO {ADSET_TABLE} (
                adset_id, campaign_id, source_account_id, company, platform, source, name, status
            )
            SELECT DISTINCT adset_id, campaign_id, 'unknown', '{COMPANY}', '{PLATFORM}', '{SOURCE}', '(unknown)', 'UNKNOWN'
            FROM marketing.meta_ad_hourly_metric
            WHERE adset_id IS NOT NULL AND adset_id <> ''
              AND campaign_id IS NOT NULL AND campaign_id <> ''
            ON CONFLICT (adset_id) DO NOTHING
        """,
        "ads": f"""
            INSERT INTO {AD_TABLE} (
                ad_id, adset_id, campaign_id, source_account_id, company, platform, source, name, status
            )
            SELECT DISTINCT ad_id, adset_id, campaign_id, 'unknown', '{COMPANY}', '{PLATFORM}', '{SOURCE}', '(unknown)', 'UNKNOWN'
            FROM marketing.meta_ad_hourly_metric
            WHERE ad_id IS NOT NULL AND ad_id <> ''
              AND adset_id IS NOT NULL AND adset_id <> ''
              AND campaign_id IS NOT NULL AND campaign_id <> ''
            ON CONFLICT (ad_id) DO NOTHING
        """,
    }
    with conn.cursor() as cursor:
        for key, sql in stub_sql.items():
            cursor.execute(sql)
            counts[key] = cursor.rowcount
    return counts


def sync_all_rows(
    conn,
    *,
    campaign_rows: list[tuple[Any, ...]],
    adset_rows: list[tuple[Any, ...]],
    ad_rows: list[tuple[Any, ...]],
) -> dict[str, int]:
    return {
        "campaigns": upsert_property_rows(
            conn,
            CAMPAIGN_TABLE,
            CAMPAIGN_INSERT_COLUMNS,
            "campaign_id",
            CAMPAIGN_CHANGE_COLUMNS,
            campaign_rows,
        ),
        "adsets": upsert_property_rows(
            conn,
            ADSET_TABLE,
            ADSET_INSERT_COLUMNS,
            "adset_id",
            ADSET_CHANGE_COLUMNS,
            adset_rows,
        ),
        "ads": upsert_property_rows(
            conn,
            AD_TABLE,
            AD_INSERT_COLUMNS,
            "ad_id",
            AD_CHANGE_COLUMNS,
            ad_rows,
        ),
    }


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

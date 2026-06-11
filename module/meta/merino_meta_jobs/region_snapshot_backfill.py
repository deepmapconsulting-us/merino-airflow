"""Plan Meta region snapshot backfills from dimension tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

from merino_meta_jobs.facebook_graph import ensure_act_prefix

DEFAULT_CHUNK_SIZE = 50
REGION_BACKFILL_LEVELS = ("campaign", "adset", "ad")


@dataclass(frozen=True)
class EntityWindow:
    start_date: date
    end_date: date


def parse_report_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def report_dates(start_date: str | date, end_date: str | date) -> list[str]:
    current = parse_report_date(start_date)
    end = parse_report_date(end_date)
    if current > end:
        raise ValueError(f"start_date {current} is after end_date {end}")

    dates: list[str] = []
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def _date_value(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _latest(*values: date | None) -> date | None:
    dates = [value for value in values if value is not None]
    return max(dates) if dates else None


def _earliest(*values: date | None) -> date | None:
    dates = [value for value in values if value is not None]
    return min(dates) if dates else None


def entity_window(
    *,
    lower_bound: date,
    upper_bound: date,
    created_at: Any = None,
    start_time: Any = None,
    stop_time: Any = None,
    parent_start: date | None = None,
    parent_end: date | None = None,
) -> EntityWindow | None:
    start = _latest(lower_bound, parent_start, _date_value(start_time), _date_value(created_at))
    end = _earliest(upper_bound, parent_end, _date_value(stop_time))
    if start is None:
        start = lower_bound
    if end is None:
        end = upper_bound
    if start > end:
        return None
    return EntityWindow(start, end)


def chunks(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    return [values[index : index + size] for index in range(0, len(values), size)]


def _existing_key(level: str, row: dict[str, Any]) -> tuple[Any, ...]:
    if level == "campaign":
        return (
            str(row["report_date"]),
            ensure_act_prefix(str(row["source_account_id"])),
            str(row["campaign_id"]),
        )
    if level == "adset":
        return (
            str(row["report_date"]),
            ensure_act_prefix(str(row["source_account_id"])),
            str(row["campaign_id"]),
            str(row["adset_id"]),
        )
    if level == "ad":
        return (
            str(row["report_date"]),
            ensure_act_prefix(str(row["source_account_id"])),
            str(row["campaign_id"]),
            str(row["adset_id"]),
            str(row["ad_id"]),
        )
    raise ValueError(f"unknown level {level!r}")


def _scheduled_dates(window: EntityWindow) -> list[str]:
    return report_dates(window.start_date, window.end_date)


def _skip_existing(
    *,
    force: bool,
    existing_keys: set[tuple[Any, ...]],
    key: tuple[Any, ...],
) -> bool:
    return not force and key in existing_keys


def plan_region_backfill(
    *,
    campaigns: list[dict[str, Any]],
    adsets: list[dict[str, Any]],
    ads: list[dict[str, Any]],
    existing: dict[str, set[tuple[Any, ...]]] | None,
    start_date: str | date,
    end_date: str | date,
    levels: Iterable[str] = REGION_BACKFILL_LEVELS,
    account_ids: Iterable[str] | None = None,
    campaign_chunk_size: int = DEFAULT_CHUNK_SIZE,
    adset_chunk_size: int = DEFAULT_CHUNK_SIZE,
    ad_chunk_size: int = DEFAULT_CHUNK_SIZE,
    force: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    lower_bound = parse_report_date(start_date)
    upper_bound = parse_report_date(end_date)
    selected_levels = set(levels)
    selected_accounts = {
        ensure_act_prefix(str(account_id))
        for account_id in (account_ids or [])
        if str(account_id).strip()
    }
    existing = existing or {}

    campaign_by_id = {str(row["campaign_id"]): row for row in campaigns}
    adsets_by_campaign: dict[str, list[dict[str, Any]]] = {}
    adset_by_id: dict[str, dict[str, Any]] = {}
    for row in adsets:
        campaign_id = str(row["campaign_id"])
        adset_id = str(row["adset_id"])
        adsets_by_campaign.setdefault(campaign_id, []).append(row)
        adset_by_id[adset_id] = row

    ads_by_campaign: dict[str, list[dict[str, Any]]] = {}
    ads_by_adset: dict[str, list[dict[str, Any]]] = {}
    for row in ads:
        campaign_id = str(row["campaign_id"])
        adset_id = str(row["adset_id"])
        ads_by_campaign.setdefault(campaign_id, []).append(row)
        ads_by_adset.setdefault(adset_id, []).append(row)

    campaign_ids_by_date_account: dict[tuple[str, str], list[str]] = {}
    adset_ids_by_date_account_campaign: dict[tuple[str, str, str], list[str]] = {}
    ad_ids_by_date_account_campaign: dict[tuple[str, str, str], list[str]] = {}
    campaign_windows: dict[str, EntityWindow] = {}
    adset_windows: dict[str, EntityWindow] = {}

    for row in campaigns:
        campaign_id = str(row["campaign_id"])
        account_id = ensure_act_prefix(str(row["source_account_id"]))
        if selected_accounts and account_id not in selected_accounts:
            continue
        window = entity_window(
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            created_at=row.get("created_at"),
            start_time=row.get("start_time"),
            stop_time=row.get("stop_time"),
        )
        if window is None:
            continue
        campaign_windows[campaign_id] = window

        if "campaign" not in selected_levels:
            continue
        for report_date in _scheduled_dates(window):
            key = (report_date, account_id, campaign_id)
            if _skip_existing(force=force, existing_keys=existing.get("campaign", set()), key=key):
                continue
            campaign_ids_by_date_account.setdefault((report_date, account_id), []).append(campaign_id)

    for row in adsets:
        campaign_id = str(row["campaign_id"])
        campaign = campaign_by_id.get(campaign_id)
        campaign_window = campaign_windows.get(campaign_id)
        if campaign is None or campaign_window is None:
            continue
        account_id = ensure_act_prefix(str(row["source_account_id"]))
        if selected_accounts and account_id not in selected_accounts:
            continue
        window = entity_window(
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            created_at=row.get("created_at"),
            start_time=row.get("start_time"),
            stop_time=row.get("end_time"),
            parent_start=campaign_window.start_date,
            parent_end=campaign_window.end_date,
        )
        if window is None:
            continue
        adset_id = str(row["adset_id"])
        adset_windows[adset_id] = window

        if "adset" not in selected_levels:
            continue
        for report_date in _scheduled_dates(window):
            key = (report_date, account_id, campaign_id, adset_id)
            if _skip_existing(force=force, existing_keys=existing.get("adset", set()), key=key):
                continue
            adset_ids_by_date_account_campaign.setdefault(
                (report_date, account_id, campaign_id),
                [],
            ).append(adset_id)

    for row in ads:
        campaign_id = str(row["campaign_id"])
        adset_id = str(row["adset_id"])
        campaign = campaign_by_id.get(campaign_id)
        adset = adset_by_id.get(adset_id)
        adset_window = adset_windows.get(adset_id)
        if campaign is None or adset is None or adset_window is None:
            continue
        account_id = ensure_act_prefix(str(row["source_account_id"]))
        if selected_accounts and account_id not in selected_accounts:
            continue
        window = entity_window(
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            created_at=row.get("created_at"),
            parent_start=adset_window.start_date,
            parent_end=adset_window.end_date,
        )
        if window is None:
            continue

        ad_id = str(row["ad_id"])
        if "ad" not in selected_levels:
            continue
        for report_date in _scheduled_dates(window):
            key = (report_date, account_id, campaign_id, adset_id, ad_id)
            if _skip_existing(force=force, existing_keys=existing.get("ad", set()), key=key):
                continue
            ad_ids_by_date_account_campaign.setdefault(
                (report_date, account_id, campaign_id),
                [],
            ).append(ad_id)

    batches = {
        "campaign_batches": _campaign_batches(campaign_ids_by_date_account, campaign_chunk_size),
        "adset_batches": _entity_batches(
            adset_ids_by_date_account_campaign,
            adsets_by_campaign,
            ads_by_adset,
            adset_chunk_size,
            "adset_ids",
        ),
        "ad_batches": _entity_batches(
            ad_ids_by_date_account_campaign,
            ads_by_campaign,
            ads_by_adset,
            ad_chunk_size,
            "ad_ids",
        ),
    }

    return batches


def _campaign_batches(
    campaign_ids_by_date_account: dict[tuple[str, str], list[str]],
    chunk_size: int,
) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    for (report_date, account_id), campaign_ids in sorted(campaign_ids_by_date_account.items()):
        for ids in chunks(sorted(set(campaign_ids)), chunk_size):
            batches.append(
                {
                    "report_date": report_date,
                    "account": {"id": account_id, "timezone_name": None},
                    "campaign_ids": ids,
                }
            )
    return batches


def _entity_batches(
    ids_by_date_account_campaign: dict[tuple[str, str, str], list[str]],
    entity_rows_by_campaign: dict[str, list[dict[str, Any]]],
    ads_by_adset: dict[str, list[dict[str, Any]]],
    chunk_size: int,
    id_field: str,
) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    for (report_date, account_id, campaign_id), entity_ids in sorted(ids_by_date_account_campaign.items()):
        for ids in chunks(sorted(set(entity_ids)), chunk_size):
            batches.append(
                {
                    "report_date": report_date,
                    "account": {"id": account_id, "timezone_name": None},
                    "campaign": _campaign_payload(
                        campaign_id,
                        entity_rows_by_campaign.get(campaign_id, []),
                        ads_by_adset,
                        ids,
                        id_field,
                    ),
                    id_field: ids,
                }
            )
    return batches


def _campaign_payload(
    campaign_id: str,
    rows: list[dict[str, Any]],
    ads_by_adset: dict[str, list[dict[str, Any]]],
    selected_ids: list[str],
    id_field: str,
) -> dict[str, Any]:
    selected = set(selected_ids)
    adsets: dict[str, dict[str, Any]] = {}
    if id_field == "adset_ids":
        for row in rows:
            adset_id = str(row["adset_id"])
            if adset_id not in selected:
                continue
            adsets[adset_id] = {
                "id": adset_id,
                "ads": _ad_payloads(ads_by_adset.get(adset_id, [])),
            }
    else:
        for row in rows:
            ad_id = str(row["ad_id"])
            if ad_id not in selected:
                continue
            adset_id = str(row["adset_id"])
            adsets.setdefault(adset_id, {"id": adset_id, "ads": []})["ads"].append(
                {
                    "id": ad_id,
                    "creative_id": row.get("creative_id"),
                }
            )
    return {"id": campaign_id, "adsets": list(adsets.values())}


def _ad_payloads(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "id": str(row["ad_id"]),
            "creative_id": row.get("creative_id"),
        }
        for row in rows
    ]


def existing_region_keys(conn: Any, start_date: str, end_date: str) -> dict[str, set[tuple[Any, ...]]]:
    return {
        "campaign": _existing_keys(
            conn,
            """
            SELECT DISTINCT report_date::text, source_account_id, campaign_id
            FROM marketing.meta_campaign_region_daily_snapshot
            WHERE report_date BETWEEN %s AND %s
            """,
            start_date,
            end_date,
        ),
        "adset": _existing_keys(
            conn,
            """
            SELECT DISTINCT report_date::text, source_account_id, campaign_id, adset_id
            FROM marketing.meta_adset_region_daily_snapshot
            WHERE report_date BETWEEN %s AND %s
            """,
            start_date,
            end_date,
        ),
        "ad": _existing_keys(
            conn,
            """
            SELECT DISTINCT report_date::text, source_account_id, campaign_id, adset_id, ad_id
            FROM marketing.meta_ad_region_daily_snapshot
            WHERE report_date BETWEEN %s AND %s
            """,
            start_date,
            end_date,
        ),
    }


def _existing_keys(conn: Any, sql: str, start_date: str, end_date: str) -> set[tuple[Any, ...]]:
    with conn.cursor() as cursor:
        cursor.execute(sql, (start_date, end_date))
        return {tuple(row) for row in cursor.fetchall()}


def load_dimension_rows(conn: Any, account_ids: Iterable[str] | None = None) -> dict[str, list[dict[str, Any]]]:
    selected_accounts = [
        ensure_act_prefix(str(account_id))
        for account_id in (account_ids or [])
        if str(account_id).strip()
    ]
    account_filter = "AND source_account_id = ANY(%s)" if selected_accounts else ""
    params = (selected_accounts,) if selected_accounts else ()
    return {
        "campaigns": _dict_rows(
            conn,
            f"""
            SELECT campaign_id, source_account_id, name, status, created_at, start_time, stop_time
            FROM marketing.meta_campaign
            WHERE 1 = 1 {account_filter}
            """,
            params,
        ),
        "adsets": _dict_rows(
            conn,
            f"""
            SELECT adset_id, campaign_id, source_account_id, name, status, created_at, start_time, end_time
            FROM marketing.meta_adset
            WHERE 1 = 1 {account_filter}
            """,
            params,
        ),
        "ads": _dict_rows(
            conn,
            f"""
            SELECT ad_id, adset_id, campaign_id, creative_id, source_account_id, name, status, created_at
            FROM marketing.meta_ad
            WHERE 1 = 1 {account_filter}
            """,
            params,
        ),
    }


def _dict_rows(conn: Any, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

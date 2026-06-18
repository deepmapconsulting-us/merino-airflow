"""Shared row builders and upserts for Meta daily snapshot tables."""

from __future__ import annotations

import json
from typing import Any

from merino_meta_jobs.traffic import insight_metric_values

POSTGRES_CONN_ID = "merino_analytics"
COALESCE_ON_UPDATE_COLUMNS = frozenset({"creative_id"})
COMPANY = "merino"
PLATFORM = "meta"
SOURCE = "facebook"

CAMPAIGN_DAILY_TABLE = "marketing.meta_campaign_daily_snapshot"
CAMPAIGN_REGION_DAILY_TABLE = "marketing.meta_campaign_region_daily_snapshot"
ADSET_DAILY_TABLE = "marketing.meta_adset_daily_snapshot"
ADSET_REGION_DAILY_TABLE = "marketing.meta_adset_region_daily_snapshot"
AD_DAILY_TABLE = "marketing.meta_ad_daily_snapshot"
AD_REGION_DAILY_TABLE = "marketing.meta_ad_region_daily_snapshot"
AD_GENDER_AGE_DAILY_TABLE = "marketing.meta_ad_gender_age_daily_snapshot"

JSON_COLUMNS = {
    "actions",
    "action_values",
    "cost_per_action_type",
    "conversions",
    "video_avg_time_watched_actions",
}
METRIC_COLUMNS = (
    "spend",
    "impressions",
    "reach",
    "frequency",
    "clicks",
    "unique_clicks",
    "ctr",
    "cpc",
    "cpm",
    "actions",
    "link_clicks",
    "landing_page_views",
    "page_engagement",
    "post_reactions",
    "post_comments",
    "post_saves",
    "post_shares",
    "facebook_likes",
    "instagram_follows",
    "app_installs",
    "mobile_app_installs",
    "results",
    "cost_per_result",
    "cost_per_app_install",
    "cost_per_action_type",
    "action_values",
    "conversions",
    "attribution_setting",
    "video_avg_time_watched_actions",
)
CHANGE_COLUMNS = ("active_status", "spend", "clicks", "impressions", "unique_clicks", "ctr")
BASE_INSERT_COLUMNS = (
    "snapshot_run_id",
    "report_date",
    "company",
    "platform",
    "source",
    "source_account_id",
    "source_account_name",
    "currency_code",
    "timezone_name",
    "campaign_id",
    "campaign_name",
)
CAMPAIGN_INSERT_COLUMNS = (*BASE_INSERT_COLUMNS, "attribution_window", "active_status", *METRIC_COLUMNS)
CAMPAIGN_REGION_INSERT_COLUMNS = (
    *BASE_INSERT_COLUMNS,
    "region",
    "attribution_window",
    "active_status",
    *METRIC_COLUMNS,
)
ADSET_INSERT_COLUMNS = (
    *BASE_INSERT_COLUMNS,
    "adset_id",
    "adset_name",
    "attribution_window",
    "active_status",
    *METRIC_COLUMNS,
)
ADSET_REGION_INSERT_COLUMNS = (
    *BASE_INSERT_COLUMNS,
    "adset_id",
    "adset_name",
    "region",
    "attribution_window",
    "active_status",
    *METRIC_COLUMNS,
)
AD_INSERT_COLUMNS = (
    *BASE_INSERT_COLUMNS,
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "creative_id",
    "creative_name",
    "attribution_window",
    "active_status",
    *METRIC_COLUMNS,
)
AD_REGION_INSERT_COLUMNS = (
    *BASE_INSERT_COLUMNS,
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "creative_id",
    "creative_name",
    "region",
    "attribution_window",
    "active_status",
    *METRIC_COLUMNS,
)
AD_GENDER_AGE_INSERT_COLUMNS = (
    *BASE_INSERT_COLUMNS,
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "creative_id",
    "creative_name",
    "age",
    "gender",
    "attribution_window",
    "active_status",
    *METRIC_COLUMNS,
)

CAMPAIGN_CONFLICT_COLUMNS = (
    "report_date",
    "source_account_id",
    "campaign_id",
    "(COALESCE(attribution_window, ''))",
)
CAMPAIGN_REGION_CONFLICT_COLUMNS = (
    "report_date",
    "source_account_id",
    "campaign_id",
    "region",
    "(COALESCE(attribution_window, ''))",
)
ADSET_CONFLICT_COLUMNS = (
    "report_date",
    "source_account_id",
    "campaign_id",
    "adset_id",
    "(COALESCE(attribution_window, ''))",
)
ADSET_REGION_CONFLICT_COLUMNS = (
    "report_date",
    "source_account_id",
    "campaign_id",
    "adset_id",
    "region",
    "(COALESCE(attribution_window, ''))",
)
AD_CONFLICT_COLUMNS = (
    "report_date",
    "source_account_id",
    "campaign_id",
    "adset_id",
    "ad_id",
    "(COALESCE(attribution_window, ''))",
)
AD_REGION_CONFLICT_COLUMNS = (
    "report_date",
    "source_account_id",
    "campaign_id",
    "adset_id",
    "ad_id",
    "region",
    "(COALESCE(attribution_window, ''))",
)
AD_GENDER_AGE_CONFLICT_COLUMNS = (
    "report_date",
    "source_account_id",
    "campaign_id",
    "adset_id",
    "ad_id",
    "age",
    "gender",
    "(COALESCE(attribution_window, ''))",
)


def campaign_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: Any,
) -> tuple[Any, ...]:
    row_report_date = row_report_date_from(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or "")
    return (
        *base_values(snapshot, insight, account, snapshot_run_id, report_date),
        insight.get("attribution_window"),
        status_resolver.campaign_status(row_report_date, campaign_id),
        *metric_values(insight),
    )


def campaign_region_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: Any,
) -> tuple[Any, ...]:
    row_report_date = row_report_date_from(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or "")
    return (
        *base_values(snapshot, insight, account, snapshot_run_id, report_date),
        str(insight["region"]),
        insight.get("attribution_window"),
        status_resolver.campaign_status(row_report_date, campaign_id),
        *metric_values(insight),
    )


def adset_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: Any,
) -> tuple[Any, ...]:
    row_report_date = row_report_date_from(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or campaign["id"])
    adset_id = str(insight.get("adset_id") or "")
    return (
        *base_values(snapshot, insight, account, snapshot_run_id, report_date, campaign),
        adset_id,
        insight.get("adset_name"),
        insight.get("attribution_window"),
        status_resolver.adset_status(row_report_date, campaign_id, adset_id),
        *metric_values(insight),
    )


def adset_region_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: Any,
) -> tuple[Any, ...]:
    row_report_date = row_report_date_from(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or campaign["id"])
    adset_id = str(insight.get("adset_id") or "")
    return (
        *base_values(snapshot, insight, account, snapshot_run_id, report_date, campaign),
        adset_id,
        insight.get("adset_name"),
        str(insight["region"]),
        insight.get("attribution_window"),
        status_resolver.adset_status(row_report_date, campaign_id, adset_id),
        *metric_values(insight),
    )


def ad_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    adset_by_ad_id: dict[str, dict[str, Any]],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: Any,
) -> tuple[Any, ...]:
    ad_id = str(insight["ad_id"])
    adset = adset_by_ad_id[ad_id]
    row_report_date = row_report_date_from(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or campaign["id"])
    adset_id = str(insight.get("adset_id") or adset["id"])
    return (
        *base_values(snapshot, insight, account, snapshot_run_id, report_date, campaign),
        adset_id,
        insight.get("adset_name"),
        ad_id,
        insight.get("ad_name"),
        creative_id_by_ad_id(adset).get(ad_id),
        None,
        insight.get("attribution_window"),
        status_resolver.ad_status(row_report_date, campaign_id, adset_id, ad_id),
        *metric_values(insight),
    )


def ad_region_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    adset_by_ad_id: dict[str, dict[str, Any]],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: Any,
) -> tuple[Any, ...]:
    ad_id = str(insight["ad_id"])
    adset = adset_by_ad_id[ad_id]
    row_report_date = row_report_date_from(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or campaign["id"])
    adset_id = str(insight.get("adset_id") or adset["id"])
    return (
        *base_values(snapshot, insight, account, snapshot_run_id, report_date, campaign),
        adset_id,
        insight.get("adset_name"),
        ad_id,
        insight.get("ad_name"),
        creative_id_by_ad_id(adset).get(ad_id),
        None,
        str(insight["region"]),
        insight.get("attribution_window"),
        status_resolver.ad_status(row_report_date, campaign_id, adset_id, ad_id),
        *metric_values(insight),
    )


def ad_gender_age_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    adset_by_ad_id: dict[str, dict[str, Any]],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: Any,
) -> tuple[Any, ...]:
    ad_id = str(insight["ad_id"])
    adset = adset_by_ad_id[ad_id]
    row_report_date = row_report_date_from(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or campaign["id"])
    adset_id = str(insight.get("adset_id") or adset["id"])
    return (
        *base_values(snapshot, insight, account, snapshot_run_id, report_date, campaign),
        adset_id,
        insight.get("adset_name"),
        ad_id,
        insight.get("ad_name"),
        creative_id_by_ad_id(adset).get(ad_id),
        None,
        str(insight["age"]),
        str(insight["gender"]),
        insight.get("attribution_window"),
        status_resolver.ad_status(row_report_date, campaign_id, adset_id, ad_id),
        *metric_values(insight),
    )


def base_values(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    snapshot_run_id: str,
    report_date: str,
    campaign: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    return (
        snapshot_run_id,
        row_report_date_from(snapshot, insight, report_date),
        COMPANY,
        PLATFORM,
        SOURCE,
        account["id"],
        insight.get("account_name"),
        None,
        account.get("timezone_name"),
        insight.get("campaign_id") or (campaign or {}).get("id"),
        insight.get("campaign_name"),
    )


def row_report_date_from(snapshot: dict[str, Any], insight: dict[str, Any], fallback: str) -> str:
    return str(insight.get("date_start") or snapshot["metric_date"] or fallback)


def metric_values(insight: dict[str, Any]) -> tuple[Any, ...]:
    values = insight_metric_values(insight)
    return tuple(sql_value(column, values.get(column)) for column in METRIC_COLUMNS)


def sql_value(column: str, value: Any) -> Any:
    if column in JSON_COLUMNS:
        return json.dumps(value or [], separators=(",", ":"), sort_keys=True)
    return value


def upsert_daily_rows(
    table_name: str,
    column_names: tuple[str, ...],
    conflict_columns: tuple[str, ...],
    rows: list[tuple[Any, ...]],
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> None:
    if not rows:
        return

    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    conflict_target = ", ".join(conflict_columns)
    key_column_names = {column for column in conflict_columns if not column.startswith("(")}
    update_columns = [
        column
        for column in column_names
        if column not in key_column_names and column not in {"report_date"}
    ]
    assignments = ",\n            ".join(
        [
            "record_updated_at = now()",
            "update_count = target.update_count + 1",
            *[
                (
                    f"{column} = COALESCE(EXCLUDED.{column}, target.{column})"
                    if column in COALESCE_ON_UPDATE_COLUMNS
                    else f"{column} = EXCLUDED.{column}"
                )
                for column in update_columns
            ],
        ]
    )
    changed = "\n            OR ".join(
        f"target.{column} IS DISTINCT FROM EXCLUDED.{column}" for column in CHANGE_COLUMNS
    )
    sql = f"""
        INSERT INTO {table_name} AS target ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_target}) DO UPDATE
        SET {assignments}
        WHERE {changed}
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


def adset_by_ad_id(campaign: dict[str, Any]) -> dict[str, dict[str, Any]]:
    adset_by_id: dict[str, dict[str, Any]] = {}
    for adset in campaign.get("adsets", []):
        for ad in adset.get("ads", []):
            if ad.get("id"):
                adset_by_id[str(ad["id"])] = adset
    return adset_by_id


def creative_id_by_ad_id(adset: dict[str, Any]) -> dict[str, str]:
    creative_by_ad_id: dict[str, str] = {}
    for ad in adset.get("ads", []):
        if ad.get("id") and ad.get("creative_id"):
            creative_by_ad_id[str(ad["id"])] = str(ad["creative_id"])
    return creative_by_ad_id

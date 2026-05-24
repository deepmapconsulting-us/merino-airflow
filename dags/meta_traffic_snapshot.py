"""Sync daily Meta traffic snapshots from campaign-config-driven work.

Meta access token is read from Airflow Variable `meta_access_token` (GSM:
`airflow-variables-meta_access_token`), with fallback to env `META_ACCESS_TOKEN`.
The visible DAG hierarchy is built from the latest successful
`facebook_campaign_config_update` GCS snapshot. Each run writes current-day and
yesterday daily snapshots to Postgres and updates rows only when metrics change.
"""

from __future__ import annotations

import json
import os
import re
import sys
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from pendulum.parsing.exceptions import ParserError  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]
from airflow.utils.task_group import TaskGroup  # type: ignore[import-not-found]

try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found]
except ImportError:
    from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found,no-redef]

from meta_gcs import (
    REPORT_TIMEZONE,
    gcs_console_link,
    gcs_uri,
    latest_object_name,
    meta_access_token,
    read_json_from_gcs,
    read_latest_snapshot_pointer,
    report_datetime,
    variable_get,
)
from meta_status import DailyStatusResolver

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.traffic import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS,
    ad_daily_snapshot,
    ad_ids_from_config,
    adset_daily_snapshot,
    campaign_daily_snapshot,
    insight_metric_values,
    traffic_accounts_from_config,
)

DAG_ID = "meta_traffic_snapshot"
CAMPAIGN_CONFIG_DAG_ID = "facebook_campaign_config_update"
CONFIG_GCS_PREFIX = "facebook_campaign_config_update"
ACTIVE_ACCOUNTS_VARIABLE_NAME = "facebook_active_accounts"
ACTIVE_ACCOUNTS_ENV = "FACEBOOK_ACTIVE_ACCOUNTS"
LOOKUP_WINDOW_VARIABLE_NAME = "FACEBOOK_TRAFFIC_LOOKUP_WINDOWS"
LOOKUP_WINDOW_ENV = "FACEBOOK_TRAFFIC_LOOKUP_WINDOWS"
POSTGRES_CONN_ID = "merino_analytics"
DEFAULT_META_PAGE_LIMIT = 500
COMPANY = "merino"
PLATFORM = "meta"
SOURCE = "facebook"
CAMPAIGN_DAILY_TABLE = "marketing.meta_campaign_daily_snapshot"
ADSET_DAILY_TABLE = "marketing.meta_adset_daily_snapshot"
AD_DAILY_TABLE = "marketing.meta_ad_daily_snapshot"
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
ADSET_INSERT_COLUMNS = (
    *BASE_INSERT_COLUMNS,
    "adset_id",
    "adset_name",
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
CAMPAIGN_CONFLICT_COLUMNS = (
    "report_date",
    "source_account_id",
    "campaign_id",
    "(COALESCE(attribution_window, ''))",
)
ADSET_CONFLICT_COLUMNS = (
    "report_date",
    "source_account_id",
    "campaign_id",
    "adset_id",
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


@dag(
    dag_id=DAG_ID,
    schedule="0 2,14 * * *",
    start_date=pendulum.datetime(2026, 1, 1, 2, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["meta", "traffic", "daily-snapshot"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def meta_traffic_snapshot():
    config_source = _campaign_config_for_display()
    config_log = _config_log_payload(config_source)

    @task
    def log_campaign_config_source(source: dict[str, Any]) -> None:
        print(f"{DAG_ID}: config pointer: {source['pointer_uri']}")
        print(f"{DAG_ID}: config pointer link: {source['pointer_link']}")
        if source.get("snapshot_uri"):
            print(f"{DAG_ID}: config snapshot: {source['snapshot_uri']}")
            print(f"{DAG_ID}: config snapshot link: {source['snapshot_link']}")
        if source.get("error"):
            print(f"{DAG_ID}: campaign config unavailable during DAG parse: {source['error']}")
        else:
            print(
                f"{DAG_ID}: displaying {source['account_count']} accounts, "
                f"{source.get('campaign_count', 0)} campaigns, {source['adset_count']} adsets "
                "from campaign config"
            )

    @task
    def no_campaigns_from_campaign_config(source: dict[str, Any]) -> None:
        print(
            f"{DAG_ID}: no campaign traffic tasks were created. "
            f"pointer={source['pointer_uri']} snapshot={source.get('snapshot_uri') or '<none>'}"
        )
        if source.get("error"):
            raise RuntimeError(source["error"])

    @task
    def pull_campaign_snapshots(account: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        campaign_ids = [campaign["id"] for campaign in account.get("campaigns", []) if campaign.get("id")]
        snapshots = [
            campaign_daily_snapshot(
                access_token,
                account["id"],
                campaign_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"campaign daily rows for account={account['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_campaign_snapshots(
        campaign_snapshots: dict[str, Any],
        account: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "campaign", account["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        status_resolver = _daily_status_resolver()
        rows = [
            _campaign_row(snapshot, insight, account, snapshot_run_id, report_date, status_resolver)
            for snapshot in campaign_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("campaign_id")
        ]
        _upsert_daily_rows(CAMPAIGN_DAILY_TABLE, CAMPAIGN_INSERT_COLUMNS, CAMPAIGN_CONFLICT_COLUMNS, rows)
        print(f"{DAG_ID}: upserted {len(rows)} campaign daily rows for account={account['id']}")
        return {"level": "campaign", "row_count": len(rows), "account_id": account["id"]}

    @task
    def pull_adset_snapshots(
        account: dict[str, Any],
        campaign: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        adset_ids = [adset["id"] for adset in campaign.get("adsets", []) if adset.get("id")]
        snapshots = [
            adset_daily_snapshot(
                access_token,
                account["id"],
                campaign["id"],
                adset_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"adset daily rows for account={account['id']} campaign={campaign['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_adset_snapshots(
        adset_snapshots: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "adset", account["id"], campaign["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        status_resolver = _daily_status_resolver()
        rows = [
            _adset_row(snapshot, insight, account, campaign, snapshot_run_id, report_date, status_resolver)
            for snapshot in adset_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("adset_id")
        ]
        _upsert_daily_rows(ADSET_DAILY_TABLE, ADSET_INSERT_COLUMNS, ADSET_CONFLICT_COLUMNS, rows)
        print(
            f"{DAG_ID}: upserted {len(rows)} adset daily rows for "
            f"account={account['id']} campaign={campaign['id']}"
        )
        return {"level": "adset", "row_count": len(rows), "account_id": account["id"], "campaign_id": campaign["id"]}

    @task
    def pull_ad_snapshots(
        account: dict[str, Any],
        campaign: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        ad_ids = [
            ad_id
            for adset in campaign.get("adsets", [])
            for ad_id in ad_ids_from_config(adset)
        ]
        snapshots = [
            ad_daily_snapshot(
                access_token,
                account["id"],
                campaign["id"],
                ad_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"ad daily rows for account={account['id']} campaign={campaign['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_ad_snapshots(
        ad_snapshots: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "ad", account["id"], campaign["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        adset_by_ad_id = _adset_by_ad_id(campaign)
        status_resolver = _daily_status_resolver()
        rows = [
            _ad_row(snapshot, insight, account, campaign, adset_by_ad_id, snapshot_run_id, report_date, status_resolver)
            for snapshot in ad_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("ad_id") in adset_by_ad_id
        ]
        _upsert_daily_rows(AD_DAILY_TABLE, AD_INSERT_COLUMNS, AD_CONFLICT_COLUMNS, rows)
        print(
            f"{DAG_ID}: upserted {len(rows)} ad daily rows for "
            f"account={account['id']} campaign={campaign['id']}"
        )
        return {"level": "ad", "row_count": len(rows), "account_id": account["id"], "campaign_id": campaign["id"]}

    wait_for_campaign_config = ExternalTaskSensor(
        task_id="wait_for_facebook_campaign_config_update",
        external_dag_id=CAMPAIGN_CONFIG_DAG_ID,
        external_task_id=None,
        execution_date_fn=_campaign_config_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )
    config_task = log_campaign_config_source(config_log)
    wait_for_campaign_config >> config_task
    accounts = config_source.get("accounts", [])
    if not accounts:
        config_task >> no_campaigns_from_campaign_config(config_log)
        return

    for account in accounts:
        with TaskGroup(group_id=f"account_{_airflow_id(account['id'])}") as account_group:
            campaign_snapshots = pull_campaign_snapshots.override(task_id="pull_campaign_snapshots")(
                account,
                config_log,
            )
            write_campaign_snapshots.override(task_id="write_campaign_snapshots")(campaign_snapshots, account)

            for campaign in account["campaigns"]:
                with TaskGroup(group_id=f"campaign_{_airflow_id(campaign['id'])}") as campaign_group:
                    adset_snapshots = pull_adset_snapshots.override(task_id="pull_adset_snapshots")(
                        account,
                        campaign,
                        config_log,
                    )
                    write_adset_snapshots.override(task_id="write_adset_snapshots")(
                        adset_snapshots,
                        account,
                        campaign,
                    )

                    ad_snapshots = pull_ad_snapshots.override(task_id="pull_ad_snapshots")(
                        account,
                        campaign,
                        config_log,
                    )
                    write_ad_snapshots.override(task_id="write_ad_snapshots")(
                        ad_snapshots,
                        account,
                        campaign,
                    )

                config_task >> campaign_group

        config_task >> account_group


def _campaign_config_for_display() -> dict[str, Any]:
    pointer_uri = gcs_uri("airflow-run-us-west2", latest_object_name(CONFIG_GCS_PREFIX))
    source: dict[str, Any] = {
        "pointer_uri": pointer_uri,
        "pointer_link": gcs_console_link(pointer_uri),
        "snapshot_uri": "",
        "snapshot_link": "",
        "accounts": [],
        "account_count": 0,
        "campaign_count": 0,
        "adset_count": 0,
        "error": "",
    }
    try:
        import google.auth  # type: ignore[import-not-found]
        from google.cloud import storage  # type: ignore[import-not-found]

        credentials, _project_id = google.auth.default()
        storage_client = storage.Client(credentials=credentials)
        pointer_uri, pointer = read_latest_snapshot_pointer(storage_client, CONFIG_GCS_PREFIX)
        snapshot_uri = str(pointer["final_output"])
        snapshot = read_json_from_gcs(storage_client, snapshot_uri)
        lookup_window_days = int(
            variable_get(
                LOOKUP_WINDOW_VARIABLE_NAME,
                os.environ.get(LOOKUP_WINDOW_ENV, str(DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS)),
            )
        )
        active_accounts = variable_get(
            ACTIVE_ACCOUNTS_VARIABLE_NAME,
            os.environ.get(ACTIVE_ACCOUNTS_ENV, ""),
        )
        accounts = traffic_accounts_from_config(
            snapshot,
            active_accounts_value=active_accounts,
            lookup_window_days=lookup_window_days,
        )
        source.update(
            {
                "pointer_uri": pointer_uri,
                "pointer_link": gcs_console_link(pointer_uri),
                "snapshot_uri": snapshot_uri,
                "snapshot_link": gcs_console_link(snapshot_uri),
                "accounts": accounts,
                "account_count": len(accounts),
                "campaign_count": _campaign_count(accounts),
                "adset_count": _adset_count(accounts),
            }
        )
    except Exception as exc:
        source["error"] = str(exc)
    return source


def _config_log_payload(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "pointer_uri": source["pointer_uri"],
        "pointer_link": source["pointer_link"],
        "snapshot_uri": source.get("snapshot_uri", ""),
        "snapshot_link": source.get("snapshot_link", ""),
        "account_count": source.get("account_count", 0),
        "campaign_count": source.get("campaign_count", 0),
        "adset_count": source.get("adset_count", 0),
        "error": source.get("error", ""),
    }


def _campaign_count(accounts: list[dict[str, Any]]) -> int:
    return sum(len(account.get("campaigns", [])) for account in accounts)


def _adset_count(accounts: list[dict[str, Any]]) -> int:
    return sum(
        len(campaign.get("adsets", []))
        for account in accounts
        for campaign in account.get("campaigns", [])
    )


def _campaign_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: DailyStatusResolver,
) -> tuple[Any, ...]:
    row_report_date = _row_report_date(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or "")
    return (
        *_base_values(snapshot, insight, account, snapshot_run_id, report_date),
        insight.get("attribution_window"),
        status_resolver.campaign_status(row_report_date, campaign_id),
        *_metric_values(insight),
    )


def _adset_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: DailyStatusResolver,
) -> tuple[Any, ...]:
    row_report_date = _row_report_date(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or campaign["id"])
    adset_id = str(insight.get("adset_id") or "")
    return (
        *_base_values(snapshot, insight, account, snapshot_run_id, report_date, campaign),
        adset_id,
        insight.get("adset_name"),
        insight.get("attribution_window"),
        status_resolver.adset_status(row_report_date, campaign_id, adset_id),
        *_metric_values(insight),
    )


def _ad_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    adset_by_ad_id: dict[str, dict[str, Any]],
    snapshot_run_id: str,
    report_date: str,
    status_resolver: DailyStatusResolver,
) -> tuple[Any, ...]:
    ad_id = str(insight["ad_id"])
    adset = adset_by_ad_id[ad_id]
    row_report_date = _row_report_date(snapshot, insight, report_date)
    campaign_id = str(insight.get("campaign_id") or campaign["id"])
    adset_id = str(insight.get("adset_id") or adset["id"])
    return (
        *_base_values(snapshot, insight, account, snapshot_run_id, report_date, campaign),
        adset_id,
        insight.get("adset_name"),
        ad_id,
        insight.get("ad_name"),
        _creative_id_by_ad_id(adset).get(ad_id),
        None,
        insight.get("attribution_window"),
        status_resolver.ad_status(row_report_date, campaign_id, adset_id, ad_id),
        *_metric_values(insight),
    )


def _base_values(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    snapshot_run_id: str,
    report_date: str,
    campaign: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    return (
        snapshot_run_id,
        _row_report_date(snapshot, insight, report_date),
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


def _row_report_date(snapshot: dict[str, Any], insight: dict[str, Any], fallback: str) -> str:
    return str(insight.get("date_start") or snapshot["metric_date"] or fallback)


def _daily_status_resolver() -> DailyStatusResolver:
    import google.auth  # type: ignore[import-not-found]
    from google.cloud import storage  # type: ignore[import-not-found]

    credentials, _project_id = google.auth.default()
    return DailyStatusResolver(storage.Client(credentials=credentials), CONFIG_GCS_PREFIX)


def _metric_values(insight: dict[str, Any]) -> tuple[Any, ...]:
    values = insight_metric_values(insight)
    return tuple(_sql_value(column, values.get(column)) for column in METRIC_COLUMNS)


def _sql_value(column: str, value: Any) -> Any:
    if column in JSON_COLUMNS:
        return json.dumps(value or [], separators=(",", ":"), sort_keys=True)
    return value


def _upsert_daily_rows(
    table_name: str,
    column_names: tuple[str, ...],
    conflict_columns: tuple[str, ...],
    rows: list[tuple[Any, ...]],
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
            *[f"{column} = EXCLUDED.{column}" for column in update_columns],
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
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


def _snapshot_run_id(run_id: str, level: str, *ids: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, ":".join([DAG_ID, level, run_id, *ids])))


def _snapshot_report_date(logical_date: Any) -> str:
    return report_datetime(logical_date).format("YYYY-MM-DD")


def _report_dates(logical_date: Any) -> list[str]:
    current = report_datetime(logical_date)
    return [current.format("YYYY-MM-DD"), current.subtract(days=1).format("YYYY-MM-DD")]


def _context_logical_date(context: dict[str, Any]) -> Any:
    if context.get("logical_date"):
        return context["logical_date"]
    for key in ("data_interval_start", "execution_date", "ts"):
        if context.get(key):
            return context[key]
    for source in (context.get("dag_run"), context.get("task_instance"), context.get("ti")):
        if source is None:
            continue
        for attr in ("logical_date", "data_interval_start", "execution_date", "run_after", "start_date"):
            value = getattr(source, attr, None)
            if value:
                return value
    run_id = str(context.get("run_id") or getattr(context.get("dag_run"), "run_id", ""))
    if "__" in run_id:
        try:
            return pendulum.parse(run_id.split("__", 1)[1])
        except ParserError:
            pass
    raise KeyError(f"No logical date found in Airflow context keys: {sorted(context)}")


def _adset_by_ad_id(campaign: dict[str, Any]) -> dict[str, dict[str, Any]]:
    adset_by_id: dict[str, dict[str, Any]] = {}
    for adset in campaign.get("adsets", []):
        for ad in adset.get("ads", []):
            if ad.get("id"):
                adset_by_id[str(ad["id"])] = adset
    return adset_by_id


def _creative_id_by_ad_id(adset: dict[str, Any]) -> dict[str, str]:
    creative_by_ad_id: dict[str, str] = {}
    for ad in adset.get("ads", []):
        if ad.get("id") and ad.get("creative_id"):
            creative_by_ad_id[str(ad["id"])] = str(ad["creative_id"])
    return creative_by_ad_id


def _airflow_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_") or "unknown"


def _campaign_config_logical_date(logical_date=None, **context):
    local = report_datetime(logical_date or _context_logical_date(context))
    config_hour = 0 if local.hour < 12 else 12
    return local.set(hour=config_hour, minute=0, second=0, microsecond=0)


meta_traffic_snapshot()

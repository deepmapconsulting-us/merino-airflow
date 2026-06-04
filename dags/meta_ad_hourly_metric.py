"""Sync Meta ad hourly metrics from campaign-config-driven ad batches.

This DAG reads the matching 2-hour campaign config snapshot from GCS, pulls the
last 12 hours of advertiser-time-zone hourly ad insights, and upserts changed
rows into `marketing.meta_ad_hourly_metric`.
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
    SNAPSHOT_BUCKET,
    campaign_config_logical_date,
    gcs_console_link,
    gcs_uri,
    meta_access_token,
    read_json_from_gcs,
    report_datetime,
    report_partition_datetime,
    snapshot_object_name,
    env_config_value,
)
from meta_status import HourlyStatusResolver

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.traffic import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS,
    ad_hourly_snapshot,
    ad_ids_from_config,
    insight_metric_values,
    traffic_accounts_from_config,
)

DAG_ID = "meta_ad_hourly_metric"
CAMPAIGN_CONFIG_DAG_ID = "facebook_campaign_config_update"
CONFIG_GCS_PREFIX = "facebook_campaign_config_update"
ACTIVE_ACCOUNTS_ENV = "FACEBOOK_ACTIVE_ACCOUNTS"
LOOKUP_WINDOW_ENV = "FACEBOOK_TRAFFIC_LOOKUP_WINDOWS"
POSTGRES_CONN_ID = "merino_analytics"
DEFAULT_META_PAGE_LIMIT = 500
AD_BATCH_SIZE = 5
LOOKBACK_HOURS = 12
AD_HOURLY_TABLE = "marketing.meta_ad_hourly_metric"
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
AD_HOURLY_INSERT_COLUMNS = (
    "hourly_run_id",
    "report_datetime",
    "metric_hour",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "creative_id",
    "creative_name",
    "creative_status",
    "creative_object_type",
    "effective_object_story_id",
    "video_ids",
    "attribution_window",
    "active_status",
    *METRIC_COLUMNS,
)
AD_HOURLY_CONFLICT_COLUMNS = ("metric_hour", "ad_id")


@dag(
    dag_id=DAG_ID,
    schedule="0 */2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["meta", "traffic", "hourly", "ad"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def meta_ad_hourly_metric():
    config_source = _campaign_config_for_display()
    config_log = _config_log_payload(config_source)

    @task
    def log_campaign_config_source(source: dict[str, Any]) -> None:
        print(f"{DAG_ID}: config snapshot: {source.get('snapshot_uri') or '<none>'}")
        print(f"{DAG_ID}: config snapshot link: {source.get('snapshot_link') or '<none>'}")
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
        if source.get("error"):
            raise RuntimeError(source["error"])
        print(f"{DAG_ID}: no ad hourly metric tasks were created")

    @task
    def pull_campaign_ad_hourly_metrics(
        account: dict[str, Any],
        campaign: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        window_start, window_end = _hourly_window(_context_logical_date(context))
        ad_batches = _ad_id_batches(campaign, AD_BATCH_SIZE)
        snapshots: list[dict[str, Any]] = []
        for report_date in _hourly_report_dates(window_start, window_end):
            for ad_ids in ad_batches:
                snapshot = ad_hourly_snapshot(
                    access_token,
                    account["id"],
                    campaign["id"],
                    ad_ids,
                    report_date,
                    page_limit=page_limit,
                )
                snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
                snapshot["ad_ids"] = ad_ids
                snapshots.append(snapshot)
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"hourly rows for account={account['id']} campaign={campaign['id']} "
            f"batches={len(ad_batches)} window={window_start.isoformat()}..{window_end.isoformat()}"
        )
        return {
            "snapshots": snapshots,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        }

    @task
    def write_campaign_ad_hourly_metrics(
        hourly_snapshots: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        hourly_run_id = _hourly_run_id(context["run_id"], account["id"], campaign["id"])
        report_datetime = report_datetime_for_row(_context_logical_date(context))
        window_start = pendulum.parse(hourly_snapshots["window_start"])
        window_end = pendulum.parse(hourly_snapshots["window_end"])
        adset_by_ad_id = _ad_context_by_ad_id(campaign)
        status_resolver = _hourly_status_resolver()
        rows = [
            _ad_hourly_row(
                snapshot,
                insight,
                campaign,
                adset_by_ad_id,
                hourly_run_id,
                report_datetime,
                status_resolver,
            )
            for snapshot in hourly_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if _include_hourly_row(insight, adset_by_ad_id, window_start, window_end)
        ]
        _upsert_hourly_rows(AD_HOURLY_TABLE, AD_HOURLY_INSERT_COLUMNS, AD_HOURLY_CONFLICT_COLUMNS, rows)
        print(
            f"{DAG_ID}: upserted {len(rows)} hourly ad rows for "
            f"account={account['id']} campaign={campaign['id']}"
        )
        return {"row_count": len(rows), "account_id": account["id"], "campaign_id": campaign["id"]}

    wait_for_campaign_config = ExternalTaskSensor(
        task_id="wait_for_facebook_campaign_config_update",
        external_dag_id=CAMPAIGN_CONFIG_DAG_ID,
        external_task_id=None,
        execution_date_fn=campaign_config_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )
    wait_for_object_property = ExternalTaskSensor(
        task_id="wait_for_meta_object_property_sync",
        external_dag_id="meta_object_property_sync",
        external_task_id="sync_object_properties",
        execution_date_fn=campaign_config_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )
    config_task = log_campaign_config_source(config_log)
    wait_for_campaign_config >> wait_for_object_property >> config_task
    accounts = config_source.get("accounts", [])
    if not accounts:
        config_task >> no_campaigns_from_campaign_config(config_log)
        return

    for account in accounts:
        with TaskGroup(group_id=f"account_{_airflow_id(account['id'])}") as account_group:
            for campaign in account["campaigns"]:
                with TaskGroup(group_id=f"campaign_{_airflow_id(campaign['id'])}") as campaign_group:
                    hourly_metrics = pull_campaign_ad_hourly_metrics.override(
                        task_id="pull_campaign_ad_hourly_metrics"
                    )(
                        account,
                        campaign,
                        config_log,
                    )
                    write_campaign_ad_hourly_metrics.override(task_id="write_campaign_ad_hourly_metrics")(
                        hourly_metrics,
                        account,
                        campaign,
                    )
                config_task >> campaign_group
        config_task >> account_group


def _campaign_config_for_display() -> dict[str, Any]:
    source: dict[str, Any] = {
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
        snapshot_uri = _config_snapshot_uri()
        snapshot = read_json_from_gcs(storage_client, snapshot_uri)
        lookup_window_days = int(
            env_config_value(LOOKUP_WINDOW_ENV, str(DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS))
        )
        active_accounts = env_config_value(ACTIVE_ACCOUNTS_ENV)
        accounts = traffic_accounts_from_config(
            snapshot,
            active_accounts_value=active_accounts,
            lookup_window_days=lookup_window_days,
        )
        source.update(
            {
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
        "snapshot_uri": source.get("snapshot_uri", ""),
        "snapshot_link": source.get("snapshot_link", ""),
        "account_count": source.get("account_count", 0),
        "campaign_count": source.get("campaign_count", 0),
        "adset_count": source.get("adset_count", 0),
        "error": source.get("error", ""),
    }


def _config_snapshot_uri(logical_date: Any | None = None) -> str:
    partition = report_partition_datetime(logical_date)
    return gcs_uri(
        SNAPSHOT_BUCKET,
        snapshot_object_name(
            CONFIG_GCS_PREFIX,
            partition.format("YYYY-MM-DD"),
            partition.format("YYYYMMDDTHHmmssZZ"),
        ),
    )


def _campaign_count(accounts: list[dict[str, Any]]) -> int:
    return sum(len(account.get("campaigns", [])) for account in accounts)


def _adset_count(accounts: list[dict[str, Any]]) -> int:
    return sum(
        len(campaign.get("adsets", []))
        for account in accounts
        for campaign in account.get("campaigns", [])
    )


def _ad_id_batches(campaign: dict[str, Any], size: int) -> list[list[str]]:
    ad_ids = [
        ad_id
        for adset in campaign.get("adsets", [])
        for ad_id in ad_ids_from_config(adset)
    ]
    return [ad_ids[index : index + size] for index in range(0, len(ad_ids), size)]


def _hourly_window(logical_date: Any) -> tuple[pendulum.DateTime, pendulum.DateTime]:
    window_end = report_datetime(logical_date).replace(minute=0, second=0, microsecond=0)
    return window_end.subtract(hours=LOOKBACK_HOURS), window_end


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


def _hourly_report_dates(
    window_start: pendulum.DateTime,
    window_end: pendulum.DateTime,
) -> list[str]:
    if window_start >= window_end:
        return []
    current = window_start.start_of("day")
    final = window_end.subtract(seconds=1).start_of("day")
    dates = []
    while current <= final:
        dates.append(current.format("YYYY-MM-DD"))
        current = current.add(days=1)
    return dates


def _include_hourly_row(
    insight: dict[str, Any],
    adset_by_ad_id: dict[str, dict[str, Any]],
    window_start: pendulum.DateTime,
    window_end: pendulum.DateTime,
) -> bool:
    if str(insight.get("ad_id") or "") not in adset_by_ad_id:
        return False
    metric_hour = _metric_hour_from_insight(insight)
    return metric_hour is not None and window_start <= metric_hour < window_end


def _ad_hourly_row(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    campaign: dict[str, Any],
    adset_by_ad_id: dict[str, dict[str, Any]],
    hourly_run_id: str,
    report_datetime: str,
    status_resolver: HourlyStatusResolver,
) -> tuple[Any, ...]:
    ad_id = str(insight["ad_id"])
    adset = adset_by_ad_id[ad_id]
    metric_hour = _metric_hour_from_insight(insight)
    if metric_hour is None:
        raise ValueError(f"Missing hourly breakdown for ad_id={ad_id}")
    campaign_id = str(insight.get("campaign_id") or campaign["id"])
    adset_id = str(insight.get("adset_id") or adset["id"])
    return (
        hourly_run_id,
        report_datetime,
        metric_hour.isoformat(),
        campaign_id,
        insight.get("campaign_name"),
        adset_id,
        insight.get("adset_name"),
        ad_id,
        insight.get("ad_name"),
        _creative_id_by_ad_id(adset).get(ad_id),
        None,
        None,
        None,
        None,
        None,
        insight.get("attribution_window"),
        status_resolver.ad_status(metric_hour, campaign_id, adset_id, ad_id),
        *_metric_values(insight),
    )


def _metric_values(insight: dict[str, Any]) -> tuple[Any, ...]:
    values = insight_metric_values(insight)
    return tuple(_sql_value(column, values.get(column)) for column in METRIC_COLUMNS)


def _sql_value(column: str, value: Any) -> Any:
    if column in JSON_COLUMNS:
        return json.dumps(value or [], separators=(",", ":"), sort_keys=True)
    return value


def _metric_hour_from_insight(insight: dict[str, Any]) -> pendulum.DateTime | None:
    report_date = insight.get("date_start")
    hour_range = insight.get("hourly_stats_aggregated_by_advertiser_time_zone")
    if not report_date or not hour_range:
        return None
    hour_start = str(hour_range).split(" - ", 1)[0]
    return pendulum.parse(f"{report_date}T{hour_start}", tz=REPORT_TIMEZONE).replace(
        minute=0,
        second=0,
        microsecond=0,
    )


def _ad_context_by_ad_id(campaign: dict[str, Any]) -> dict[str, dict[str, Any]]:
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


def _upsert_hourly_rows(
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
    key_column_names = set(conflict_columns)
    update_columns = [
        column
        for column in column_names
        if column not in key_column_names and column not in {"metric_hour"}
    ]
    assignments = ",\n            ".join(
        [
            "updated_at = now()",
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
    _ensure_hourly_conflict_index(hook)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


def _ensure_hourly_conflict_index(hook) -> None:
    hook.run(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS meta_ad_hourly_metric_metric_hour_ad_id_unique_idx
            ON marketing.meta_ad_hourly_metric (metric_hour, ad_id)
        """
    )


def _hourly_status_resolver() -> HourlyStatusResolver:
    import google.auth  # type: ignore[import-not-found]
    from google.cloud import storage  # type: ignore[import-not-found]

    credentials, _project_id = google.auth.default()
    return HourlyStatusResolver(storage.Client(credentials=credentials), CONFIG_GCS_PREFIX)


def _hourly_run_id(run_id: str, account_id: str, campaign_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{DAG_ID}:{run_id}:{account_id}:{campaign_id}"))


def report_datetime_for_row(logical_date: Any) -> str:
    return report_datetime(logical_date).replace(minute=0, second=0, microsecond=0).isoformat()


def _airflow_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_") or "unknown"


meta_ad_hourly_metric()


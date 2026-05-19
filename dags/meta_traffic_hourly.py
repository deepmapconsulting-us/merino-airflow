"""Sync hourly Meta ad traffic from campaign-config-driven adset work.

Meta access token is read from Airflow Variable `meta_access_token` (GSM:
`airflow-variables-meta_access_token`), with fallback to env `META_ACCESS_TOKEN`.
The visible DAG hierarchy is built from the latest successful
`facebook_campaign_config_update` GCS snapshot. Each included adset pulls raw ad
metrics, writes snapshot rows, then writes hourly deltas to Postgres.
"""

from __future__ import annotations

import os
import re
import sys
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]
from airflow.utils.task_group import TaskGroup  # type: ignore[import-not-found]

try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found]
except ImportError:
    from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found,no-redef]

from meta_gcs import (
    gcs_console_link,
    gcs_uri,
    latest_object_name,
    meta_access_token,
    metric_date,
    read_json_from_gcs,
    read_latest_snapshot_pointer,
    variable_get,
)

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.traffic import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS,
    adset_traffic_hourly_snapshot,
    traffic_accounts_from_config,
)

DAG_ID = "meta_traffic_hourly"
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
SNAPSHOT_INSERT_COLUMNS = (
    "snapshot_run_id",
    "snapshot_at",
    "partition_hour",
    "company",
    "platform",
    "source",
    "source_account_id",
    "report_start_date",
    "report_end_date",
    "time_increment",
    "object_type",
    "object_id",
    "object_name",
    "parent_object_type",
    "parent_object_id",
    "account_id",
    "campaign_id",
    "adset_id",
    "ad_id",
    "ad_name",
    "creative_id",
    "breakdown_key",
    "impressions",
    "clicks",
    "spend",
    "reach",
    "frequency",
    "ctr",
    "cpc",
    "cpm",
)


@dag(
    dag_id=DAG_ID,
    schedule=timedelta(hours=4),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["meta", "traffic", "hourly"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def meta_traffic_hourly():
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
                f"{DAG_ID}: displaying {source['account_count']} accounts and "
                f"{source['adset_count']} adsets from campaign config"
            )

    @task
    def no_adsets_from_campaign_config(source: dict[str, Any]) -> None:
        print(
            f"{DAG_ID}: no adset tasks were created. "
            f"pointer={source['pointer_uri']} snapshot={source.get('snapshot_uri') or '<none>'}"
        )
        if source.get("error"):
            raise RuntimeError(source["error"])

    @task
    def pull_all_ads_metrics(
        account: dict[str, Any],
        adset: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        print(
            f"{DAG_ID}: pulling ad metrics for account={account['id']} adset={adset['id']} "
            f"from config snapshot {source.get('snapshot_uri') or source['pointer_uri']}"
        )
        access_token = meta_access_token()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        snapshot = adset_traffic_hourly_snapshot(
            access_token,
            account["id"],
            adset["id"],
            metric_date(),
            page_limit=page_limit,
        )
        snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        snapshot["campaign_id"] = adset.get("campaign_id")
        print(
            f"{DAG_ID}: pulled {len(snapshot['insights'])} ad metric rows for "
            f"account={account['id']} adset={adset['id']}"
        )
        return snapshot

    @task
    def write_snapshot_rows(
        adset_snapshot: dict[str, Any],
        account: dict[str, Any],
        adset: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        context = get_current_context()
        partition_hour = _partition_hour(context["logical_date"])
        snapshot_run_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{DAG_ID}:{context['run_id']}:{account['id']}:{adset['id']}",
            )
        )
        rows = [
            _snapshot_row(adset_snapshot, insight, account, adset, snapshot_run_id, partition_hour)
            for insight in adset_snapshot.get("insights", [])
            if insight.get("ad_id")
        ]
        if rows:
            column_names = ", ".join(SNAPSHOT_INSERT_COLUMNS)
            placeholders = ", ".join(["%s"] * len(SNAPSHOT_INSERT_COLUMNS))
            sql = (
                f"INSERT INTO marketing.ad_object_snapshot ({column_names}) "
                f"VALUES ({placeholders}) "
                "ON CONFLICT DO NOTHING"
            )
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            try:
                with conn.cursor() as cursor:
                    cursor.executemany(sql, rows)
                conn.commit()
            finally:
                conn.close()

        print(
            f"{DAG_ID}: wrote {len(rows)} snapshot rows for account={account['id']} "
            f"adset={adset['id']} snapshot_run_id={snapshot_run_id}"
        )
        return {
            "snapshot_run_id": snapshot_run_id,
            "account_id": account["id"],
            "adset_id": adset["id"],
            "row_count": len(rows),
        }

    @task
    def write_hourly_rows(snapshot_write: dict[str, Any]) -> None:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        context = get_current_context()
        report_run_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{DAG_ID}:hourly:{context['run_id']}:{snapshot_write['adset_id']}",
            )
        )
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(
            """
            WITH current_rows AS (
                SELECT *
                FROM marketing.ad_object_snapshot
                WHERE snapshot_run_id = %s::uuid
            ),
            hourly_rows AS (
                SELECT
                    current_rows.*,
                    previous_rows.impressions AS previous_impressions,
                    previous_rows.clicks AS previous_clicks,
                    previous_rows.spend AS previous_spend,
                    previous_rows.reach AS previous_reach,
                    previous_rows.frequency AS previous_frequency,
                    previous_rows.ctr AS previous_ctr,
                    previous_rows.cpc AS previous_cpc,
                    previous_rows.cpm AS previous_cpm
                FROM current_rows
                LEFT JOIN LATERAL (
                    SELECT *
                    FROM marketing.ad_object_snapshot previous_rows
                    WHERE previous_rows.company = current_rows.company
                      AND previous_rows.platform = current_rows.platform
                      AND previous_rows.source = current_rows.source
                      AND previous_rows.source_account_id = current_rows.source_account_id
                      AND previous_rows.object_type = current_rows.object_type
                      AND previous_rows.object_id = current_rows.object_id
                      AND previous_rows.breakdown_key = current_rows.breakdown_key
                      AND COALESCE(previous_rows.attribution_window, '') =
                          COALESCE(current_rows.attribution_window, '')
                      AND previous_rows.snapshot_at < current_rows.snapshot_at
                    ORDER BY previous_rows.snapshot_at DESC
                    LIMIT 1
                ) previous_rows ON TRUE
            )
            INSERT INTO marketing.ad_object_hourly (
                report_run_id,
                metric_hour,
                partition_hour,
                company,
                platform,
                source,
                source_account_id,
                report_start_date,
                report_end_date,
                time_increment,
                object_type,
                object_id,
                object_name,
                parent_object_type,
                parent_object_id,
                account_id,
                campaign_id,
                adset_id,
                ad_id,
                ad_name,
                creative_id,
                breakdown_key,
                impressions,
                clicks,
                spend,
                reach,
                frequency,
                ctr,
                cpc,
                cpm
            )
            SELECT
                %s::uuid,
                partition_hour,
                partition_hour,
                company,
                platform,
                source,
                source_account_id,
                report_start_date,
                report_end_date,
                time_increment,
                object_type,
                object_id,
                object_name,
                parent_object_type,
                parent_object_id,
                account_id,
                campaign_id,
                adset_id,
                ad_id,
                ad_name,
                creative_id,
                breakdown_key,
                COALESCE(impressions, 0) - COALESCE(previous_impressions, 0),
                COALESCE(clicks, 0) - COALESCE(previous_clicks, 0),
                COALESCE(spend, 0) - COALESCE(previous_spend, 0),
                COALESCE(reach, 0) - COALESCE(previous_reach, 0),
                COALESCE(frequency, 0) - COALESCE(previous_frequency, 0),
                COALESCE(ctr, 0) - COALESCE(previous_ctr, 0),
                COALESCE(cpc, 0) - COALESCE(previous_cpc, 0),
                COALESCE(cpm, 0) - COALESCE(previous_cpm, 0)
            FROM hourly_rows
            ON CONFLICT DO NOTHING
            """,
            parameters=(snapshot_write["snapshot_run_id"], report_run_id),
        )
        print(
            f"{DAG_ID}: wrote hourly rows for account={snapshot_write['account_id']} "
            f"adset={snapshot_write['adset_id']} report_run_id={report_run_id}"
        )

    wait_for_campaign_config = ExternalTaskSensor(
        task_id="wait_for_facebook_campaign_config_update",
        external_dag_id=CAMPAIGN_CONFIG_DAG_ID,
        external_task_id=None,
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
        config_task >> no_adsets_from_campaign_config(config_log)
        return

    for account in accounts:
        account_group_id = f"account_{_airflow_id(account['id'])}"
        with TaskGroup(group_id=account_group_id) as account_group:
            for adset in account["adsets"]:
                with TaskGroup(group_id=f"adset_{_airflow_id(adset['id'])}"):
                    raw_metrics = pull_all_ads_metrics.override(task_id="pull_all_ads_metrics")(
                        account,
                        adset,
                        config_log,
                    )
                    snapshot_write = write_snapshot_rows.override(task_id="write_snapshot_rows")(
                        raw_metrics,
                        account,
                        adset,
                    )
                    write_hourly_rows.override(task_id="write_hourly_rows")(snapshot_write)

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
                "generated_at": snapshot.get("generated_at"),
                "active_accounts": active_accounts,
                "lookup_window_days": lookup_window_days,
                "accounts": accounts,
                "account_count": len(accounts),
                "adset_count": sum(len(account["adsets"]) for account in accounts),
            }
        )
        print(
            f"{DAG_ID}: loaded config snapshot {snapshot_uri} "
            f"({source['snapshot_link']}) with {source['account_count']} accounts and "
            f"{source['adset_count']} adsets"
        )
    except Exception as exc:
        source["error"] = str(exc)
        print(f"{DAG_ID}: could not load config snapshot for DAG display: {exc}")
    return source


def _config_log_payload(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "pointer_uri": source["pointer_uri"],
        "pointer_link": source["pointer_link"],
        "snapshot_uri": source.get("snapshot_uri", ""),
        "snapshot_link": source.get("snapshot_link", ""),
        "generated_at": source.get("generated_at"),
        "active_accounts": source.get("active_accounts", ""),
        "lookup_window_days": source.get("lookup_window_days"),
        "account_count": source.get("account_count", 0),
        "adset_count": source.get("adset_count", 0),
        "error": source.get("error", ""),
    }


def _airflow_id(value: str) -> str:
    task_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return task_id.strip("._-") or "unknown"


def _partition_hour(value: Any) -> str:
    if hasattr(value, "astimezone"):
        value = value.astimezone(pendulum.timezone("UTC"))
    return value.replace(minute=0, second=0, microsecond=0).isoformat()


def _snapshot_row(
    adset_snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    adset: dict[str, Any],
    snapshot_run_id: str,
    partition_hour: str,
) -> tuple[Any, ...]:
    ad_id = str(insight["ad_id"])
    return (
        snapshot_run_id,
        adset_snapshot["generated_at"],
        partition_hour,
        COMPANY,
        PLATFORM,
        SOURCE,
        account["id"],
        insight.get("date_start") or adset_snapshot["metric_date"],
        insight.get("date_stop") or adset_snapshot["metric_date"],
        "1",
        "ad",
        ad_id,
        insight.get("ad_name"),
        "adset",
        adset["id"],
        account["id"],
        insight.get("campaign_id") or adset.get("campaign_id"),
        insight.get("adset_id") or adset["id"],
        ad_id,
        insight.get("ad_name"),
        insight.get("creative_id"),
        "",
        insight.get("impressions"),
        insight.get("clicks"),
        insight.get("spend"),
        insight.get("reach"),
        insight.get("frequency"),
        insight.get("ctr"),
        insight.get("cpc"),
        insight.get("cpm"),
    )


meta_traffic_hourly()

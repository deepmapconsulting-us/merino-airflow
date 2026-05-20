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
    REPORT_TIMEZONE,
    gcs_console_link,
    gcs_uri,
    latest_object_name,
    meta_access_token,
    metric_date,
    partition_hour as report_partition_hour,
    read_json_from_gcs,
    read_latest_snapshot_pointer,
    report_datetime,
    variable_get,
)

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.traffic import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS,
    ad_traffic_snapshot,
    adset_traffic_snapshot,
    campaign_traffic_snapshot,
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
CAMPAIGN_SNAPSHOT_TABLE = "marketing.meta_campaign_snapshot_metric"
CAMPAIGN_HOURLY_TABLE = "marketing.meta_campaign_hourly_metric"
ADSET_SNAPSHOT_TABLE = "marketing.meta_adset_snapshot_metric"
ADSET_HOURLY_TABLE = "marketing.meta_adset_hourly_metric"
AD_SNAPSHOT_TABLE = "marketing.meta_ad_snapshot_metric"
AD_HOURLY_TABLE = "marketing.meta_ad_hourly_metric"
CAMPAIGN_SNAPSHOT_INSERT_COLUMNS = (
    "snapshot_run_id",
    "snapshot_at",
    "partition_hour",
    "company",
    "platform",
    "source",
    "source_account_id",
    "timezone_name",
    "report_start_date",
    "report_end_date",
    "time_increment",
    "campaign_id",
    "campaign_name",
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
ADSET_SNAPSHOT_INSERT_COLUMNS = (
    *CAMPAIGN_SNAPSHOT_INSERT_COLUMNS[:13],
    "adset_id",
    "adset_name",
    *CAMPAIGN_SNAPSHOT_INSERT_COLUMNS[13:],
)
AD_SNAPSHOT_INSERT_COLUMNS = (
    *ADSET_SNAPSHOT_INSERT_COLUMNS[:15],
    "ad_id",
    "ad_name",
    "creative_id",
    *ADSET_SNAPSHOT_INSERT_COLUMNS[15:],
)
CAMPAIGN_HOURLY_INSERT_COLUMNS = (
    "report_run_id",
    "metric_hour",
    "partition_hour",
    "company",
    "platform",
    "source",
    "source_account_id",
    "timezone_name",
    "report_start_date",
    "report_end_date",
    "time_increment",
    "campaign_id",
    "campaign_name",
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
ADSET_HOURLY_INSERT_COLUMNS = (
    *CAMPAIGN_HOURLY_INSERT_COLUMNS[:13],
    "adset_id",
    "adset_name",
    *CAMPAIGN_HOURLY_INSERT_COLUMNS[13:],
)
AD_HOURLY_INSERT_COLUMNS = (
    *ADSET_HOURLY_INSERT_COLUMNS[:15],
    "ad_id",
    "ad_name",
    "creative_id",
    *ADSET_HOURLY_INSERT_COLUMNS[15:],
)
METRIC_COLUMNS = ("impressions", "clicks", "spend", "reach", "frequency", "ctr", "cpc", "cpm")


@dag(
    dag_id=DAG_ID,
    schedule=timedelta(hours=4),
    start_date=pendulum.datetime(2026, 1, 1, tz=REPORT_TIMEZONE),
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
                f"{source.get('campaign_count', 0)} campaigns and {source['adset_count']} adsets "
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
    def pull_campaign_metrics(
        account: dict[str, Any],
        campaign: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        print(
            f"{DAG_ID}: pulling campaign metrics for account={account['id']} campaign={campaign['id']} "
            f"from config snapshot {source.get('snapshot_uri') or source['pointer_uri']}"
        )
        access_token = meta_access_token()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        snapshot = campaign_traffic_snapshot(
            access_token,
            account["id"],
            campaign["id"],
            metric_date(),
            page_limit=page_limit,
        )
        snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {len(snapshot['insights'])} campaign metric rows for "
            f"account={account['id']} campaign={campaign['id']}"
        )
        return snapshot

    @task
    def pull_adset_metrics(
        account: dict[str, Any],
        campaign: dict[str, Any],
        adset: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        print(
            f"{DAG_ID}: pulling adset metrics for account={account['id']} adset={adset['id']} "
            f"from config snapshot {source.get('snapshot_uri') or source['pointer_uri']}"
        )
        access_token = meta_access_token()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        snapshot = adset_traffic_snapshot(
            access_token,
            account["id"],
            adset["id"],
            metric_date(),
            page_limit=page_limit,
        )
        snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        snapshot["campaign_id"] = campaign["id"]
        print(
            f"{DAG_ID}: pulled {len(snapshot['insights'])} adset metric rows for "
            f"account={account['id']} adset={adset['id']}"
        )
        return snapshot

    @task
    def pull_ad_metrics(
        account: dict[str, Any],
        campaign: dict[str, Any],
        adset: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        print(
            f"{DAG_ID}: pulling ad/creative metrics for account={account['id']} adset={adset['id']} "
            f"from config snapshot {source.get('snapshot_uri') or source['pointer_uri']}"
        )
        access_token = meta_access_token()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        snapshot = ad_traffic_snapshot(
            access_token,
            account["id"],
            adset["id"],
            metric_date(),
            page_limit=page_limit,
        )
        snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        snapshot["campaign_id"] = campaign["id"]
        print(
            f"{DAG_ID}: pulled {len(snapshot['insights'])} ad/creative metric rows for "
            f"account={account['id']} adset={adset['id']}"
        )
        return snapshot

    @task
    def write_campaign_snapshot_rows(
        campaign_snapshot: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{DAG_ID}:campaign:{context['run_id']}:{account['id']}:{campaign['id']}",
            )
        )
        rows = [
            _campaign_snapshot_row(
                campaign_snapshot,
                insight,
                account,
                campaign,
                snapshot_run_id,
                report_partition_hour(context["logical_date"]),
            )
            for insight in campaign_snapshot.get("insights", [])
            if insight.get("campaign_id") or campaign.get("id")
        ]
        _insert_snapshot_rows(CAMPAIGN_SNAPSHOT_TABLE, CAMPAIGN_SNAPSHOT_INSERT_COLUMNS, rows)
        print(
            f"{DAG_ID}: wrote {len(rows)} campaign snapshot rows for account={account['id']} "
            f"campaign={campaign['id']} snapshot_run_id={snapshot_run_id}"
        )
        return {
            "level": "campaign",
            "snapshot_table": CAMPAIGN_SNAPSHOT_TABLE,
            "hourly_table": CAMPAIGN_HOURLY_TABLE,
            "snapshot_run_id": snapshot_run_id,
            "account_id": account["id"],
            "campaign_id": campaign["id"],
            "row_count": len(rows),
        }

    @task
    def write_adset_snapshot_rows(
        adset_snapshot: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
        adset: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{DAG_ID}:adset:{context['run_id']}:{account['id']}:{adset['id']}",
            )
        )
        rows = [
            _adset_snapshot_row(
                adset_snapshot,
                insight,
                account,
                campaign,
                adset,
                snapshot_run_id,
                report_partition_hour(context["logical_date"]),
            )
            for insight in adset_snapshot.get("insights", [])
            if insight.get("adset_id") or adset.get("id")
        ]
        _insert_snapshot_rows(ADSET_SNAPSHOT_TABLE, ADSET_SNAPSHOT_INSERT_COLUMNS, rows)
        print(
            f"{DAG_ID}: wrote {len(rows)} adset snapshot rows for account={account['id']} "
            f"adset={adset['id']} snapshot_run_id={snapshot_run_id}"
        )
        return {
            "level": "adset",
            "snapshot_table": ADSET_SNAPSHOT_TABLE,
            "hourly_table": ADSET_HOURLY_TABLE,
            "snapshot_run_id": snapshot_run_id,
            "account_id": account["id"],
            "campaign_id": campaign["id"],
            "adset_id": adset["id"],
            "row_count": len(rows),
        }

    @task
    def write_ad_snapshot_rows(
        ad_snapshot: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
        adset: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{DAG_ID}:ad:{context['run_id']}:{account['id']}:{adset['id']}",
            )
        )
        rows = [
            _ad_snapshot_row(
                ad_snapshot,
                insight,
                account,
                campaign,
                adset,
                snapshot_run_id,
                report_partition_hour(context["logical_date"]),
            )
            for insight in ad_snapshot.get("insights", [])
            if insight.get("ad_id")
        ]
        _insert_snapshot_rows(AD_SNAPSHOT_TABLE, AD_SNAPSHOT_INSERT_COLUMNS, rows)
        print(
            f"{DAG_ID}: wrote {len(rows)} ad/creative snapshot rows for account={account['id']} "
            f"adset={adset['id']} snapshot_run_id={snapshot_run_id}"
        )
        return {
            "level": "ad",
            "snapshot_table": AD_SNAPSHOT_TABLE,
            "hourly_table": AD_HOURLY_TABLE,
            "snapshot_run_id": snapshot_run_id,
            "account_id": account["id"],
            "campaign_id": campaign["id"],
            "adset_id": adset["id"],
            "row_count": len(rows),
        }

    @task
    def write_delta_rows(snapshot_write: dict[str, Any]) -> None:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        context = get_current_context()
        level = snapshot_write["level"]
        report_run_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{DAG_ID}:delta:{level}:{context['run_id']}:{snapshot_write['snapshot_run_id']}",
            )
        )
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        metric_hour = report_partition_hour(context["logical_date"])
        if _delta_run_is_stale(context["logical_date"]):
            print(
                f"{DAG_ID}: skipped {level} delta rows for stale run "
                f"metric_hour={metric_hour}; Meta returns latest daily totals, "
                "so historical/manual runs should not write deltas"
            )
            return

        _write_delta_rows(
            hook,
            snapshot_write=snapshot_write,
            report_run_id=report_run_id,
            metric_hour=metric_hour,
            first_report_run=_is_first_report_run(context["logical_date"]),
        )
        print(
            f"{DAG_ID}: wrote {level} delta rows for account={snapshot_write['account_id']} "
            f"snapshot_run_id={snapshot_write['snapshot_run_id']} report_run_id={report_run_id}"
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
        config_task >> no_campaigns_from_campaign_config(config_log)
        return

    for account in accounts:
        account_group_id = f"account_{_airflow_id(account['id'])}"
        with TaskGroup(group_id=account_group_id) as account_group:
            for campaign in account["campaigns"]:
                with TaskGroup(group_id=f"campaign_{_airflow_id(campaign['id'])}") as campaign_group:
                    campaign_metrics = pull_campaign_metrics.override(task_id="pull_campaign_metrics")(
                        account,
                        campaign,
                        config_log,
                    )
                    campaign_snapshot_write = write_campaign_snapshot_rows.override(
                        task_id="write_campaign_snapshot_rows"
                    )(
                        campaign_metrics,
                        account,
                        campaign,
                    )
                    write_delta_rows.override(task_id="write_campaign_delta_rows")(campaign_snapshot_write)

                    for adset in campaign["adsets"]:
                        with TaskGroup(group_id=f"adset_{_airflow_id(adset['id'])}"):
                            adset_metrics = pull_adset_metrics.override(task_id="pull_adset_metrics")(
                                account,
                                campaign,
                                adset,
                                config_log,
                            )
                            adset_snapshot_write = write_adset_snapshot_rows.override(
                                task_id="write_adset_snapshot_rows"
                            )(
                                adset_metrics,
                                account,
                                campaign,
                                adset,
                            )
                            write_delta_rows.override(task_id="write_adset_delta_rows")(adset_snapshot_write)

                            ad_metrics = pull_ad_metrics.override(task_id="pull_ad_metrics")(
                                account,
                                campaign,
                                adset,
                                config_log,
                            )
                            ad_snapshot_write = write_ad_snapshot_rows.override(task_id="write_ad_snapshot_rows")(
                                ad_metrics,
                                account,
                                campaign,
                                adset,
                            )
                            write_delta_rows.override(task_id="write_ad_delta_rows")(ad_snapshot_write)

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
                "generated_at": snapshot.get("generated_at"),
                "active_accounts": active_accounts,
                "lookup_window_days": lookup_window_days,
                "accounts": accounts,
                "account_count": len(accounts),
                "campaign_count": _campaign_count(accounts),
                "adset_count": _adset_count(accounts),
            }
        )
        print(
            f"{DAG_ID}: loaded config snapshot {snapshot_uri} "
            f"({source['snapshot_link']}) with {source['account_count']} accounts and "
            f"{source['campaign_count']} campaigns and {source['adset_count']} adsets"
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
        "campaign_count": source.get("campaign_count", 0),
        "adset_count": source.get("adset_count", 0),
        "error": source.get("error", ""),
    }


def _airflow_id(value: str) -> str:
    task_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return task_id.strip("._-") or "unknown"


def _campaign_count(accounts: list[dict[str, Any]]) -> int:
    return sum(len(account.get("campaigns", [])) for account in accounts)


def _adset_count(accounts: list[dict[str, Any]]) -> int:
    return sum(
        len(campaign.get("adsets", []))
        for account in accounts
        for campaign in account.get("campaigns", [])
    )


def _campaign_snapshot_row(
    campaign_snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    snapshot_run_id: str,
    partition_hour: str,
) -> tuple[Any, ...]:
    return (
        *_campaign_values(campaign_snapshot, insight, account, campaign, snapshot_run_id, partition_hour),
        *_metric_values(insight),
    )


def _adset_snapshot_row(
    adset_snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    adset: dict[str, Any],
    snapshot_run_id: str,
    partition_hour: str,
) -> tuple[Any, ...]:
    return (
        *_campaign_values(adset_snapshot, insight, account, campaign, snapshot_run_id, partition_hour),
        insight.get("adset_id") or adset["id"],
        insight.get("adset_name"),
        *_metric_values(insight),
    )


def _ad_snapshot_row(
    ad_snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    adset: dict[str, Any],
    snapshot_run_id: str,
    partition_hour: str,
) -> tuple[Any, ...]:
    ad_id = str(insight["ad_id"])
    return (
        *_campaign_values(ad_snapshot, insight, account, campaign, snapshot_run_id, partition_hour),
        insight.get("adset_id") or adset["id"],
        insight.get("adset_name"),
        ad_id,
        insight.get("ad_name"),
        insight.get("creative_id") or _creative_id_by_ad_id(adset).get(ad_id),
        *_metric_values(insight),
    )


def _campaign_values(
    snapshot: dict[str, Any],
    insight: dict[str, Any],
    account: dict[str, Any],
    campaign: dict[str, Any],
    snapshot_run_id: str,
    partition_hour: str,
) -> tuple[Any, ...]:
    return (
        snapshot_run_id,
        snapshot["generated_at"],
        partition_hour,
        COMPANY,
        PLATFORM,
        SOURCE,
        account["id"],
        account.get("timezone_name"),
        insight.get("date_start") or snapshot["metric_date"],
        insight.get("date_stop") or snapshot["metric_date"],
        "1",
        insight.get("campaign_id") or campaign["id"],
        insight.get("campaign_name"),
        "",
    )


def _metric_values(insight: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(insight.get(column) for column in METRIC_COLUMNS)


def _creative_id_by_ad_id(adset: dict[str, Any]) -> dict[str, str]:
    creative_by_ad_id: dict[str, str] = {}
    for ad in adset.get("ads", []):
        if ad.get("id") and ad.get("creative_id"):
            creative_by_ad_id[str(ad["id"])] = str(ad["creative_id"])
    return creative_by_ad_id


def _insert_snapshot_rows(table_name: str, column_names: tuple[str, ...], rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return

    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


def _write_delta_rows(
    hook,
    *,
    snapshot_write: dict[str, Any],
    report_run_id: str,
    metric_hour: str,
    first_report_run: bool,
) -> None:
    level = snapshot_write["level"]
    insert_columns = _hourly_insert_columns(level)
    key_columns = _delta_key_columns(level)
    non_metric_columns = insert_columns[2:]
    previous_metric_columns = ",\n                    ".join(
        f"previous_rows.{column} AS previous_{column}" for column in METRIC_COLUMNS
    )
    key_conditions = "\n                      AND ".join(
        f"previous_rows.{column} = current_rows.{column}" for column in key_columns
    )
    select_columns = ",\n                ".join(
        _delta_select_expression(column) for column in non_metric_columns
    )

    hook.run(
        f"""
        WITH current_rows AS (
            SELECT *
            FROM {snapshot_write["snapshot_table"]}
            WHERE snapshot_run_id = %s::uuid
        ),
        delta_rows AS (
            SELECT
                current_rows.*,
                {previous_metric_columns}
            FROM current_rows
            LEFT JOIN LATERAL (
                SELECT *
                FROM {snapshot_write["snapshot_table"]} previous_rows
                WHERE {key_conditions}
                  AND previous_rows.breakdown_key = current_rows.breakdown_key
                  AND COALESCE(previous_rows.attribution_window, '') =
                      COALESCE(current_rows.attribution_window, '')
                  AND previous_rows.report_start_date = current_rows.report_start_date
                  AND previous_rows.snapshot_at < current_rows.snapshot_at
                ORDER BY previous_rows.snapshot_at DESC
                LIMIT 1
            ) previous_rows ON (%s = FALSE)
        )
        INSERT INTO {snapshot_write["hourly_table"]} (
            {", ".join(insert_columns)}
        )
        SELECT
            %s::uuid,
            %s::timestamptz,
            {select_columns}
        FROM delta_rows
        ON CONFLICT DO NOTHING
        """,
        parameters=(snapshot_write["snapshot_run_id"], first_report_run, report_run_id, metric_hour),
    )


def _delta_select_expression(column: str) -> str:
    if column in METRIC_COLUMNS:
        return f"COALESCE({column}, 0) - COALESCE(previous_{column}, 0)"
    return column


def _hourly_insert_columns(level: str) -> tuple[str, ...]:
    if level == "campaign":
        return CAMPAIGN_HOURLY_INSERT_COLUMNS
    if level == "adset":
        return ADSET_HOURLY_INSERT_COLUMNS
    if level == "ad":
        return AD_HOURLY_INSERT_COLUMNS
    raise ValueError(f"Unknown traffic level: {level}")


def _delta_key_columns(level: str) -> tuple[str, ...]:
    if level == "campaign":
        return ("company", "platform", "source", "source_account_id", "campaign_id")
    if level == "adset":
        return ("company", "platform", "source", "source_account_id", "campaign_id", "adset_id")
    if level == "ad":
        return ("company", "platform", "source", "source_account_id", "campaign_id", "adset_id", "ad_id")
    raise ValueError(f"Unknown traffic level: {level}")


def _is_first_report_run(value: Any) -> bool:
    return report_datetime(value).hour == 0


def _delta_run_is_stale(value: Any) -> bool:
    run_hour = report_datetime(value).start_of("hour")
    current_hour = pendulum.now(REPORT_TIMEZONE).start_of("hour")
    return run_hour < current_hour


meta_traffic_hourly()
